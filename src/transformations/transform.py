"""
AWS Glue / PySpark transformation job for solar site energy data.

Pipeline:
  1. Read newline-delimited JSON (JSONL) records from the raw_data/ S3 prefix
     using an explicit, enforced schema.
  2. Apply `calculate_net_energy`:
       net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
  3. Apply `flag_negative_energy`:
       negative_energy_flag = 1 if energy_generated_kwh < 0 OR energy_consumed_kwh < 0
                               else 0
  4. Write the result as Parquet (Snappy compressed) to the transformed_data/
     S3 prefix in overwrite mode.

This module is written as a set of pure, testable functions so it can be
unit tested outside of the Glue runtime, and it also exposes a `main()`
entry point suitable for invocation as a Glue / spark-submit job.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
# Raw input schema as provided by the data source contract. timestamp is read
# as a string and cast to a timestamp column explicitly (see `read_raw_data`)
# so that malformed timestamp strings do not silently fail schema inference.
#
# NOTE: `nullable=False` here documents the data contract but is NOT enforced
# by Spark at read/construction time (Spark does not validate incoming values
# against declared nullability). Null values can and do occur in practice
# (e.g. malformed timestamps cast to null, missing fields in source JSON), so
# null-handling is performed explicitly in `handle_nulls` rather than being
# relied upon implicitly via the schema.
RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("energy_generated_kwh", FloatType(), nullable=False),
        StructField("energy_consumed_kwh", FloatType(), nullable=False),
    ]
)

DEFAULT_INPUT_PATH = "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
DEFAULT_OUTPUT_PATH = "s3://multi-agent-pipeline-dev-raw-data/transformed_data/"


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read JSONL raw energy data from S3 using the enforced RAW_SCHEMA.

    The `timestamp` column is read as a string per the schema contract and
    then cast to a proper timestamp type so downstream consumers get a
    well-typed column while malformed values become null rather than
    raising a parse error.
    """
    logger.info("Reading JSONL raw data from %s", input_path)
    df = spark.read.schema(RAW_SCHEMA).json(input_path)
    df = df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
    return df


# ---------------------------------------------------------------------------
# Transformations (pure functions: DataFrame in -> DataFrame out)
# ---------------------------------------------------------------------------
def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh.

    Nulls in either input column propagate to a null net_energy_kwh
    (explicit, standard Spark arithmetic null-propagation semantics),
    which is the desired behavior here since a missing reading makes the
    net energy value fundamentally unknown rather than zero.
    """
    return df.withColumn(
        "net_energy_kwh",
        (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast(
            FloatType()
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag: 1 if either energy column is negative, else 0.

    Null-safe: if either column is null, the comparison against 0 evaluates
    to null/false, so a null reading does not spuriously flag a row as
    negative. The flag defaults to 0 for null inputs since a null value is
    not a confirmed negative value.
    """
    negative_condition = (F.col("energy_generated_kwh") < 0) | (
        F.col("energy_consumed_kwh") < 0
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(negative_condition, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )


def handle_nulls(df: DataFrame) -> DataFrame:
    """Explicit null-handling policy for required business keys.

    - Rows missing `site_id` or `timestamp` are dropped (cannot be attributed
      to a site or point in time, so they are not useful downstream).
    - Missing numeric readings (`energy_generated_kwh` / `energy_consumed_kwh`)
      are left as null (not defaulted to 0.0) so they don't distort the
      net_energy_kwh calculation and are visible for data-quality auditing.
    """
    before_count_cols = ["site_id", "timestamp"]
    return df.na.drop(subset=before_count_cols)


def transform(df: DataFrame) -> DataFrame:
    """Apply the full transformation pipeline in order."""
    logger.info("Applying null-handling policy")
    df = handle_nulls(df)

    logger.info("Applying calculate_net_energy transformation")
    df = calculate_net_energy(df)

    logger.info("Applying flag_negative_energy transformation")
    df = flag_negative_energy(df)

    return df.select(
        "site_id",
        "timestamp",
        "energy_generated_kwh",
        "energy_consumed_kwh",
        "net_energy_kwh",
        "negative_energy_flag",
    )


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def write_transformed_data(df: DataFrame, output_path: str) -> None:
    """Write the transformed DataFrame as Snappy-compressed Parquet.

    Partitioned by site_id for efficient downstream per-site reads, in
    overwrite mode as required by the pipeline contract.
    """
    logger.info("Writing transformed data as Parquet to %s", output_path)
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(output_path)
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform raw energy JSONL data into enriched Parquet output."
    )
    parser.add_argument(
        "--input-path",
        default=DEFAULT_INPUT_PATH,
        help="S3 URI prefix to read raw JSONL data from.",
    )
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help="S3 URI prefix to write transformed Parquet data to.",
    )
    # Support AWS Glue-style --JOB_NAME / getResolvedOptions args being passed
    # through without breaking argparse when run inside a Glue job.
    known_args, _unknown = parser.parse_known_args(argv)
    return known_args


def build_spark_session(app_name: str = "energy-transform-job") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    logger.info(
        "Starting energy transformation job | input_path=%s output_path=%s",
        args.input_path,
        args.output_path,
    )

    spark = build_spark_session()
    try:
        raw_df = read_raw_data(spark, args.input_path)
        transformed_df = transform(raw_df)
        write_transformed_data(transformed_df, args.output_path)
        logger.info("Energy transformation job completed successfully")
    except Exception:
        logger.exception("Energy transformation job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])