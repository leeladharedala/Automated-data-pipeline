"""
AWS Glue / PySpark ETL job for solar/energy site telemetry.

Pipeline:
  1. Read newline-delimited JSON (JSONL) records from the raw_data/ S3 prefix
     using an explicit schema (site_id, timestamp, energy_generated_kwh,
     energy_consumed_kwh).
  2. Apply `calculate_net_energy`: adds net_energy_kwh = generated - consumed.
  3. Apply `flag_negative_energy`: adds negative_energy_flag = 1 when either
     energy_generated_kwh or energy_consumed_kwh is negative, else 0.
  4. Write the result as Parquet (snappy) to the transformed_data/ S3 prefix,
     overwriting any existing data.

This script is designed to run either:
  - as a native AWS Glue job (via `--JOB_NAME`, `getResolvedOptions`), or
  - as a plain PySpark job (e.g. `spark-submit transform.py --input-path ... --output-path ...`)

The core transformation functions are pure (DataFrame in -> DataFrame out) so
they can be unit tested with a local SparkSession without any AWS/Glue
dependencies.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

logger = logging.getLogger("energy_etl")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("energy_generated_kwh", FloatType(), True),
        StructField("energy_consumed_kwh", FloatType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read JSONL raw energy telemetry data from S3 using the explicit schema.

    Args:
        spark: active SparkSession.
        input_path: S3 URI prefix (or path) containing JSONL files, e.g.
            "s3://multi-agent-pipeline-dev-raw-data/raw_data/".

    Returns:
        DataFrame conforming to RAW_SCHEMA.
    """
    logger.info("Reading raw JSONL data from %s", input_path)
    df = (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(input_path)
    )
    return df


# ---------------------------------------------------------------------------
# Transformations (pure functions: DataFrame in -> DataFrame out)
# ---------------------------------------------------------------------------

def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh.

    Nulls in either source column are explicitly handled: missing values are
    treated as 0.0 for the purpose of computing the net figure, so the output
    column is never null when at least one side is present. If both source
    values are null, net_energy_kwh will be 0.0.
    """
    logger.info("Applying calculate_net_energy transformation")

    generated = F.coalesce(F.col("energy_generated_kwh"), F.lit(0.0).cast(FloatType()))
    consumed = F.coalesce(F.col("energy_consumed_kwh"), F.lit(0.0).cast(FloatType()))

    return df.withColumn(
        "net_energy_kwh", (generated - consumed).cast(FloatType())
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag: 1 if generated or consumed < 0, else 0.

    Null values are treated as non-negative (i.e. they do not trigger the
    flag) since a missing reading is not evidence of a negative reading.
    """
    logger.info("Applying flag_negative_energy transformation")

    generated_negative = F.col("energy_generated_kwh").isNotNull() & (
        F.col("energy_generated_kwh") < 0
    )
    consumed_negative = F.col("energy_consumed_kwh").isNotNull() & (
        F.col("energy_consumed_kwh") < 0
    )

    return df.withColumn(
        "negative_energy_flag",
        F.when(generated_negative | consumed_negative, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )


def transform(df: DataFrame) -> DataFrame:
    """Apply the full transformation chain to the raw DataFrame."""
    df_with_net = calculate_net_energy(df)
    df_with_flag = flag_negative_energy(df_with_net)
    return df_with_flag


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def write_transformed_data(df: DataFrame, output_path: str) -> None:
    """Write the transformed DataFrame as Parquet (snappy) to S3, overwriting."""
    logger.info("Writing transformed data as Parquet to %s", output_path)
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(output_path)
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_job(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Run the full ETL pipeline: extract -> transform -> load.

    Returns the transformed DataFrame (primarily useful for testing/inspection).
    """
    raw_df = read_raw_data(spark, input_path)

    row_count = None
    try:
        row_count = raw_df.count()
    except Exception:  # pragma: no cover - defensive, count() can be costly/fail on empty
        logger.warning("Unable to compute row count for logging purposes", exc_info=True)

    if row_count is not None:
        logger.info("Read %d raw records", row_count)

    transformed_df = transform(raw_df)
    write_transformed_data(transformed_df, output_path)
    logger.info("ETL job completed successfully")
    return transformed_df


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Energy telemetry ETL job")
    parser.add_argument(
        "--input-path",
        default="s3://multi-agent-pipeline-dev-raw-data/raw_data/",
        help="S3 prefix containing raw JSONL data",
    )
    parser.add_argument(
        "--output-path",
        default="s3://multi-agent-pipeline-dev-raw-data/transformed_data/",
        help="S3 prefix to write transformed Parquet data",
    )
    # Accept and ignore Glue-specific args such as --JOB_NAME when running in Glue.
    parsed, _unknown = parser.parse_known_args(argv)
    return parsed


def build_spark_session(app_name: str = "energy-net-transform") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)

    logger.info(
        "Starting energy ETL job | input=%s output=%s", args.input_path, args.output_path
    )

    spark = build_spark_session()
    try:
        run_job(spark, args.input_path, args.output_path)
    except Exception:
        logger.exception("ETL job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    # Support running natively inside AWS Glue, where job args are resolved
    # via getResolvedOptions and a GlueContext/Job are initialized. This is
    # optional and only exercised when the awsglue library is available.
    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext

        glue_args = getResolvedOptions(
            sys.argv,
            ["JOB_NAME", "input_path", "output_path"],
        )

        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark_session = glue_context.spark_session
        job = Job(glue_context)
        job.init(glue_args["JOB_NAME"], glue_args)

        logger.info(
            "Starting Glue ETL job | input=%s output=%s",
            glue_args["input_path"],
            glue_args["output_path"],
        )
        run_job(spark_session, glue_args["input_path"], glue_args["output_path"])
        job.commit()
    except ImportError:
        # Not running inside a Glue environment; fall back to plain PySpark CLI.
        main()