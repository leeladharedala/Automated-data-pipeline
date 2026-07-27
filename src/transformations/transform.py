"""
AWS Glue / PySpark ETL job: Solar/Energy site data transformation.

Reads raw JSONL energy telemetry data from an S3 prefix, applies:
  1. calculate_net_energy   -> net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
  2. flag_negative_energy   -> negative_energy_flag = 1 if either generated or
                                consumed energy is negative, else 0

Writes the result as Parquet (snappy compression) to a target S3 prefix in
overwrite mode, partitioned by site_id for efficient downstream reads.

This module is designed to run both as a standalone PySpark script and as an
AWS Glue ETL job (via GlueContext). Transformation logic is expressed as pure
functions that take DataFrame(s) in and return a DataFrame, so it can be unit
tested with a local SparkSession without any AWS/Glue dependencies.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, StructType, StructField

logger = logging.getLogger(__name__)
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


# Raw input schema, as specified by the data source contract.
# timestamp is ingested as string and cast to timestamp explicitly below,
# since JSON has no native timestamp type.
RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("energy_generated_kwh", FloatType(), nullable=True),
        StructField("energy_consumed_kwh", FloatType(), nullable=True),
    ]
)


def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read raw JSONL energy telemetry data from S3.

    Args:
        spark: Active SparkSession.
        input_path: S3 URI prefix containing JSONL files
                     (e.g. s3://bucket/raw_data/).

    Returns:
        DataFrame conforming to RAW_SCHEMA with timestamp cast to
        TimestampType and site_id normalized.
    """
    logger.info("Reading raw JSONL data from %s", input_path)
    df = (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(input_path)
    )

    df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

    logger.info("Read complete. Row count: %d", df.count())
    return df


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """
    Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh.

    Nulls in either operand propagate to a null result (explicit and
    predictable), rather than silently defaulting to 0, so downstream
    consumers can distinguish "unknown" from "zero net energy".
    """
    logger.info("Applying transformation: calculate_net_energy")
    return df.withColumn(
        "net_energy_kwh",
        F.when(
            F.col("energy_generated_kwh").isNotNull()
            & F.col("energy_consumed_kwh").isNotNull(),
            F.col("energy_generated_kwh") - F.col("energy_consumed_kwh"),
        ).otherwise(F.lit(None).cast(FloatType())),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """
    Add negative_energy_flag = 1 if energy_generated_kwh < 0 or
    energy_consumed_kwh < 0, else 0.

    Null values are treated as non-negative (flag = 0) since a null reading
    does not represent a known negative value; this keeps the flag strictly
    boolean (0/1) with no nulls.
    """
    logger.info("Applying transformation: flag_negative_energy")
    is_negative = (
        F.coalesce(F.col("energy_generated_kwh"), F.lit(0.0)) < F.lit(0.0)
    ) | (F.coalesce(F.col("energy_consumed_kwh"), F.lit(0.0)) < F.lit(0.0))

    return df.withColumn(
        "negative_energy_flag",
        F.when(is_negative, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )


def clean_nulls(df: DataFrame) -> DataFrame:
    """
    Explicit null handling for required identifier/timestamp fields.

    Rows missing a site_id or timestamp are dropped since they cannot be
    reliably attributed or ordered; missing energy readings are preserved
    (not dropped) so that partial records are not silently discarded, but
    default to null rather than being coerced to 0.
    """
    before = df.count()
    cleaned = df.na.drop(subset=["site_id", "timestamp"])
    after = cleaned.count()
    if before != after:
        logger.warning(
            "Dropped %d rows with null site_id/timestamp (of %d total)",
            before - after,
            before,
        )
    return cleaned


def transform(df: DataFrame) -> DataFrame:
    """
    Apply the full transformation pipeline:
      1. Clean nulls in required fields.
      2. Calculate net energy.
      3. Flag negative energy readings.

    Args:
        df: Raw input DataFrame matching RAW_SCHEMA (timestamp as timestamp
            type).

    Returns:
        Transformed DataFrame with net_energy_kwh and negative_energy_flag
        columns added.
    """
    df = clean_nulls(df)
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)

    return df.select(
        "site_id",
        "timestamp",
        "energy_generated_kwh",
        "energy_consumed_kwh",
        "net_energy_kwh",
        "negative_energy_flag",
    )


def write_transformed_data(df: DataFrame, output_path: str) -> None:
    """
    Write the transformed DataFrame as Parquet (snappy) to S3, overwrite
    mode, partitioned by site_id for efficient downstream reads.
    """
    logger.info("Writing transformed data to %s", output_path)
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(output_path)
    )
    logger.info("Write complete.")


def run_job(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """
    End-to-end job: read -> transform -> write. Returns the transformed
    DataFrame for convenience (e.g. testing/inspection).
    """
    raw_df = read_raw_data(spark, input_path)
    transformed_df = transform(raw_df)
    write_transformed_data(transformed_df, output_path)
    return transformed_df


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Energy data ETL transformation job")
    parser.add_argument(
        "--input-path",
        required=True,
        help="S3 URI prefix for raw JSONL input data (e.g. s3://bucket/raw_data/)",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="S3 URI prefix for transformed Parquet output (e.g. s3://bucket/transformed_data/)",
    )
    return parser.parse_args(argv)


def build_spark_session(app_name: str = "energy-data-transform") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main(argv: Optional[list] = None) -> None:
    """
    Entry point. Supports both plain `spark-submit` invocation and AWS Glue
    job invocation (Glue passes --input-path/--output-path via job
    parameters resolved through getResolvedOptions upstream, or they can be
    passed directly here).
    """
    args = parse_args(argv)

    spark = build_spark_session()

    try:
        logger.info(
            "Starting energy data transform job. input=%s output=%s",
            args.input_path,
            args.output_path,
        )
        run_job(spark, args.input_path, args.output_path)
        logger.info("Job completed successfully.")
    except Exception:
        logger.exception("Job failed with an unhandled exception.")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()