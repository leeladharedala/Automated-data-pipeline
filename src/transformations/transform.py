"""
AWS Glue PySpark job: energy transformation pipeline.

Reads JSONL site energy records from S3, computes:
  - net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
  - negative_energy_flag = 1 if either generated or consumed energy < 0, else 0

Writes the result as Parquet (snappy, overwrite mode) to S3.

This script is designed to run either:
  - as a standalone PySpark job (spark-submit), or
  - as an AWS Glue job (via GlueContext / Glue job arguments).

Usage (standalone):
    spark-submit transform.py \
        --input-path s3://bucket/raw_data/ \
        --output-path s3://bucket/transformed_data/

Usage (AWS Glue job args, e.g. via getResolvedOptions):
    --JOB_NAME my-job --input-path s3://... --output-path s3://...
"""

import argparse
import logging
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

logger = logging.getLogger("energy_transform_job")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("energy_generated_kwh", FloatType(), True),
        StructField("energy_consumed_kwh", FloatType(), True),
    ]
)


def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read JSONL raw energy data from S3 using the fixed schema."""
    logger.info("Reading JSONL data from %s", input_path)
    df = (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(input_path)
    )
    logger.info("Finished reading raw data")
    return df


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh."""
    logger.info("Calculating net_energy_kwh")
    return df.withColumn(
        "net_energy_kwh",
        F.when(
            df["energy_generated_kwh"].isNull() | df["energy_consumed_kwh"].isNull(),
            F.lit(None).cast(FloatType()),
        ).otherwise(
            (df["energy_generated_kwh"] - df["energy_consumed_kwh"]).cast(FloatType())
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag: 1 if generated or consumed energy < 0, else 0."""
    logger.info("Calculating negative_energy_flag")
    is_negative = (
        (df["energy_generated_kwh"].isNotNull() & (df["energy_generated_kwh"] < 0))
        | (df["energy_consumed_kwh"].isNotNull() & (df["energy_consumed_kwh"] < 0))
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(is_negative, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )


def transform(df: DataFrame) -> DataFrame:
    """Apply the full transformation chain to the raw energy DataFrame."""
    result = (
        df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))
        .transform(calculate_net_energy)
        .transform(flag_negative_energy)
    )

    return result.select(
        "site_id",
        "timestamp",
        "energy_generated_kwh",
        "energy_consumed_kwh",
        "net_energy_kwh",
        "negative_energy_flag",
    )


def write_transformed_data(df: DataFrame, output_path: str) -> None:
    """Write the transformed DataFrame as Parquet (snappy, overwrite)."""
    logger.info("Writing transformed data to %s", output_path)
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(output_path)
    )
    logger.info("Finished writing transformed data")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    """Parse command-line arguments for input/output S3 paths."""
    parser = argparse.ArgumentParser(description="Energy data transformation job")
    parser.add_argument(
        "--input-path",
        required=True,
        help="S3 URI to the raw JSONL data (e.g. s3://bucket/raw_data/)",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="S3 URI to write transformed Parquet data (e.g. s3://bucket/transformed_data/)",
    )
    known_args, _unknown = parser.parse_known_args(argv)
    return known_args


def build_spark_session(app_name: str = "energy-transform-job") -> SparkSession:
    """Create (or fetch) a SparkSession, configured for S3 Parquet I/O."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def main(argv: Optional[list] = None) -> None:
    """Entry point: read raw data, transform, and write Parquet output."""
    args = parse_args(argv if argv is not None else sys.argv[1:])

    spark = build_spark_session()
    try:
        logger.info(
            "Starting energy transform job: input=%s output=%s",
            args.input_path,
            args.output_path,
        )
        raw_df = read_raw_data(spark, args.input_path)
        transformed_df = transform(raw_df)
        write_transformed_data(transformed_df, args.output_path)
        logger.info("Job completed successfully")
    except Exception:
        logger.exception("Energy transform job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()