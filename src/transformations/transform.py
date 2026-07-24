"""
AWS Glue PySpark ETL job.

Reads raw solar/energy telemetry JSONL records from an S3 "raw_data/" prefix,
applies two transformations:

  1. calculate_net_energy   -> net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
  2. flag_negative_energy   -> negative_energy_flag = 1 if either energy_generated_kwh < 0
                                or energy_consumed_kwh < 0, else 0

and writes the result as Parquet (snappy, overwrite mode) to a "transformed_data/" S3 prefix.

Usage (Glue job args or CLI):
    spark-submit transform.py \
        --input-path s3://multi-agent-pipeline-dev-raw-data/raw_data/ \
        --output-path s3://multi-agent-pipeline-dev-raw-data/transformed_data/

The module is also usable as a library: import and call `run_transformations`,
`calculate_net_energy`, `flag_negative_energy`, `read_raw_data`, `write_transformed_data`
directly (e.g. from unit tests) without needing a live Glue/S3 environment.
"""

import argparse
import logging
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

RAW_SCHEMA = T.StructType(
    [
        T.StructField("site_id", T.StringType(), nullable=True),
        T.StructField("timestamp", T.StringType(), nullable=True),
        T.StructField("energy_generated_kwh", T.FloatType(), nullable=True),
        T.StructField("energy_consumed_kwh", T.FloatType(), nullable=True),
    ]
)


def get_spark_session(app_name: str = "energy-net-transform") -> SparkSession:
    """Create (or fetch) a SparkSession suitable for Glue or local execution."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )


def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read raw JSONL energy telemetry data from S3 using the explicit schema."""
    logger.info("Reading raw JSONL data from %s", input_path)
    df = (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(input_path)
    )
    return df


def cast_and_clean(df: DataFrame) -> DataFrame:
    """Cast columns to their proper types and handle nulls explicitly."""
    logger.info("Casting types and handling nulls")
    df = (
        df.withColumn("timestamp", F.col("timestamp").cast(T.TimestampType()))
        .withColumn(
            "energy_generated_kwh", F.col("energy_generated_kwh").cast(T.DoubleType())
        )
        .withColumn(
            "energy_consumed_kwh", F.col("energy_consumed_kwh").cast(T.DoubleType())
        )
    )

    df = df.na.fill({"energy_generated_kwh": 0.0, "energy_consumed_kwh": 0.0})
    df = df.na.drop(subset=["site_id"])

    return df


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh."""
    logger.info("Applying transformation: calculate_net_energy")
    return df.withColumn(
        "net_energy_kwh",
        (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast(
            T.DoubleType()
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag = 1 if either generated or consumed energy is negative."""
    logger.info("Applying transformation: flag_negative_energy")
    return df.withColumn(
        "negative_energy_flag",
        F.when(
            (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0),
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .cast(T.IntegerType()),
    )


def run_transformations(df: DataFrame) -> DataFrame:
    """Apply the full transformation pipeline to the raw input DataFrame."""
    df = cast_and_clean(df)
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
    """Write the transformed DataFrame as Parquet (snappy, overwrite) partitioned by site_id."""
    logger.info("Writing transformed Parquet data to %s", output_path)
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(output_path)
    )


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Energy net/negative-flag ETL job")
    parser.add_argument(
        "--input-path",
        required=True,
        help="S3 URI for raw_data/ JSONL prefix, "
        "e.g. s3://multi-agent-pipeline-dev-raw-data/raw_data/",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="S3 URI for transformed_data/ Parquet prefix, "
        "e.g. s3://multi-agent-pipeline-dev-raw-data/transformed_data/",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)

    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext

        glue_args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(glue_args["JOB_NAME"], glue_args)
        is_glue = True
    except ImportError:
        logger.info("awsglue module not available; running as plain Spark job")
        spark = get_spark_session()
        job = None
        is_glue = False

    try:
        raw_df = read_raw_data(spark, args.input_path)
        transformed_df = run_transformations(raw_df)
        write_transformed_data(transformed_df, args.output_path)
        logger.info("ETL job completed successfully")
    finally:
        if is_glue and job is not None:
            job.commit()


if __name__ == "__main__":
    main()