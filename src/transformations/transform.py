"""
Energy ETL transformation job (AWS Glue compatible).

Reads raw JSONL energy telemetry from an S3 "raw_data/" prefix, applies
business transformations (net energy calculation and negative-energy
flagging), and writes the result as Parquet (overwrite mode) to a
"transformed_data/" prefix.

Designed to run either:
  - As an AWS Glue job (GlueContext/Job available), or
  - As a plain PySpark script (e.g. locally, in tests, or on EMR).

Source schema (raw_data/*.jsonl):
    site_id                 : string
    timestamp               : string
    energy_generated_kwh    : float
    energy_consumed_kwh     : float

Output schema (transformed_data/*.parquet):
    site_id                 : string
    timestamp               : timestamp
    energy_generated_kwh    : double
    energy_consumed_kwh     : double
    net_energy_kwh          : double
    negative_energy_flag    : int
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    DoubleType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Explicit input schema so malformed/missing fields become nulls we can
# handle deliberately, rather than causing schema-inference surprises.
RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("energy_generated_kwh", DoubleType(), True),
        StructField("energy_consumed_kwh", DoubleType(), True),
    ]
)


def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read raw JSONL energy data from S3 into a typed DataFrame."""
    logger.info("Reading raw JSONL data from: %s", input_path)
    df = spark.read.schema(RAW_SCHEMA).json(input_path)

    df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

    return df


def clean_raw_data(df: DataFrame) -> DataFrame:
    """Handle nulls explicitly before applying business transformations."""
    before_count = df.count()

    cleaned = df.na.drop(subset=["site_id", "timestamp"])
    cleaned = cleaned.na.fill(
        {"energy_generated_kwh": 0.0, "energy_consumed_kwh": 0.0}
    )

    after_count = cleaned.count()
    dropped = before_count - after_count
    if dropped > 0:
        logger.warning(
            "Dropped %d row(s) with null site_id/timestamp during cleaning",
            dropped,
        )

    return cleaned


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh."""
    return df.withColumn(
        "net_energy_kwh",
        (
            F.col("energy_generated_kwh").cast(DoubleType())
            - F.col("energy_consumed_kwh").cast(DoubleType())
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag = 1 if either energy reading is negative."""
    return df.withColumn(
        "negative_energy_flag",
        F.when(
            (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0),
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .cast("int"),
    )


def transform(df: DataFrame) -> DataFrame:
    """Apply the full set of business transformations to raw energy data."""
    cleaned = clean_raw_data(df)
    with_net_energy = calculate_net_energy(cleaned)
    with_flag = flag_negative_energy(with_net_energy)

    result = with_flag.select(
        F.col("site_id").cast(StringType()).alias("site_id"),
        F.col("timestamp").cast("timestamp").alias("timestamp"),
        F.col("energy_generated_kwh").cast(DoubleType()).alias("energy_generated_kwh"),
        F.col("energy_consumed_kwh").cast(DoubleType()).alias("energy_consumed_kwh"),
        F.col("net_energy_kwh").cast(DoubleType()).alias("net_energy_kwh"),
        F.col("negative_energy_flag").cast("int").alias("negative_energy_flag"),
    )

    return result


def write_transformed_data(df: DataFrame, output_path: str) -> None:
    """Write the transformed DataFrame as Parquet (overwrite) to S3."""
    logger.info("Writing transformed data (overwrite) to: %s", output_path)
    (
        df.write.mode("overwrite")
        .partitionBy("site_id")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    logger.info("Write complete.")


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the full read -> transform -> write pipeline."""
    raw_df = read_raw_data(spark, input_path)
    transformed_df = transform(raw_df)
    write_transformed_data(transformed_df, output_path)
    return transformed_df


def parse_args(argv=None) -> argparse.Namespace:
    """Parse CLI / Glue job arguments."""
    parser = argparse.ArgumentParser(description="Energy ETL transformation job")
    parser.add_argument(
        "--input_path",
        required=False,
        default="s3://multi-agent-pipeline-dev-raw-data/raw_data/",
        help="S3 URI to the raw_data/ JSONL prefix",
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="S3 URI to the transformed_data/ Parquet output prefix",
    )
    parser.add_argument(
        "--JOB_NAME",
        required=False,
        default="energy_etl_transform",
        help="Glue job name (ignored outside Glue)",
    )
    return parser.parse_known_args(argv)[0]


def build_spark_session(app_name: str = "energy_etl_transform") -> SparkSession:
    """Build (or reuse) a SparkSession."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def main(argv=None) -> None:
    """Entry point for both Glue job execution and local/CLI execution."""
    args = parse_args(argv)

    spark = build_spark_session(args.JOB_NAME)

    glue_job = None
    try:
        from awsglue.context import GlueContext  # type: ignore
        from awsglue.job import Job  # type: ignore
        from awsglue.utils import getResolvedOptions  # type: ignore

        glue_context = GlueContext(spark.sparkContext)
        spark = glue_context.spark_session

        resolved_args = getResolvedOptions(
            sys.argv, ["JOB_NAME", "input_path", "output_path"]
        )
        glue_job = Job(glue_context)
        glue_job.init(resolved_args["JOB_NAME"], resolved_args)

        args.input_path = resolved_args.get("input_path", args.input_path)
        args.output_path = resolved_args["output_path"]

        logger.info("Running inside AWS Glue with job name: %s", resolved_args["JOB_NAME"])
    except ImportError:
        logger.info("awsglue not available; running as a plain PySpark job.")

    logger.info(
        "Starting energy ETL job | input_path=%s output_path=%s",
        args.input_path,
        args.output_path,
    )

    run_etl(spark, args.input_path, args.output_path)

    if glue_job is not None:
        glue_job.commit()

    logger.info("Energy ETL job finished successfully.")


if __name__ == "__main__":
    main()