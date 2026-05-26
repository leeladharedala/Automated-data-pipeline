"""
AWS Glue PySpark ETL job for Energy pipeline.

Reads JSONL from S3, applies energy transformations, writes Parquet to S3.
"""

import sys
import logging
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("energy_etl")

INPUT_SCHEMA = StructType([
    StructField("site_id",               StringType(), nullable=False),
    StructField("timestamp",             StringType(), nullable=False),
    StructField("energy_generated_kwh",  DoubleType(), nullable=False),
    StructField("energy_consumed_kwh",   DoubleType(), nullable=False),
])

REQUIRED_COLUMNS = {"site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh"}


def validate_schema(df: DataFrame) -> None:
    actual_columns = set(df.columns)
    missing = REQUIRED_COLUMNS - actual_columns
    if missing:
        raise ValueError(f"Input DataFrame is missing required columns: {missing}")
    for col_name in ["site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh"]:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            logger.warning("Column '%s' contains %d null value(s) - these rows will be dropped.", col_name, null_count)
    logger.info("Schema validation passed.")


def cast_and_clean(df: DataFrame) -> DataFrame:
    df = (
        df
        .withColumn("timestamp",            F.to_timestamp(F.col("timestamp")))
        .withColumn("energy_generated_kwh", F.col("energy_generated_kwh").cast(DoubleType()))
        .withColumn("energy_consumed_kwh",  F.col("energy_consumed_kwh").cast(DoubleType()))
    )
    before = df.count()
    df = df.na.drop(subset=list(REQUIRED_COLUMNS))
    dropped = before - df.count()
    if dropped:
        logger.warning("Dropped %d row(s) containing nulls.", dropped)
    return df


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add column: net_energy_kwh = energy_generated_kwh - energy_consumed_kwh"""
    return df.withColumn(
        "net_energy_kwh",
        F.col("energy_generated_kwh") - F.col("energy_consumed_kwh"),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add column: negative_energy_flag = 1 if either energy field < 0, else 0"""
    condition = (
        (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0)
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(condition, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )


def apply_transformations(df: DataFrame) -> DataFrame:
    df = cast_and_clean(df)
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    return df


def read_jsonl(spark: SparkSession, input_path: str) -> DataFrame:
    logger.info("Reading JSONL from: %s", input_path)
    df = (
        spark.read
        .schema(INPUT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(input_path)
    )
    logger.info("Raw record count: %d", df.count())
    return df


def write_parquet(df: DataFrame, output_path: str) -> None:
    logger.info("Writing Parquet to: %s", output_path)
    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(output_path)
    )
    logger.info("Write complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Energy ETL - AWS Glue PySpark job")
    parser.add_argument("--input_path",  default="s3://multi-agent-pipeline-dev-raw-data/raw_data/")
    parser.add_argument("--output_path", default="s3://multi-agent-pipeline-dev-raw-data/transformed_data/")
    args, _ = parser.parse_known_args()

    spark = (
        SparkSession.builder
        .appName("EnergyETL")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw = read_jsonl(spark, args.input_path)
        validate_schema(df_raw)
        df_transformed = apply_transformations(df_raw)
        logger.info("Transformed record count: %d", df_transformed.count())
        write_parquet(df_transformed, args.output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()