"""
AWS Glue PySpark ETL Job - Energy Pipeline
Source : s3://multi-agent-pipeline-dev-raw-data/raw_data/   (JSONL)
Sink   : s3://multi-agent-pipeline-dev-raw-data/transformed_data/ (Parquet, snappy, overwrite)

Transformations:
1. calculate_net_energy   : net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
2. flag_negative_energy   : negative_energy_flag = 1 if either kwh column < 0, else 0
"""

from __future__ import annotations
import argparse
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType, TimestampType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("energy_etl")

RAW_SCHEMA = StructType([
    StructField("site_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("energy_generated_kwh", FloatType(), nullable=False),
    StructField("energy_consumed_kwh", FloatType(), nullable=False),
])


def cast_and_clean(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        .withColumn("energy_generated_kwh", F.col("energy_generated_kwh").cast(FloatType()))
        .withColumn("energy_consumed_kwh", F.col("energy_consumed_kwh").cast(FloatType()))
    )
    return df.na.drop(subset=["site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh"])


def calculate_net_energy(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "net_energy_kwh",
        F.when(
            F.col("energy_generated_kwh").isNull() | F.col("energy_consumed_kwh").isNull(),
            F.lit(None).cast(FloatType()),
        ).otherwise(
            (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast(FloatType())
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "negative_energy_flag",
        F.when(
            (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )


def apply_transformations(df: DataFrame) -> DataFrame:
    df = cast_and_clean(df)
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    return df


def read_jsonl(spark: SparkSession, input_path: str) -> DataFrame:
    logger.info("Reading JSONL from: %s", input_path)
    df = (
        spark.read.format("json")
        .schema(RAW_SCHEMA)
        .option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(input_path)
    )
    if "_corrupt_record" in df.columns:
        df = df.drop("_corrupt_record")
    return df


def write_parquet(df: DataFrame, output_path: str) -> None:
    logger.info("Writing Parquet to: %s", output_path)
    (
        df.write.format("parquet")
        .option("compression", "snappy")
        .mode("overwrite")
        .partitionBy("site_id")
        .save(output_path)
    )


def main(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("EnergyETL").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    raw_df = read_jsonl(spark, input_path)
    transformed_df = apply_transformations(raw_df)
    write_parquet(transformed_df, output_path)
    spark.stop()
    logger.info("Job finished successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", default="s3://multi-agent-pipeline-dev-raw-data/raw_data/")
    parser.add_argument("--output_path", default="s3://multi-agent-pipeline-dev-raw-data/transformed_data/")
    args, _ = parser.parse_known_args()
    main(args.input_path, args.output_path)