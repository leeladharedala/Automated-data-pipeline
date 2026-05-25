"""
AWS Glue PySpark ETL job for energy data transformation.

Transformations applied:
  1. calculate_net_energy  — net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
  2. flag_negative_energy  — negative_energy_flag = 1 if either kwh column < 0, else 0

Usage (Glue job arguments):
  --JOB_NAME   <glue-job-name>
  --source_path s3://multi-agent-pipeline-dev-raw-data/raw_data/
  --sink_path   s3://multi-agent-pipeline-dev-raw-data/transformed_data/
"""

import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)

RAW_SCHEMA = StructType([
    StructField("site_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("energy_generated_kwh", DoubleType(), nullable=False),
    StructField("energy_consumed_kwh", DoubleType(), nullable=False),
])


def calculate_net_energy(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "net_energy_kwh",
        (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast(DoubleType()),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    condition = (
        F.col("energy_generated_kwh").isNotNull()
        & F.col("energy_consumed_kwh").isNotNull()
        & ((F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0))
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(condition, F.lit(1)).otherwise(F.lit(0)),
    )


def read_jsonl(spark, source_path: str) -> DataFrame:
    logger.info("Reading JSONL from: %s", source_path)
    df = spark.read.format("json").schema(RAW_SCHEMA).option("multiline", "false").load(source_path)
    df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")).cast(TimestampType()))
    return df


def write_parquet(df: DataFrame, sink_path: str) -> None:
    logger.info("Writing Parquet to: %s", sink_path)
    df.write.mode("overwrite").option("compression", "snappy").partitionBy("site_id").parquet(sink_path)


def run_pipeline(spark, source_path: str, sink_path: str) -> DataFrame:
    df = read_jsonl(spark, source_path)
    df = df.na.drop(subset=["site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh"])
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    write_parquet(df, sink_path)
    return df


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "sink_path"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    logger.info("Job started: %s", args["JOB_NAME"])
    run_pipeline(spark, args["source_path"], args["sink_path"])
    job.commit()
    logger.info("Job committed successfully.")


if __name__ == "__main__":
    main()