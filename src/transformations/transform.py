"""
AWS Glue PySpark ETL job for energy data transformation.

Reads JSONL from S3, applies net energy calculation and negative energy flagging,
and writes Parquet output to S3.
"""

import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SOURCE_PATH = "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
SINK_PATH = "s3://multi-agent-pipeline-dev-raw-data/transformed_data/"


def cast_schema(df: DataFrame) -> DataFrame:
    logger.info("Casting schema to expected types.")
    return (
        df
        .withColumn("site_id", F.col("site_id").cast(StringType()))
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        .withColumn("energy_generated_kwh", F.col("energy_generated_kwh").cast(DoubleType()))
        .withColumn("energy_consumed_kwh", F.col("energy_consumed_kwh").cast(DoubleType()))
    )


def drop_nulls(df: DataFrame) -> DataFrame:
    required_cols = ["site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh"]
    before = df.count()
    df_clean = df.na.drop(subset=required_cols)
    after = df_clean.count()
    dropped = before - after
    if dropped:
        logger.warning("Dropped %d rows containing nulls in required columns.", dropped)
    return df_clean


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """
    Add column net_energy_kwh = energy_generated_kwh - energy_consumed_kwh.
    """
    logger.info("Calculating net_energy_kwh.")
    return df.withColumn(
        "net_energy_kwh",
        (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast(DoubleType()),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """
    Add column negative_energy_flag:
      1 if energy_generated_kwh < 0 OR energy_consumed_kwh < 0, else 0.
    """
    logger.info("Flagging rows with negative energy values.")
    condition = (
        (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0)
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(condition, F.lit(1)).otherwise(F.lit(0)).cast(IntegerType()),
    )


def transform(df: DataFrame) -> DataFrame:
    logger.info("Starting transformation pipeline.")
    df = cast_schema(df)
    df = drop_nulls(df)
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    logger.info("Transformation pipeline complete.")
    return df


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("Reading JSONL from: %s", SOURCE_PATH)
    raw_df = spark.read.option("multiline", "false").json(SOURCE_PATH)
    logger.info("Raw record count: %d", raw_df.count())

    transformed_df = transform(raw_df)
    logger.info("Transformed record count: %d", transformed_df.count())

    logger.info("Writing Parquet to: %s", SINK_PATH)
    (
        transformed_df
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(SINK_PATH)
    )
    logger.info("Write complete.")
    job.commit()


if __name__ == "__main__":
    main()