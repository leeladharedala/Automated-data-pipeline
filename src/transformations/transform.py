import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

RAW_SCHEMA = StructType([
    StructField("site_id",               StringType(),  nullable=False),
    StructField("timestamp",             StringType(),  nullable=False),
    StructField("energy_generated_kwh",  DoubleType(),  nullable=False),
    StructField("energy_consumed_kwh",   DoubleType(),  nullable=False),
])


def cast_timestamp(df: DataFrame) -> DataFrame:
    df = df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
    before = df.count()
    df = df.na.drop(subset=["timestamp"])
    after = df.count()
    if before != after:
        logger.warning("cast_timestamp: dropped %d row(s) with unparseable timestamps.", before - after)
    return df


def drop_required_nulls(df: DataFrame) -> DataFrame:
    required_cols = ["site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh"]
    before = df.count()
    df = df.na.drop(subset=required_cols)
    after = df.count()
    if before != after:
        logger.warning("drop_required_nulls: dropped %d row(s) with null values.", before - after)
    return df


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add column net_energy_kwh = energy_generated_kwh - energy_consumed_kwh."""
    return df.withColumn(
        "net_energy_kwh",
        F.col("energy_generated_kwh").cast(DoubleType()) - F.col("energy_consumed_kwh").cast(DoubleType()),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add column negative_energy_flag = 1 if either energy column < 0, else 0."""
    condition = (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0)
    return df.withColumn(
        "negative_energy_flag",
        F.when(condition, F.lit(1)).otherwise(F.lit(0)).cast(IntegerType()),
    )


def apply_transformations(df: DataFrame) -> DataFrame:
    logger.info("Applying cast_timestamp ...")
    df = cast_timestamp(df)
    logger.info("Applying drop_required_nulls ...")
    df = drop_required_nulls(df)
    logger.info("Applying calculate_net_energy ...")
    df = calculate_net_energy(df)
    logger.info("Applying flag_negative_energy ...")
    df = flag_negative_energy(df)
    return df.select(
        F.col("site_id"),
        F.col("timestamp"),
        F.col("energy_generated_kwh").cast(DoubleType()),
        F.col("energy_consumed_kwh").cast(DoubleType()),
        F.col("net_energy_kwh").cast(DoubleType()),
        F.col("negative_energy_flag"),
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "sink_path"])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    source_path: str = args["source_path"]
    sink_path: str = args["sink_path"]

    logger.info("Reading JSONL from: %s", source_path)
    raw_df = spark.read.schema(RAW_SCHEMA).option("multiline", "false").json(source_path)
    logger.info("Raw row count: %d", raw_df.count())

    transformed_df = apply_transformations(raw_df)
    logger.info("Transformed row count: %d", transformed_df.count())

    logger.info("Writing Parquet to: %s", sink_path)
    (
        transformed_df.write
                      .mode("overwrite")
                      .option("compression", "snappy")
                      .partitionBy("site_id")
                      .parquet(sink_path)
    )
    logger.info("Write complete.")
    job.commit()


if __name__ == "__main__":
    main()