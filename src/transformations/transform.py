# src/transformations/transform.py

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
    StructType, StructField,
    StringType, FloatType, TimestampType, IntegerType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("energy_etl")

RAW_SCHEMA = StructType([
    StructField("site_id",               StringType(), nullable=False),
    StructField("timestamp",             StringType(), nullable=False),
    StructField("energy_generated_kwh",  FloatType(),  nullable=False),
    StructField("energy_consumed_kwh",   FloatType(),  nullable=False),
])


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh."""
    return df.withColumn(
        "net_energy_kwh",
        F.col("energy_generated_kwh").cast("double") - F.col("energy_consumed_kwh").cast("double"),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag = 1 if either energy column < 0, else 0."""
    return df.withColumn(
        "negative_energy_flag",
        F.when(
            (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0),
            F.lit(1),
        ).otherwise(F.lit(0)).cast(IntegerType()),
    )


def cast_timestamp(df: DataFrame) -> DataFrame:
    """Cast string timestamp to TimestampType."""
    return df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")).cast(TimestampType()))


def drop_critical_nulls(df: DataFrame) -> DataFrame:
    """Drop rows with NULL site_id or timestamp."""
    before = df.count()
    df_clean = df.na.drop(subset=["site_id", "timestamp"])
    dropped = before - df_clean.count()
    if dropped:
        logger.warning("Dropped %d rows with NULL site_id or timestamp.", dropped)
    return df_clean


def apply_transformations(df: DataFrame) -> DataFrame:
    """Orchestrate all transformations."""
    df = cast_timestamp(df)
    df = drop_critical_nulls(df)
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    return df


def read_jsonl(glue_context: GlueContext, source_path: str) -> DataFrame:
    """Read JSONL from S3 with enforced schema."""
    logger.info("Reading JSONL from: %s", source_path)
    df = (
        glue_context.spark_session.read
        .schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(source_path)
    )
    logger.info("Raw row count: %d", df.count())
    return df


def write_parquet(df: DataFrame, sink_path: str) -> None:
    """Write Parquet to S3 (snappy, overwrite, partitioned by site_id)."""
    logger.info("Writing Parquet to: %s", sink_path)
    df.write.mode("overwrite").option("compression", "snappy").partitionBy("site_id").parquet(sink_path)
    logger.info("Write complete.")


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "sink_path"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    logger.info("Starting job '%s' | source=%s | sink=%s", args["JOB_NAME"], args["source_path"], args["sink_path"])
    try:
        raw_df = read_jsonl(glue_context, args["source_path"])
        transformed_df = apply_transformations(raw_df)
        logger.info("Transformed row count: %d", transformed_df.count())
        write_parquet(transformed_df, args["sink_path"])
    except Exception:
        logger.exception("Job failed.")
        raise
    finally:
        job.commit()
        logger.info("Job committed.")


if __name__ == "__main__":
    main()