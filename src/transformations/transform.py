import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType, IntegerType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

INPUT_PATH = "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
OUTPUT_PATH = "s3://multi-agent-pipeline-dev-transformed-data/transformed_data/"


def cast_schema(df):
    logger.info("Casting schema to expected types.")
    return (
        df
        .withColumn("site_id", F.col("site_id").cast(StringType()))
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        .withColumn("energy_generated_kwh", F.col("energy_generated_kwh").cast(DoubleType()))
        .withColumn("energy_consumed_kwh", F.col("energy_consumed_kwh").cast(DoubleType()))
    )


def drop_nulls(df):
    required_cols = ["site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh"]
    before = df.count()
    df_clean = df.na.drop(subset=required_cols)
    after = df_clean.count()
    dropped = before - after
    if dropped:
        logger.warning("Dropped %d rows containing nulls in required columns.", dropped)
    else:
        logger.info("No null rows found in required columns.")
    return df_clean


def calculate_net_energy(df):
    logger.info("Calculating net_energy_kwh.")
    return df.withColumn(
        "net_energy_kwh",
        (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast(DoubleType()),
    )


def flag_negative_energy(df):
    logger.info("Flagging rows with negative energy values.")
    return df.withColumn(
        "negative_energy_flag",
        F.when(
            (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0),
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .cast(IntegerType()),
    )


def apply_transformations(df):
    df = cast_schema(df)
    df = drop_nulls(df)
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    return df


def read_jsonl(spark, path: str):
    logger.info("Reading JSONL from: %s", path)
    return spark.read.option("multiline", "false").json(path)


def write_parquet(df, path: str, partition_cols=None):
    logger.info("Writing Parquet output to: %s", path)
    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .format("parquet")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    logger.info("Write complete.")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("Job '%s' started.", args["JOB_NAME"])

    raw_df = read_jsonl(spark, INPUT_PATH)
    logger.info("Raw row count: %d", raw_df.count())

    transformed_df = apply_transformations(raw_df)
    logger.info("Transformed row count: %d", transformed_df.count())

    write_parquet(transformed_df, OUTPUT_PATH, partition_cols=["site_id"])

    job.commit()
    logger.info("Job '%s' committed successfully.", args["JOB_NAME"])


if __name__ == "__main__":
    main()