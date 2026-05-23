"""
AWS Glue PySpark ETL Job for Energy Data Pipeline
Reads JSONL from S3, applies transformations, writes Parquet to S3
"""

import sys
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE_PATH = "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
SINK_PATH = "s3://multi-agent-pipeline-dev-raw-data/transformed_data/"


def calculate_net_energy(df):
    """Add column net_energy_kwh = energy_generated_kwh - energy_consumed_kwh"""
    logger.info("Applying calculate_net_energy transformation")
    return df.withColumn(
        "net_energy_kwh",
        F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")
    )


def flag_negative_energy(df):
    """Add column negative_energy_flag (1 if either energy value < 0, else 0)"""
    logger.info("Applying flag_negative_energy transformation")
    return df.withColumn(
        "negative_energy_flag",
        F.when(
            (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0),
            F.lit(1)
        ).otherwise(F.lit(0))
    )


def apply_transformations(df):
    """Apply all transformations in sequence"""
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    return df


def main():
    """Main entry point for the AWS Glue job"""
    try:
        from awsglue.utils import getResolvedOptions
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from pyspark.context import SparkContext

        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)

        logger.info(f"Starting Glue job: {args['JOB_NAME']}")
        raw_df = spark.read.option("multiline", "false").json(SOURCE_PATH)

        typed_df = raw_df.select(
            F.col("site_id").cast(StringType()).alias("site_id"),
            F.to_timestamp(F.col("timestamp")).alias("timestamp"),
            F.col("energy_generated_kwh").cast(FloatType()).alias("energy_generated_kwh"),
            F.col("energy_consumed_kwh").cast(FloatType()).alias("energy_consumed_kwh"),
        ).filter(
            F.col("site_id").isNotNull() &
            F.col("timestamp").isNotNull() &
            F.col("energy_generated_kwh").isNotNull() &
            F.col("energy_consumed_kwh").isNotNull()
        )

        transformed_df = apply_transformations(typed_df)
        transformed_df.write.mode("overwrite").parquet(SINK_PATH)
        logger.info("Data successfully written to S3")
        job.commit()

    except ImportError:
        logger.warning("awsglue not available, running in local mode")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[*]").appName("EnergyETL_Local").getOrCreate()
        raw_df = spark.read.option("multiline", "false").json(SOURCE_PATH)
        typed_df = raw_df.select(
            F.col("site_id").cast(StringType()).alias("site_id"),
            F.to_timestamp(F.col("timestamp")).alias("timestamp"),
            F.col("energy_generated_kwh").cast(FloatType()).alias("energy_generated_kwh"),
            F.col("energy_consumed_kwh").cast(FloatType()).alias("energy_consumed_kwh"),
        ).filter(
            F.col("site_id").isNotNull() & F.col("timestamp").isNotNull() &
            F.col("energy_generated_kwh").isNotNull() & F.col("energy_consumed_kwh").isNotNull()
        )
        apply_transformations(typed_df).write.mode("overwrite").parquet(SINK_PATH)


if __name__ == "__main__":
    main()