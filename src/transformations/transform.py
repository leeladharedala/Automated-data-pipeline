"""
AWS Glue PySpark ETL job for energy data transformation.

Transformations applied:
  - calculate_net_energy: net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
  - flag_negative_energy: negative_energy_flag = 1 if either kwh column < 0, else 0

Pure transformation functions are defined at module level so they can be
imported and unit-tested without triggering any Glue initialisation.
"""

import logging
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("energy_generated_kwh", FloatType(), nullable=False),
        StructField("energy_consumed_kwh", FloatType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# Pure transformation functions (importable without Glue)
# ---------------------------------------------------------------------------


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh."""
    logger.info("Applying calculate_net_energy transformation.")
    return df.withColumn(
        "net_energy_kwh",
        (
            F.col("energy_generated_kwh").cast(FloatType())
            - F.col("energy_consumed_kwh").cast(FloatType())
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag = 1 if either kwh column is negative, else 0."""
    logger.info("Applying flag_negative_energy transformation.")
    condition = (F.col("energy_generated_kwh") < 0) | (
        F.col("energy_consumed_kwh") < 0
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(condition, 1).otherwise(0),
    )


# ---------------------------------------------------------------------------
# Glue entry point
# ---------------------------------------------------------------------------


def main() -> None:  # pragma: no cover
    """AWS Glue job entry point."""
    from awsglue.context import GlueContext  # type: ignore[import]
    from awsglue.job import Job  # type: ignore[import]
    from awsglue.utils import getResolvedOptions  # type: ignore[import]
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "source_path", "sink_path"],
    )

    source_path: str = args.get(
        "source_path", "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
    )
    sink_path: str = args.get(
        "sink_path", "s3://multi-agent-pipeline-dev-raw-data/transformed_data/"
    )
    job_name: str = args["JOB_NAME"]

    logger.info("Initialising Glue job: %s", job_name)
    logger.info("Source path : %s", source_path)
    logger.info("Sink path   : %s", sink_path)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(job_name, args)

    try:
        logger.info("Reading JSONL data from %s", source_path)
        raw_df = (
            spark.read.schema(RAW_SCHEMA)
            .option("multiline", "false")
            .json(source_path)
        )

        raw_df = raw_df.withColumn(
            "timestamp",
            F.to_timestamp(F.col("timestamp")).cast(TimestampType()),
        )

        raw_df = raw_df.na.drop(
            subset=["site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh"]
        )

        logger.info("Raw record count: %d", raw_df.count())

        transformed_df = calculate_net_energy(raw_df)
        transformed_df = flag_negative_energy(transformed_df)

        logger.info("Transformation complete. Writing output to %s", sink_path)

        (
            transformed_df.write.mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("site_id")
            .parquet(sink_path)
        )

        logger.info("Write complete.")

    except Exception:
        logger.exception("Unhandled error during ETL job execution.")
        raise
    finally:
        job.commit()
        logger.info("Job committed.")


if __name__ == "__main__":
    main()