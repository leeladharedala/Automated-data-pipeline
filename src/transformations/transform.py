"""
AWS Glue PySpark job: energy net-generation transformation pipeline.

Reads raw JSONL site energy telemetry from S3, applies:
  1. calculate_net_energy   -> net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
  2. flag_negative_energy   -> negative_energy_flag = 1 if either generated or
                               consumed energy is negative, else 0

and writes the result as Parquet (Snappy compressed, overwrite mode) to S3.

This module is written as a set of pure, unit-testable functions so it can be
exercised both as a Glue job (via main()) and via local pytest with a local
SparkSession.
"""

import logging
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

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

DEFAULT_INPUT_PATH = "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
DEFAULT_OUTPUT_PATH = "s3://multi-agent-pipeline-dev-raw-data/transformed_data/"


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read JSONL raw energy data from S3 using the declared schema.

    Malformed records are dropped (mode=DROPMALFORMED) rather than allowed
    to silently corrupt downstream numeric columns.
    """
    logger.info("Reading raw JSONL data from %s", input_path)
    df = (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "DROPMALFORMED")
        .json(input_path)
    )
    logger.info("Read completed. Row count: %d", df.count())
    return df


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh.

    Rows with null generated/consumed values yield a null net_energy_kwh
    (explicit, rather than silently coalescing to 0) so downstream consumers
    can detect and handle incomplete telemetry.
    """
    logger.info("Applying transformation: calculate_net_energy")
    return df.withColumn(
        "net_energy_kwh",
        (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast(
            FloatType()
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag = 1 if either energy value is negative else 0.

    Null generated/consumed values are treated as "not negative" for flagging
    purposes (i.e., they do not trigger the flag on their own). Spark's
    three-valued SQL logic means a NULL comparison combined via OR with a
    False comparison yields NULL, which `when()` treats as no-match, falling
    through to `otherwise(0)`.
    """
    logger.info("Applying transformation: flag_negative_energy")
    is_negative = (
        (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0)
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(is_negative, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )


def cast_and_clean(df: DataFrame) -> DataFrame:
    """Explicit type casting and null-handling for raw input columns.

    - timestamp: cast string -> timestamp
    - site_id: null site_id rows are dropped (cannot be attributed to a site)
    - energy columns: nulls are preserved (not coerced to 0) so that
      downstream flag/net-energy logic can make an informed decision;
      records missing both energy readings are dropped as unusable.
    """
    logger.info("Casting types and handling nulls")
    df = df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))

    # Drop rows with no site identifier - cannot be attributed/partitioned.
    df = df.na.drop(subset=["site_id"])

    # Drop rows where BOTH energy readings are missing - nothing to compute.
    df = df.na.drop(
        how="all", subset=["energy_generated_kwh", "energy_consumed_kwh"]
    )

    return df.select(
        F.col("site_id").cast(StringType()),
        F.col("timestamp").cast(TimestampType()),
        F.col("energy_generated_kwh").cast(FloatType()),
        F.col("energy_consumed_kwh").cast(FloatType()),
    )


def apply_transformations(df: DataFrame) -> DataFrame:
    """Apply the full transformation chain in the required order."""
    df = cast_and_clean(df)
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    return df


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def write_output(df: DataFrame, output_path: str) -> None:
    """Write the transformed DataFrame as Parquet (Snappy, overwrite)."""
    logger.info("Writing transformed data as Parquet to %s", output_path)
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(output_path)
    )
    logger.info("Write completed successfully")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build_spark_session(app_name: str = "energy-net-transform") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def run(
    spark: SparkSession,
    input_path: str = DEFAULT_INPUT_PATH,
    output_path: str = DEFAULT_OUTPUT_PATH,
) -> DataFrame:
    """Full pipeline: read -> transform -> write. Returns the final DataFrame."""
    raw_df = read_raw_data(spark, input_path)
    transformed_df = apply_transformations(raw_df)
    write_output(transformed_df, output_path)
    return transformed_df


def _get_glue_args(argv) -> Optional[dict]:
    """Attempt to resolve arguments via AWS Glue's getResolvedOptions.

    Falls back to None if awsglue is not available (e.g., local/test run),
    so this module remains importable and testable outside a Glue container.
    """
    try:
        from awsglue.utils import getResolvedOptions

        options = getResolvedOptions(
            argv, ["JOB_NAME", "input_path", "output_path"]
        )
        return options
    except Exception:  # noqa: BLE001 - broad by design for optional dependency
        return None


def main() -> None:
    """Glue job entry point.

    Accepts --input_path / --output_path Glue job arguments; falls back to
    the default S3 URIs defined above if not provided (e.g. local run).
    """
    glue_args = _get_glue_args(sys.argv)

    if glue_args is not None:
        input_path = glue_args.get("input_path", DEFAULT_INPUT_PATH)
        output_path = glue_args.get("output_path", DEFAULT_OUTPUT_PATH)
    else:
        # Plain CLI fallback: python transform.py <input_path> <output_path>
        input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_PATH
        output_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_PATH

    logger.info("Starting job. input_path=%s output_path=%s", input_path, output_path)

    spark = build_spark_session()

    try:
        # Use Glue context/job wrapper if running inside Glue, for bookmarking
        # and proper job lifecycle management.
        try:
            from awsglue.context import GlueContext
            from awsglue.job import Job

            glue_context = GlueContext(spark.sparkContext)
            job = Job(glue_context)
            if glue_args is not None:
                job.init(glue_args["JOB_NAME"], glue_args)

            run(spark, input_path, output_path)

            job.commit()
        except ImportError:
            # Not running inside a Glue environment (e.g. local/test) - run plain.
            run(spark, input_path, output_path)
    except Exception:
        logger.exception("Job failed with an unhandled exception")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()