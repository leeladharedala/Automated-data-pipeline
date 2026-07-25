"""
PySpark data transformation for solar/energy site telemetry.

Reads JSONL records from a raw S3 prefix, applies:
  1. calculate_net_energy: net_energy_kwh = energy_generated_kwh - energy_consumed_kwh
  2. flag_negative_energy: negative_energy_flag = 1 if either energy value is negative, else 0

and writes the result as Parquet (overwrite mode), partitioned by site_id, to a
transformed-data S3 prefix.

This module is split into two layers:
  - Pure, unit-testable transformation functions that operate on Spark DataFrames
    (no AWS Glue / job-runtime dependencies). These are exercised directly by
    the pytest suite under tests/.
  - A thin AWS Glue entrypoint (main()) that wires up GlueContext / job args and
    delegates all actual logic to the pure functions above.

Designed for AWS Glue 4.0 (Spark 3.3.x, Python 3.10).
"""
from __future__ import annotations

import logging
import sys
from typing import Optional, Sequence

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
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

# ---------------------------------------------------------------------------
# Schema definition (matches the sampled/inferred source schema)
# ---------------------------------------------------------------------------
INPUT_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("energy_generated_kwh", FloatType(), nullable=False),
        StructField("energy_consumed_kwh", FloatType(), nullable=False),
    ]
)

DEFAULT_SOURCE_PATH = "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
DEFAULT_SINK_PATH = "s3://multi-agent-pipeline-dev-raw-data/transformed_data/"


# ---------------------------------------------------------------------------
# Pure transformation functions (unit-testable, no Glue dependencies)
# ---------------------------------------------------------------------------
def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh.

    Nulls in either operand propagate to a null net_energy_kwh (explicit,
    rather than silently coalescing to 0), since a missing reading should not
    be treated as a confirmed zero-energy measurement.
    """
    return df.withColumn(
        "net_energy_kwh",
        (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast("float"),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag = 1 if either energy reading is negative, else 0.

    Null energy values are treated as non-negative (flag = 0) since a null
    reading is a missing value, not a validated negative reading.
    """
    is_negative = (
        (F.col("energy_generated_kwh") < 0) & F.col("energy_generated_kwh").isNotNull()
    ) | (
        (F.col("energy_consumed_kwh") < 0) & F.col("energy_consumed_kwh").isNotNull()
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(is_negative, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )


def cast_and_clean(df: DataFrame) -> DataFrame:
    """Cast columns to their target types and drop rows missing required keys.

    Rows with a null site_id or timestamp are dropped (they cannot be
    attributed to a site/time and are not usable downstream). Null energy
    readings are preserved (not dropped) so they can be inspected /
    reprocessed, but are filled with 0.0 only for numeric safety in
    arithmetic performed by calculate_net_energy/flag_negative_energy.
    """
    casted = df.select(
        F.col("site_id").cast(StringType()).alias("site_id"),
        F.to_timestamp(F.col("timestamp")).alias("timestamp"),
        F.col("energy_generated_kwh").cast(FloatType()).alias("energy_generated_kwh"),
        F.col("energy_consumed_kwh").cast(FloatType()).alias("energy_consumed_kwh"),
    )

    cleaned = casted.na.drop(subset=["site_id", "timestamp"])
    return cleaned


def apply_transformations(df: DataFrame) -> DataFrame:
    """Apply the full transformation pipeline, in order, to the input DataFrame.

    Order:
      1. cast_and_clean       - normalize types, drop rows with missing keys
      2. calculate_net_energy - derive net_energy_kwh
      3. flag_negative_energy - derive negative_energy_flag
    """
    logger.info("Starting transformation pipeline")

    df = cast_and_clean(df)
    logger.info("Applied cast_and_clean")

    df = calculate_net_energy(df)
    logger.info("Applied calculate_net_energy")

    df = flag_negative_energy(df)
    logger.info("Applied flag_negative_energy")

    logger.info("Transformation pipeline complete. Output columns: %s", df.columns)
    return df


# ---------------------------------------------------------------------------
# I/O helpers (thin wrappers, still pure-ish / testable with a SparkSession)
# ---------------------------------------------------------------------------
def read_source(spark: SparkSession, source_path: str) -> DataFrame:
    """Read JSONL records from the given S3 prefix using the fixed schema."""
    logger.info("Reading JSONL data from %s", source_path)
    df = spark.read.schema(INPUT_SCHEMA).json(source_path)
    return df


def write_sink(df: DataFrame, sink_path: str, partition_by: Optional[Sequence[str]] = ("site_id",)) -> None:
    """Write the transformed DataFrame as Parquet (overwrite), partitioned by site_id."""
    logger.info(
        "Writing %d partition(s) of data to %s (mode=overwrite, format=parquet, partitionBy=%s)",
        df.rdd.getNumPartitions(),
        sink_path,
        partition_by,
    )
    writer = df.write.mode("overwrite").format("parquet").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(sink_path)
    logger.info("Write complete: %s", sink_path)


def run_pipeline(
    spark: SparkSession,
    source_path: str = DEFAULT_SOURCE_PATH,
    sink_path: str = DEFAULT_SINK_PATH,
) -> DataFrame:
    """End-to-end pipeline: read -> transform -> write. Returns the transformed DataFrame."""
    raw_df = read_source(spark, source_path)
    transformed_df = apply_transformations(raw_df)
    write_sink(transformed_df, sink_path)
    return transformed_df


# ---------------------------------------------------------------------------
# AWS Glue entrypoint
# ---------------------------------------------------------------------------
def main(argv: Optional[Sequence[str]] = None) -> None:
    """AWS Glue job entrypoint.

    Resolves job arguments (source/sink S3 paths), builds a GlueContext-backed
    SparkSession, runs the transformation pipeline, and commits the Glue job.

    Falls back to plain argv parsing / a local SparkSession when awsglue is
    not importable, so this script can also be invoked as a standalone local
    script (e.g. `python transform.py --source ... --sink ...`).
    """
    args = list(argv) if argv is not None else sys.argv[1:]

    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext

        resolved = getResolvedOptions(
            args,
            ["JOB_NAME", "SOURCE_PATH", "SINK_PATH"] if _has_all(args) else ["JOB_NAME"],
        )
        source_path = resolved.get("SOURCE_PATH", DEFAULT_SOURCE_PATH)
        sink_path = resolved.get("SINK_PATH", DEFAULT_SINK_PATH)

        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session

        job = Job(glue_context)
        job.init(resolved["JOB_NAME"], resolved)

        logger.info("Running as AWS Glue job: %s", resolved["JOB_NAME"])
        run_pipeline(spark, source_path=source_path, sink_path=sink_path)

        job.commit()
        logger.info("Glue job committed successfully")

    except ImportError:
        logger.warning("awsglue not available; falling back to local execution mode")
        source_path, sink_path = _parse_local_args(args)

        spark = (
            SparkSession.builder.appName("energy-net-transform-local")
            .getOrCreate()
        )
        try:
            run_pipeline(spark, source_path=source_path, sink_path=sink_path)
        finally:
            spark.stop()


def _has_all(args: Sequence[str]) -> bool:
    return "--SOURCE_PATH" in args and "--SINK_PATH" in args


def _parse_local_args(args: Sequence[str]) -> tuple:
    source_path = DEFAULT_SOURCE_PATH
    sink_path = DEFAULT_SINK_PATH
    it = iter(args)
    for token in it:
        if token in ("--source", "--SOURCE_PATH"):
            source_path = next(it, source_path)
        elif token in ("--sink", "--SINK_PATH"):
            sink_path = next(it, sink_path)
    return source_path, sink_path


if __name__ == "__main__":
    main()