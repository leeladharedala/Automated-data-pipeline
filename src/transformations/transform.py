"""
transform.py
============

AWS Glue / PySpark ETL job for the "energy site" pipeline.

Job summary
-----------
Reads raw JSON Lines (JSONL) energy telemetry records from an S3 "raw_data/"
prefix, applies two transformations, and writes the result as Parquet
(Snappy compressed, overwrite mode) to an S3 "transformed_data/" prefix.

Input schema (raw_data/*.jsonl)
--------------------------------
    site_id                 : string
    timestamp               : string (ISO-8601, parsed to timestamp)
    energy_generated_kwh    : float
    energy_consumed_kwh     : float

Transformations
----------------
1. calculate_net_energy:
       net_energy_kwh = energy_generated_kwh - energy_consumed_kwh

2. flag_negative_energy:
       negative_energy_flag = 1 if energy_generated_kwh < 0 OR
                                     energy_consumed_kwh < 0
                               else 0

Null handling
-------------
- Rows where site_id or timestamp is null are dropped (they cannot be
  reliably attributed to a site or time window downstream).
- Rows where energy_generated_kwh or energy_consumed_kwh is null have those
  values defaulted to 0.0 before computing net_energy_kwh /
  negative_energy_flag, so numeric transformations never produce nulls.
- timestamp is cast to a proper `timestamp` type; unparseable values become
  null and are dropped along with the other null-timestamp rows.

Sink
----
- Format: Parquet, Snappy compression, overwrite mode.
- Partitioned by `site_id` for efficient downstream reads.

Usage
-----
Plain PySpark:
    spark-submit transform.py \\
        --input_path s3://multi-agent-pipeline-dev-raw-data/raw_data/ \\
        --output_path s3://multi-agent-pipeline-dev-raw-data/transformed_data/

AWS Glue job (via GlueContext), arguments are supplied through Glue job
parameters `--input_path` / `--output_path` (in addition to the standard
`--JOB_NAME`), and the script auto-detects whether it is running inside a
Glue environment.
"""

import argparse
import logging
import sys
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger("energy_transform_job")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Raw JSONL input schema, matching the data_source spec.
RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("energy_generated_kwh", FloatType(), True),
        StructField("energy_consumed_kwh", FloatType(), True),
    ]
)


def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read raw JSONL energy telemetry data from S3.

    Parameters
    ----------
    spark : SparkSession
    input_path : str
        S3 URI/prefix pointing at the raw_data/ JSONL files.

    Returns
    -------
    DataFrame with columns matching RAW_SCHEMA.
    """
    logger.info("Reading raw JSONL data from: %s", input_path)
    df = spark.read.schema(RAW_SCHEMA).json(input_path)
    return df


def clean_raw_data(df: DataFrame) -> DataFrame:
    """
    Explicit null handling prior to transformation:
      - Drop rows with null site_id or unparseable/null timestamp.
      - Fill null energy_generated_kwh / energy_consumed_kwh with 0.0.
      - Cast timestamp string -> timestamp type.

    Parameters
    ----------
    df : DataFrame
        Raw input DataFrame (RAW_SCHEMA).

    Returns
    -------
    Cleaned DataFrame ready for transformation.
    """
    cleaned = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

    cleaned = cleaned.na.fill(
        {"energy_generated_kwh": 0.0, "energy_consumed_kwh": 0.0}
    )

    cleaned = cleaned.na.drop(subset=["site_id", "timestamp"])

    return cleaned


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """
    Add `net_energy_kwh` = energy_generated_kwh - energy_consumed_kwh.

    Parameters
    ----------
    df : DataFrame
        Must contain energy_generated_kwh and energy_consumed_kwh (non-null,
        FloatType or compatible numeric type).

    Returns
    -------
    DataFrame with an added `net_energy_kwh` (float) column.
    """
    return df.withColumn(
        "net_energy_kwh",
        (
            F.col("energy_generated_kwh").cast("float")
            - F.col("energy_consumed_kwh").cast("float")
        ).cast("float"),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """
    Add `negative_energy_flag` (int) = 1 if either energy_generated_kwh or
    energy_consumed_kwh is negative, else 0.

    Parameters
    ----------
    df : DataFrame
        Must contain energy_generated_kwh and energy_consumed_kwh.

    Returns
    -------
    DataFrame with an added `negative_energy_flag` (int) column.
    """
    return df.withColumn(
        "negative_energy_flag",
        F.when(
            (F.col("energy_generated_kwh") < 0)
            | (F.col("energy_consumed_kwh") < 0),
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .cast("int"),
    )


def transform(df: DataFrame) -> DataFrame:
    """
    Full transformation pipeline: clean -> calculate_net_energy ->
    flag_negative_energy.

    Parameters
    ----------
    df : DataFrame
        Raw input DataFrame (RAW_SCHEMA).

    Returns
    -------
    Transformed DataFrame with columns:
        site_id, timestamp, energy_generated_kwh, energy_consumed_kwh,
        net_energy_kwh, negative_energy_flag
    """
    cleaned = clean_raw_data(df)
    with_net_energy = calculate_net_energy(cleaned)
    with_flag = flag_negative_energy(with_net_energy)

    result = with_flag.select(
        "site_id",
        "timestamp",
        "energy_generated_kwh",
        "energy_consumed_kwh",
        "net_energy_kwh",
        "negative_energy_flag",
    )
    return result


def write_transformed_data(
    df: DataFrame,
    output_path: str,
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Write the transformed DataFrame as Parquet (Snappy, overwrite mode) to S3.

    Parameters
    ----------
    df : DataFrame
    output_path : str
        S3 URI/prefix (transformed_data/) to write to.
    partition_by : Optional[List[str]]
        Columns to partition output by. Defaults to ["site_id"].
    """
    if partition_by is None:
        partition_by = ["site_id"]

    logger.info(
        "Writing transformed data to: %s (partitioned by %s)",
        output_path,
        partition_by,
    )
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy(*partition_by)
        .parquet(output_path)
    )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse CLI / Glue job arguments for input and output S3 paths."""
    parser = argparse.ArgumentParser(description="Energy net-usage ETL job")
    parser.add_argument(
        "--input_path",
        required=True,
        help="S3 URI of the raw_data/ JSONL prefix",
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="S3 URI of the transformed_data/ Parquet output prefix",
    )
    # JOB_NAME is provided automatically by Glue; accept and ignore it here
    # if present so the script also runs fine as a Glue job.
    parser.add_argument("--JOB_NAME", required=False, default=None)
    known_args, _unknown = parser.parse_known_args(argv)
    return known_args


def build_spark_session(app_name: str = "energy-net-usage-etl") -> SparkSession:
    """Create (or fetch) a SparkSession, working both locally and on Glue."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def main(argv: Optional[List[str]] = None) -> None:
    """
    Entry point.

    Reads args (input_path, output_path), builds a Spark/Glue session,
    executes the read -> transform -> write pipeline, and logs progress.
    """
    args = parse_args(argv)
    logger.info(
        "Starting energy_transform_job. input_path=%s output_path=%s",
        args.input_path,
        args.output_path,
    )

    spark = build_spark_session()

    # Attempt to use GlueContext when running inside AWS Glue; fall back to
    # plain SparkSession for local/dev/test execution.
    glue_context = None
    try:
        from awsglue.context import GlueContext  # type: ignore
        from awsglue.job import Job  # type: ignore
        from awsglue.utils import getResolvedOptions  # type: ignore

        glue_context = GlueContext(spark.sparkContext)
        spark = glue_context.spark_session

        if args.JOB_NAME is not None:
            resolved = getResolvedOptions(sys.argv, ["JOB_NAME"])
            job = Job(glue_context)
            job.init(resolved["JOB_NAME"], resolved)
    except ImportError:
        logger.info(
            "awsglue library not available; running as a plain PySpark job."
        )

    try:
        raw_df = read_raw_data(spark, args.input_path)
        transformed_df = transform(raw_df)
        write_transformed_data(transformed_df, args.output_path)
        logger.info("Job completed successfully.")
    except Exception:
        logger.exception("Job failed with an exception.")
        raise
    finally:
        if glue_context is not None and args.JOB_NAME is not None:
            try:
                job.commit()  # noqa: F821
            except Exception:
                logger.warning("Unable to commit Glue job bookmark.")


if __name__ == "__main__":
    main()