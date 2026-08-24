"""
AWS Glue / PySpark transformation job.

Reads raw energy telemetry JSONL data, computes net energy and a negative
energy flag, and writes the result out as Parquet.

Transformations
----------------
1. calculate_net_energy:
   net_energy_kwh = energy_generated_kwh - energy_consumed_kwh

2. flag_negative_energy:
   negative_energy_flag = 1 if energy_generated_kwh < 0 OR energy_consumed_kwh < 0
                           else 0

Usable both as a standalone PySpark script and as an AWS Glue job entrypoint.
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
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Default source / destination locations (overridable via CLI args or Glue job args)
DEFAULT_INPUT_PATH = "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
DEFAULT_OUTPUT_PATH = "s3://multi-agent-pipeline-dev-raw-data/transformed_data/"

# Explicit schema per the data contract for the raw JSONL input.
RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("energy_generated_kwh", FloatType(), nullable=True),
        StructField("energy_consumed_kwh", FloatType(), nullable=True),
    ]
)


def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read raw JSONL energy telemetry data using the fixed source schema."""
    logger.info("Reading raw JSONL data from %s", input_path)
    df = (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(input_path)
    )
    logger.info("Read raw data with columns: %s", df.columns)
    return df


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh."""
    logger.info("Applying transformation: calculate_net_energy")
    return df.withColumn(
        "net_energy_kwh",
        F.when(
            df["energy_generated_kwh"].isNull() | df["energy_consumed_kwh"].isNull(),
            F.lit(None).cast("float"),
        ).otherwise(
            (df["energy_generated_kwh"] - df["energy_consumed_kwh"]).cast("float")
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag = 1 if either energy value is negative, else 0."""
    logger.info("Applying transformation: flag_negative_energy")
    generated = df["energy_generated_kwh"]
    consumed = df["energy_consumed_kwh"]
    is_negative = (
        (generated.isNotNull() & (generated < 0))
        | (consumed.isNotNull() & (consumed < 0))
    )
    return df.withColumn(
        "negative_energy_flag",
        F.when(is_negative, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )


def transform(df: DataFrame) -> DataFrame:
    """Apply the full transformation pipeline to the raw DataFrame."""
    normalized = df.withColumn("timestamp", df["timestamp"].cast("timestamp"))
    with_net_energy = calculate_net_energy(normalized)
    with_flags = flag_negative_energy(with_net_energy)

    return with_flags.select(
        "site_id",
        "timestamp",
        "energy_generated_kwh",
        "energy_consumed_kwh",
        "net_energy_kwh",
        "negative_energy_flag",
    )


def write_transformed_data(
    df: DataFrame, output_path: str, mode: str = "overwrite"
) -> None:
    """Write the transformed DataFrame out as Parquet with snappy compression."""
    logger.info(
        "Writing transformed data to %s (mode=%s, format=parquet, compression=snappy)",
        output_path,
        mode,
    )
    (
        df.write.mode(mode)
        .option("compression", "snappy")
        .partitionBy("site_id")
        .parquet(output_path)
    )
    logger.info("Write complete.")


def run_job(
    spark: SparkSession,
    input_path: str = DEFAULT_INPUT_PATH,
    output_path: str = DEFAULT_OUTPUT_PATH,
) -> DataFrame:
    """End-to-end job: read -> transform -> write. Returns the transformed DF."""
    raw_df = read_raw_data(spark, input_path)
    transformed_df = transform(raw_df)
    write_transformed_data(transformed_df, output_path)
    return transformed_df


def _parse_args(argv: Optional[list] = None):
    """Parse CLI / Glue job arguments."""
    import argparse

    parser = argparse.ArgumentParser(description="Energy telemetry transformation job")
    parser.add_argument(
        "--input-path",
        default=DEFAULT_INPUT_PATH,
        help="S3 path to raw JSONL input data",
    )
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help="S3 path to write transformed Parquet output",
    )
    args, _unknown = parser.parse_known_args(argv)
    return args


def main(argv: Optional[list] = None) -> None:
    """Main entrypoint usable as an AWS Glue PySpark job script."""
    args = _parse_args(argv)

    logger.info(
        "Starting transformation job. input_path=%s output_path=%s",
        args.input_path,
        args.output_path,
    )

    spark = (
        SparkSession.builder.appName("energy-net-generation-transform")
        .getOrCreate()
    )

    try:
        run_job(spark, args.input_path, args.output_path)
        logger.info("Job completed successfully.")
    except Exception:
        logger.exception("Job failed with an unhandled exception.")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])