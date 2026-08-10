"""
AWS Glue ETL job for energy data.

Reads raw JSONL energy readings from S3, applies transformations
(net energy calculation and negative-energy flagging), and writes
the result as partitioned, Snappy-compressed Parquet to S3.

Designed for AWS Glue 4.0 (Spark 3.x) but the core transformation
functions are pure PySpark DataFrame -> DataFrame functions and can
be unit tested outside of a Glue environment.
"""

import logging
import sys
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

DEFAULT_SOURCE_URI = "s3://multi-agent-pipeline-dev-raw-data/raw_data/"
DEFAULT_SINK_URI = "s3://multi-agent-pipeline-dev-raw-data/transformed_data/"

RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("energy_generated_kwh", FloatType(), True),
        StructField("energy_consumed_kwh", FloatType(), True),
    ]
)


def read_raw_energy_data(spark: SparkSession, source_uri: str) -> DataFrame:
    """Read raw JSONL energy data from S3 using the expected schema."""
    logger.info("Reading raw JSONL energy data from %s", source_uri)
    df = spark.read.schema(RAW_SCHEMA).json(source_uri)
    return df


def cast_and_clean(df: DataFrame) -> DataFrame:
    """Ensure correct types and normalize timestamp; drop rows missing key fields."""
    logger.info("Casting types and cleaning nulls")
    cleaned = (
        df.withColumn("site_id", F.col("site_id").cast(StringType()))
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn(
            "energy_generated_kwh", F.col("energy_generated_kwh").cast(FloatType())
        )
        .withColumn(
            "energy_consumed_kwh", F.col("energy_consumed_kwh").cast(FloatType())
        )
    )

    cleaned = cleaned.na.fill({"energy_generated_kwh": 0.0, "energy_consumed_kwh": 0.0})
    cleaned = cleaned.na.drop(subset=["site_id", "timestamp"])
    return cleaned


def calculate_net_energy(df: DataFrame) -> DataFrame:
    """Add net_energy_kwh = energy_generated_kwh - energy_consumed_kwh."""
    logger.info("Calculating net_energy_kwh")
    return df.withColumn(
        "net_energy_kwh",
        (F.col("energy_generated_kwh") - F.col("energy_consumed_kwh")).cast(
            FloatType()
        ),
    )


def flag_negative_energy(df: DataFrame) -> DataFrame:
    """Add negative_energy_flag = 1 if generated<0 OR consumed<0, else 0."""
    logger.info("Flagging negative energy readings")
    return df.withColumn(
        "negative_energy_flag",
        F.when(
            (F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0),
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .cast("int"),
    )


def transform(df: DataFrame) -> DataFrame:
    """Apply the full transformation pipeline in order."""
    df = cast_and_clean(df)
    df = calculate_net_energy(df)
    df = flag_negative_energy(df)
    return df


def write_transformed_data(df: DataFrame, sink_uri: str, partition_cols: List[str] = None) -> None:
    """Write the transformed DataFrame as Parquet with overwrite mode."""
    partition_cols = partition_cols or ["site_id"]
    logger.info(
        "Writing transformed data to %s (partitioned by %s, mode=overwrite, format=parquet/snappy)",
        sink_uri,
        partition_cols,
    )
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy(*partition_cols)
        .parquet(sink_uri)
    )


def get_job_args(argv: List[str]) -> Dict[str, str]:
    """
    Resolve job arguments via AWS Glue's getResolvedOptions when running inside
    Glue, falling back to sensible defaults for local runs/tests.
    """
    options = {
        "source_uri": DEFAULT_SOURCE_URI,
        "sink_uri": DEFAULT_SINK_URI,
    }

    try:
        from awsglue.utils import getResolvedOptions

        resolved = getResolvedOptions(
            argv,
            ["JOB_NAME"] if "--JOB_NAME" in argv else [],
        )
        options["job_name"] = resolved.get("JOB_NAME", "energy_etl_job")

        optional_arg_names = []
        if "--source_uri" in argv:
            optional_arg_names.append("source_uri")
        if "--sink_uri" in argv:
            optional_arg_names.append("sink_uri")

        if optional_arg_names:
            resolved_optional = getResolvedOptions(argv, optional_arg_names)
            options.update(resolved_optional)

    except ImportError:
        logger.warning(
            "awsglue module not available; falling back to default/CLI args (local mode)."
        )
        for i, arg in enumerate(argv):
            if arg == "--source_uri" and i + 1 < len(argv):
                options["source_uri"] = argv[i + 1]
            if arg == "--sink_uri" and i + 1 < len(argv):
                options["sink_uri"] = argv[i + 1]

    return options


def build_spark_session(app_name: str = "energy_etl_job") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main(argv: List[str] = None) -> None:
    argv = argv if argv is not None else sys.argv

    job_args = get_job_args(argv)
    source_uri = job_args.get("source_uri", DEFAULT_SOURCE_URI)
    sink_uri = job_args.get("sink_uri", DEFAULT_SINK_URI)

    logger.info("Starting energy ETL job. source_uri=%s sink_uri=%s", source_uri, sink_uri)

    spark = build_spark_session()

    glue_job = None
    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job

        glue_context = GlueContext(spark.sparkContext)
        spark = glue_context.spark_session
        glue_job = Job(glue_context)
        if "job_name" in job_args:
            glue_job.init(job_args["job_name"], job_args)
    except ImportError:
        logger.info("Running outside Glue runtime; using plain SparkSession.")

    try:
        raw_df = read_raw_energy_data(spark, source_uri)
        transformed_df = transform(raw_df)
        write_transformed_data(transformed_df, sink_uri)
        logger.info("Energy ETL job completed successfully.")
    except Exception:
        logger.exception("Energy ETL job failed.")
        raise
    finally:
        if glue_job is not None:
            glue_job.commit()


if __name__ == "__main__":
    main(sys.argv)