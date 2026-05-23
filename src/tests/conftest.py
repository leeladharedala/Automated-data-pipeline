"""Shared pytest fixtures for energy ETL unit tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for the entire test session."""
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("energy-etl-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()