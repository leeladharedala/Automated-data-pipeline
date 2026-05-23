"""Unit tests for energy ETL transformation functions."""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from src.transformations.transform import calculate_net_energy, flag_negative_energy


RAW_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("energy_generated_kwh", FloatType(), nullable=True),
        StructField("energy_consumed_kwh", FloatType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# calculate_net_energy tests
# ---------------------------------------------------------------------------


def test_calculate_net_energy_positive_values(spark):
    """Net energy is correctly computed for positive inputs."""
    data = [("site-1", "2024-01-01T00:00:00", 100.0, 60.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = calculate_net_energy(df)
    row = result.collect()[0]
    assert abs(row["net_energy_kwh"] - 40.0) < 1e-4


def test_calculate_net_energy_zero_values(spark):
    """Net energy is zero when both inputs are zero."""
    data = [("site-1", "2024-01-01T00:00:00", 0.0, 0.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = calculate_net_energy(df)
    row = result.collect()[0]
    assert abs(row["net_energy_kwh"]) < 1e-6


def test_calculate_net_energy_negative_result(spark):
    """Net energy is negative when consumed exceeds generated."""
    data = [("site-2", "2024-01-01T01:00:00", 30.0, 80.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = calculate_net_energy(df)
    row = result.collect()[0]
    assert abs(row["net_energy_kwh"] - (-50.0)) < 1e-4


def test_calculate_net_energy_column_exists(spark):
    """Output DataFrame contains the net_energy_kwh column."""
    data = [("site-3", "2024-01-01T02:00:00", 50.0, 50.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = calculate_net_energy(df)
    assert "net_energy_kwh" in result.columns


# ---------------------------------------------------------------------------
# flag_negative_energy tests
# ---------------------------------------------------------------------------


def test_flag_negative_energy_both_positive(spark):
    """Flag is 0 when both energy values are positive."""
    data = [("site-1", "2024-01-01T00:00:00", 100.0, 60.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = flag_negative_energy(df)
    row = result.collect()[0]
    assert row["negative_energy_flag"] == 0


def test_flag_negative_energy_generated_negative(spark):
    """Flag is 1 when energy_generated_kwh is negative."""
    data = [("site-1", "2024-01-01T00:00:00", -10.0, 60.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = flag_negative_energy(df)
    row = result.collect()[0]
    assert row["negative_energy_flag"] == 1


def test_flag_negative_energy_consumed_negative(spark):
    """Flag is 1 when energy_consumed_kwh is negative."""
    data = [("site-2", "2024-01-01T01:00:00", 50.0, -5.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = flag_negative_energy(df)
    row = result.collect()[0]
    assert row["negative_energy_flag"] == 1


def test_flag_negative_energy_both_negative(spark):
    """Flag is 1 when both energy values are negative."""
    data = [("site-3", "2024-01-01T02:00:00", -20.0, -30.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = flag_negative_energy(df)
    row = result.collect()[0]
    assert row["negative_energy_flag"] == 1


def test_flag_negative_energy_both_zero(spark):
    """Flag is 0 when both energy values are zero (zero is not negative)."""
    data = [("site-4", "2024-01-01T03:00:00", 0.0, 0.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = flag_negative_energy(df)
    row = result.collect()[0]
    assert row["negative_energy_flag"] == 0


def test_flag_negative_energy_column_exists(spark):
    """Output DataFrame contains the negative_energy_flag column."""
    data = [("site-5", "2024-01-01T04:00:00", 10.0, 5.0)]
    df = spark.createDataFrame(data, schema=RAW_SCHEMA)
    result = flag_negative_energy(df)
    assert "negative_energy_flag" in result.columns