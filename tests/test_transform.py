"""
Unit tests for energy ETL transformations.

Run with:
    pytest tests/test_transform.py -v

No S3 or AWS credentials required — all tests use local PySpark DataFrames.
"""

import sys
import os
import types
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def _stub_awsglue() -> None:
    for mod_name in ["awsglue", "awsglue.transforms", "awsglue.utils", "awsglue.context", "awsglue.job"]:
        if mod_name not in sys.modules:
            stub = types.ModuleType(mod_name)
            stub.getResolvedOptions = lambda *a, **kw: {}  # type: ignore[attr-defined]
            stub.GlueContext = object                       # type: ignore[attr-defined]
            stub.Job = object                               # type: ignore[attr-defined]
            sys.modules[mod_name] = stub


_stub_awsglue()

_HERE = os.path.dirname(__file__)
_SRC = os.path.join(_HERE, "..", "src", "transformations")
sys.path.insert(0, os.path.abspath(_SRC))

from transform import calculate_net_energy, flag_negative_energy, apply_transformations  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
                    .master("local[2]")
                    .appName("energy-etl-unit-tests")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate()
    )


INPUT_SCHEMA = StructType([
    StructField("site_id",               StringType(), True),
    StructField("timestamp",             StringType(), True),
    StructField("energy_generated_kwh",  DoubleType(), True),
    StructField("energy_consumed_kwh",   DoubleType(), True),
])


def _make_df(spark, rows):
    return spark.createDataFrame(rows, schema=INPUT_SCHEMA)


class TestCalculateNetEnergy:

    def test_positive_net_energy(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", 100.0, 60.0)])
        result = calculate_net_energy(df).collect()
        assert result[0]["net_energy_kwh"] == pytest.approx(40.0)

    def test_negative_net_energy(self, spark):
        df = _make_df(spark, [("site-2", "2024-01-01T01:00:00Z", 30.0, 80.0)])
        result = calculate_net_energy(df).collect()
        assert result[0]["net_energy_kwh"] == pytest.approx(-50.0)

    def test_zero_net_energy(self, spark):
        df = _make_df(spark, [("site-3", "2024-01-01T02:00:00Z", 50.0, 50.0)])
        result = calculate_net_energy(df).collect()
        assert result[0]["net_energy_kwh"] == pytest.approx(0.0)

    def test_multiple_rows(self, spark):
        rows = [
            ("site-1", "2024-01-01T00:00:00Z", 200.0, 50.0),
            ("site-2", "2024-01-01T01:00:00Z",  10.0, 90.0),
            ("site-3", "2024-01-01T02:00:00Z",   0.0,  0.0),
        ]
        results = {r["site_id"]: r["net_energy_kwh"] for r in calculate_net_energy(_make_df(spark, rows)).collect()}
        assert results["site-1"] == pytest.approx(150.0)
        assert results["site-2"] == pytest.approx(-80.0)
        assert results["site-3"] == pytest.approx(0.0)

    def test_output_column_present(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", 10.0, 5.0)])
        assert "net_energy_kwh" in calculate_net_energy(df).columns

    def test_fractional_values(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", 1.123, 0.456)])
        result = calculate_net_energy(df).collect()
        assert result[0]["net_energy_kwh"] == pytest.approx(0.667, rel=1e-3)


class TestFlagNegativeEnergy:

    def test_flags_negative_generated(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", -5.0, 10.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 1

    def test_flags_negative_consumed(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", 10.0, -3.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 1

    def test_flags_both_negative(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", -10.0, -3.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 1

    def test_no_flag_all_positive(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", 50.0, 30.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 0

    def test_no_flag_zero_values(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", 0.0, 0.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 0

    def test_mixed_rows(self, spark):
        rows = [
            ("site-1", "2024-01-01T00:00:00Z",  100.0,  50.0),
            ("site-2", "2024-01-01T01:00:00Z",   -1.0,  20.0),
            ("site-3", "2024-01-01T02:00:00Z",   30.0,  -5.0),
            ("site-4", "2024-01-01T03:00:00Z",    0.0,   0.0),
        ]
        flags = {r["site_id"]: r["negative_energy_flag"] for r in flag_negative_energy(_make_df(spark, rows)).collect()}
        assert flags["site-1"] == 0
        assert flags["site-2"] == 1
        assert flags["site-3"] == 1
        assert flags["site-4"] == 0

    def test_flag_column_is_integer(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00Z", 10.0, 5.0)])
        flag_type = dict(flag_negative_energy(df).dtypes)["negative_energy_flag"]
        assert flag_type == "int"


class TestApplyTransformations:

    def test_both_columns_added(self, spark):
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00+00:00", 80.0, 40.0)])
        out = apply_transformations(df)
        assert "net_energy_kwh" in out.columns
        assert "negative_energy_flag" in out.columns

    def test_end_to_end_values(self, spark):
        df = _make_df(spark, [("site-A", "2024-06-15T12:00:00+00:00", 120.0, 45.0)])
        result = apply_transformations(df).collect()
        assert result[0]["net_energy_kwh"] == pytest.approx(75.0)
        assert result[0]["negative_energy_flag"] == 0

    def test_end_to_end_negative_flag(self, spark):
        df = _make_df(spark, [("site-B", "2024-06-15T13:00:00+00:00", -10.0, 20.0)])
        result = apply_transformations(df).collect()
        assert result[0]["negative_energy_flag"] == 1
        assert result[0]["net_energy_kwh"] == pytest.approx(-30.0)

    def test_invalid_timestamp_dropped(self, spark):
        rows = [
            ("site-1", "2024-01-01T00:00:00+00:00", 10.0, 5.0),
            ("site-2", "NOT_A_TIMESTAMP",            20.0, 8.0),
        ]
        result = apply_transformations(_make_df(spark, rows)).collect()
        assert len(result) == 1
        assert result[0]["site_id"] == "site-1"

    def test_output_schema(self, spark):
        expected = {"site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh", "net_energy_kwh", "negative_energy_flag"}
        df = _make_df(spark, [("site-1", "2024-01-01T00:00:00+00:00", 10.0, 5.0)])
        assert set(apply_transformations(df).columns) == expected