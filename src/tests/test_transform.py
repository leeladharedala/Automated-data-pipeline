"""
Unit tests for energy ETL transformation functions.

Run with:
    pytest src/tests/test_transform.py -v
"""

import pytest
import sys
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType,
)
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "transformations"))

from transform import calculate_net_energy, flag_negative_energy, transform, cast_schema, drop_nulls  # noqa: E402


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("energy-etl-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


ENERGY_SCHEMA = StructType([
    StructField("site_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("energy_generated_kwh", DoubleType(), nullable=False),
    StructField("energy_consumed_kwh", DoubleType(), nullable=False),
])

_TS = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=ENERGY_SCHEMA)


class TestCalculateNetEnergy:
    def test_positive_net_energy(self, spark):
        df = make_df(spark, [Row(site_id="s1", timestamp=_TS, energy_generated_kwh=100.0, energy_consumed_kwh=60.0)])
        assert calculate_net_energy(df).collect()[0]["net_energy_kwh"] == pytest.approx(40.0)

    def test_negative_net_energy(self, spark):
        df = make_df(spark, [Row(site_id="s2", timestamp=_TS, energy_generated_kwh=30.0, energy_consumed_kwh=80.0)])
        assert calculate_net_energy(df).collect()[0]["net_energy_kwh"] == pytest.approx(-50.0)

    def test_zero_net_energy(self, spark):
        df = make_df(spark, [Row(site_id="s3", timestamp=_TS, energy_generated_kwh=55.5, energy_consumed_kwh=55.5)])
        assert calculate_net_energy(df).collect()[0]["net_energy_kwh"] == pytest.approx(0.0)

    def test_multiple_rows(self, spark):
        rows = [
            Row(site_id="A", timestamp=_TS, energy_generated_kwh=200.0, energy_consumed_kwh=150.0),
            Row(site_id="B", timestamp=_TS, energy_generated_kwh=10.0, energy_consumed_kwh=25.0),
        ]
        result = {r["site_id"]: r["net_energy_kwh"] for r in calculate_net_energy(make_df(spark, rows)).collect()}
        assert result["A"] == pytest.approx(50.0)
        assert result["B"] == pytest.approx(-15.0)

    def test_column_type_is_double(self, spark):
        df = make_df(spark, [Row(site_id="s1", timestamp=_TS, energy_generated_kwh=1.0, energy_consumed_kwh=1.0)])
        field = next(f for f in calculate_net_energy(df).schema.fields if f.name == "net_energy_kwh")
        assert isinstance(field.dataType, DoubleType)


class TestFlagNegativeEnergy:
    def test_flag_when_generated_negative(self, spark):
        df = make_df(spark, [Row(site_id="s1", timestamp=_TS, energy_generated_kwh=-5.0, energy_consumed_kwh=10.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 1

    def test_flag_when_consumed_negative(self, spark):
        df = make_df(spark, [Row(site_id="s2", timestamp=_TS, energy_generated_kwh=10.0, energy_consumed_kwh=-3.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 1

    def test_flag_when_both_negative(self, spark):
        df = make_df(spark, [Row(site_id="s3", timestamp=_TS, energy_generated_kwh=-1.0, energy_consumed_kwh=-1.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 1

    def test_no_flag_all_non_negative(self, spark):
        df = make_df(spark, [Row(site_id="s4", timestamp=_TS, energy_generated_kwh=50.0, energy_consumed_kwh=30.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 0

    def test_no_flag_both_zero(self, spark):
        df = make_df(spark, [Row(site_id="s5", timestamp=_TS, energy_generated_kwh=0.0, energy_consumed_kwh=0.0)])
        assert flag_negative_energy(df).collect()[0]["negative_energy_flag"] == 0

    def test_mixed_batch(self, spark):
        rows = [
            Row(site_id="ok",  timestamp=_TS, energy_generated_kwh=100.0, energy_consumed_kwh=80.0),
            Row(site_id="bad", timestamp=_TS, energy_generated_kwh=-10.0, energy_consumed_kwh=5.0),
        ]
        result = {r["site_id"]: r["negative_energy_flag"] for r in flag_negative_energy(make_df(spark, rows)).collect()}
        assert result["ok"] == 0
        assert result["bad"] == 1

    def test_flag_column_type_is_integer(self, spark):
        df = make_df(spark, [Row(site_id="s1", timestamp=_TS, energy_generated_kwh=1.0, energy_consumed_kwh=1.0)])
        field = next(f for f in flag_negative_energy(df).schema.fields if f.name == "negative_energy_flag")
        assert isinstance(field.dataType, IntegerType)


class TestTransformPipeline:
    def test_pipeline_adds_both_columns(self, spark):
        df = make_df(spark, [Row(site_id="s1", timestamp=_TS, energy_generated_kwh=80.0, energy_consumed_kwh=50.0)])
        cols = transform(df).columns
        assert "net_energy_kwh" in cols
        assert "negative_energy_flag" in cols

    def test_pipeline_positive_case(self, spark):
        df = make_df(spark, [Row(site_id="s1", timestamp=_TS, energy_generated_kwh=120.0, energy_consumed_kwh=70.0)])
        row = transform(df).collect()[0]
        assert row["net_energy_kwh"] == pytest.approx(50.0)
        assert row["negative_energy_flag"] == 0

    def test_pipeline_negative_generated(self, spark):
        df = make_df(spark, [Row(site_id="s2", timestamp=_TS, energy_generated_kwh=-20.0, energy_consumed_kwh=30.0)])
        row = transform(df).collect()[0]
        assert row["net_energy_kwh"] == pytest.approx(-50.0)
        assert row["negative_energy_flag"] == 1

    def test_pipeline_preserves_clean_rows(self, spark):
        rows = [Row(site_id=f"s{i}", timestamp=_TS, energy_generated_kwh=float(i*10), energy_consumed_kwh=float(i*5)) for i in range(1, 6)]
        assert transform(make_df(spark, rows)).count() == 5