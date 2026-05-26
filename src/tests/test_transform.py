"""
Unit tests for src/transformations/transform.py

Run with:
    pytest src/tests/test_transform.py -v

Requires:
    pip install pyspark pytest
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "transformations"))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from transform import (
    INPUT_SCHEMA,
    validate_schema,
    cast_and_clean,
    calculate_net_energy,
    flag_negative_energy,
    apply_transformations,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("EnergyETL-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


SAMPLE_ROWS = [
    ("site_A", "2024-01-01T00:00:00+00:00",  10.0,  6.0),
    ("site_B", "2024-01-01T01:00:00+00:00",   5.0,  8.0),
    ("site_C", "2024-01-01T02:00:00+00:00",  -1.0,  3.0),
    ("site_D", "2024-01-01T03:00:00+00:00",   4.0, -2.0),
    ("site_E", "2024-01-01T04:00:00+00:00",  -3.0, -1.0),
    ("site_F", "2024-01-01T05:00:00+00:00",   0.0,  0.0),
]

def make_df(spark, rows=None):
    return spark.createDataFrame(rows or SAMPLE_ROWS, schema=INPUT_SCHEMA)


class TestValidateSchema:
    def test_passes_with_valid_schema(self, spark):
        validate_schema(make_df(spark))

    def test_raises_on_missing_column(self, spark):
        with pytest.raises(ValueError, match="missing required columns"):
            validate_schema(make_df(spark).drop("site_id"))


class TestCastAndClean:
    def test_timestamp_cast(self, spark):
        df = cast_and_clean(make_df(spark))
        assert dict(df.dtypes)["timestamp"] == "timestamp"

    def test_null_rows_dropped(self, spark):
        rows = [(None, "2024-01-01T00:00:00+00:00", 10.0, 6.0),
                ("site_A", "2024-01-01T01:00:00+00:00", 5.0, 8.0)]
        df = cast_and_clean(spark.createDataFrame(rows, schema=INPUT_SCHEMA))
        assert df.count() == 1

    def test_all_valid_rows_retained(self, spark):
        assert cast_and_clean(make_df(spark)).count() == len(SAMPLE_ROWS)


class TestCalculateNetEnergy:
    def test_column_added(self, spark):
        df = calculate_net_energy(cast_and_clean(make_df(spark)))
        assert "net_energy_kwh" in df.columns

    def test_positive_net(self, spark):
        rows = [("site_X", "2024-01-01T00:00:00+00:00", 10.0, 6.0)]
        df = calculate_net_energy(cast_and_clean(spark.createDataFrame(rows, schema=INPUT_SCHEMA)))
        assert df.first()["net_energy_kwh"] == pytest.approx(4.0)

    def test_negative_net(self, spark):
        rows = [("site_X", "2024-01-01T00:00:00+00:00", 5.0, 8.0)]
        df = calculate_net_energy(cast_and_clean(spark.createDataFrame(rows, schema=INPUT_SCHEMA)))
        assert df.first()["net_energy_kwh"] == pytest.approx(-3.0)

    def test_formula_all_rows(self, spark):
        df = calculate_net_energy(cast_and_clean(make_df(spark)))
        for row in df.collect():
            assert row["net_energy_kwh"] == pytest.approx(
                row["energy_generated_kwh"] - row["energy_consumed_kwh"]
            )


class TestFlagNegativeEnergy:
    def _flag(self, spark, gen, con):
        rows = [("site_X", "2024-01-01T00:00:00+00:00", gen, con)]
        df = flag_negative_energy(cast_and_clean(spark.createDataFrame(rows, schema=INPUT_SCHEMA)))
        return df.first()["negative_energy_flag"]

    def test_column_added(self, spark):
        assert "negative_energy_flag" in flag_negative_energy(cast_and_clean(make_df(spark))).columns

    def test_flag_zero_both_positive(self, spark):  assert self._flag(spark, 10.0,  6.0) == 0
    def test_flag_zero_both_zero(self, spark):      assert self._flag(spark,  0.0,  0.0) == 0
    def test_flag_one_gen_negative(self, spark):    assert self._flag(spark, -1.0,  3.0) == 1
    def test_flag_one_con_negative(self, spark):    assert self._flag(spark,  4.0, -2.0) == 1
    def test_flag_one_both_negative(self, spark):   assert self._flag(spark, -3.0, -1.0) == 1

    def test_flag_values_all_rows(self, spark):
        expected = {"site_A": 0, "site_B": 0, "site_C": 1, "site_D": 1, "site_E": 1, "site_F": 0}
        df = flag_negative_energy(cast_and_clean(make_df(spark)))
        for row in df.collect():
            assert row["negative_energy_flag"] == expected[row["site_id"]]


class TestApplyTransformations:
    def test_all_columns_present(self, spark):
        df = apply_transformations(make_df(spark))
        assert {"net_energy_kwh", "negative_energy_flag"}.issubset(set(df.columns))

    def test_row_count_unchanged(self, spark):
        assert apply_transformations(make_df(spark)).count() == len(SAMPLE_ROWS)

    def test_combined_correctness(self, spark):
        rows = [("site_Z", "2024-01-01T00:00:00+00:00", -5.0, 3.0)]
        df = apply_transformations(spark.createDataFrame(rows, schema=INPUT_SCHEMA))
        row = df.first()
        assert row["net_energy_kwh"]       == pytest.approx(-8.0)
        assert row["negative_energy_flag"] == 1