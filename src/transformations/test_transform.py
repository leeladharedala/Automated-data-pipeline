"""
Unit tests for src/transformations/transform.py

Covers:
  - net_energy_kwh calculation correctness
  - negative_energy_flag correctness for:
      * energy_generated_kwh < 0 (only)
      * energy_consumed_kwh < 0 (only)
      * both negative
      * both non-negative
  - null-handling behavior in cast_and_clean
  - end-to-end run_transformations pipeline
"""

import sys
import os
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "src", "transformations")
)

from transform import (  # noqa: E402
    RAW_SCHEMA,
    calculate_net_energy,
    cast_and_clean,
    flag_negative_energy,
    read_raw_data,
    run_transformations,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-energy-net-transform")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _make_df(spark, rows):
    """Helper to build a DataFrame matching RAW_SCHEMA."""
    return spark.createDataFrame(rows, RAW_SCHEMA)


def test_calculate_net_energy_correctness(spark):
    rows = [
        ("site-1", "2024-01-01T00:00:00Z", 10.0, 4.0),
        ("site-2", "2024-01-01T01:00:00Z", 5.5, 7.25),
    ]
    df = _make_df(spark, rows)
    result = calculate_net_energy(df).select("site_id", "net_energy_kwh").collect()
    result_map = {r["site_id"]: r["net_energy_kwh"] for r in result}

    assert result_map["site-1"] == pytest.approx(6.0)
    assert result_map["site-2"] == pytest.approx(-1.75)


def test_flag_negative_energy_generated_negative(spark):
    rows = [("site-1", "2024-01-01T00:00:00Z", -3.0, 2.0)]
    df = _make_df(spark, rows)
    result = flag_negative_energy(df).collect()[0]
    assert result["negative_energy_flag"] == 1


def test_flag_negative_energy_consumed_negative(spark):
    rows = [("site-1", "2024-01-01T00:00:00Z", 3.0, -2.0)]
    df = _make_df(spark, rows)
    result = flag_negative_energy(df).collect()[0]
    assert result["negative_energy_flag"] == 1


def test_flag_negative_energy_both_negative(spark):
    rows = [("site-1", "2024-01-01T00:00:00Z", -1.0, -2.0)]
    df = _make_df(spark, rows)
    result = flag_negative_energy(df).collect()[0]
    assert result["negative_energy_flag"] == 1


def test_flag_negative_energy_both_non_negative(spark):
    rows = [
        ("site-1", "2024-01-01T00:00:00Z", 0.0, 0.0),
        ("site-2", "2024-01-01T00:00:00Z", 12.3, 4.5),
    ]
    df = _make_df(spark, rows)
    results = flag_negative_energy(df).collect()
    for r in results:
        assert r["negative_energy_flag"] == 0


def test_cast_and_clean_fills_null_measurements(spark):
    rows = [("site-1", "2024-01-01T00:00:00Z", None, None)]
    df = _make_df(spark, rows)
    cleaned = cast_and_clean(df).collect()[0]
    assert cleaned["energy_generated_kwh"] == 0.0
    assert cleaned["energy_consumed_kwh"] == 0.0


def test_cast_and_clean_drops_null_site_id(spark):
    rows = [
        (None, "2024-01-01T00:00:00Z", 1.0, 1.0),
        ("site-1", "2024-01-01T00:00:00Z", 1.0, 1.0),
    ]
    df = _make_df(spark, rows)
    cleaned = cast_and_clean(df)
    assert cleaned.count() == 1
    assert cleaned.collect()[0]["site_id"] == "site-1"


def test_cast_and_clean_casts_timestamp(spark):
    rows = [("site-1", "2024-01-01T00:00:00Z", 1.0, 1.0)]
    df = _make_df(spark, rows)
    cleaned = cast_and_clean(df).collect()[0]
    expected = datetime(2024, 1, 1, 0, 0, 0)
    assert cleaned["timestamp"] == expected


def test_run_transformations_end_to_end(spark):
    rows = [
        ("site-1", "2024-01-01T00:00:00Z", 10.0, 3.0),
        ("site-2", "2024-01-01T01:00:00Z", -2.0, 1.0),
        ("site-3", "2024-01-01T02:00:00Z", 4.0, -5.0),
        ("site-4", "2024-01-01T03:00:00Z", -1.0, -1.0),
    ]
    df = _make_df(spark, rows)
    result = run_transformations(df)

    expected_cols = {
        "site_id",
        "timestamp",
        "energy_generated_kwh",
        "energy_consumed_kwh",
        "net_energy_kwh",
        "negative_energy_flag",
    }
    assert set(result.columns) == expected_cols

    by_site = {r["site_id"]: r for r in result.collect()}

    assert by_site["site-1"]["net_energy_kwh"] == pytest.approx(7.0)
    assert by_site["site-1"]["negative_energy_flag"] == 0

    assert by_site["site-2"]["net_energy_kwh"] == pytest.approx(-3.0)
    assert by_site["site-2"]["negative_energy_flag"] == 1

    assert by_site["site-3"]["net_energy_kwh"] == pytest.approx(9.0)
    assert by_site["site-3"]["negative_energy_flag"] == 1

    assert by_site["site-4"]["net_energy_kwh"] == pytest.approx(0.0)
    assert by_site["site-4"]["negative_energy_flag"] == 1


def test_read_raw_data_from_jsonl(spark, tmp_path):
    jsonl_content = (
        '{"site_id": "site-1", "timestamp": "2024-01-01T00:00:00Z", '
        '"energy_generated_kwh": 10.0, "energy_consumed_kwh": 3.0}\n'
        '{"site_id": "site-2", "timestamp": "2024-01-01T01:00:00Z", '
        '"energy_generated_kwh": -1.0, "energy_consumed_kwh": 2.0}\n'
    )
    input_file = tmp_path / "raw_data.jsonl"
    input_file.write_text(jsonl_content)

    df = read_raw_data(spark, str(input_file))
    assert df.count() == 2
    assert set(df.columns) == {
        "site_id",
        "timestamp",
        "energy_generated_kwh",
        "energy_consumed_kwh",
    }

    transformed = run_transformations(df)
    by_site = {r["site_id"]: r for r in transformed.collect()}
    assert by_site["site-1"]["negative_energy_flag"] == 0
    assert by_site["site-2"]["negative_energy_flag"] == 1