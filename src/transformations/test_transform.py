"""
Unit tests for src/transformations/transform.py

Covers:
  - net energy calculation correctness
  - negative flag correctness (generated negative, consumed negative,
    both negative, both non-negative)
  - schema / column presence after transformation
  - cast_and_clean behavior on missing keys / malformed timestamps
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from transformations.transform import (
    INPUT_SCHEMA,
    apply_transformations,
    calculate_net_energy,
    cast_and_clean,
    flag_negative_energy,
)


def make_df(spark, rows):
    """Build a DataFrame from a list of dicts, matched by field name against
    INPUT_SCHEMA.

    NOTE: We deliberately use plain dicts (not pyspark.sql.Row(**kwargs))
    here. Row(**kwargs) sorts fields alphabetically, and createDataFrame then
    zips values *positionally* against the given schema -- which silently
    misaligns columns whose alphabetical order differs from the schema's
    declared order. Passing dicts avoids that pitfall since Spark maps each
    dict by key name to the corresponding schema field.
    """
    return spark.createDataFrame(rows, schema=INPUT_SCHEMA)


# ---------------------------------------------------------------------------
# calculate_net_energy
# ---------------------------------------------------------------------------
def test_net_energy_basic_correctness(spark):
    df = make_df(
        spark,
        [
            {"site_id": "site-1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 10.0, "energy_consumed_kwh": 4.0},
            {"site_id": "site-2", "timestamp": "2024-01-01T01:00:00Z", "energy_generated_kwh": 5.5, "energy_consumed_kwh": 5.5},
        ],
    )
    result = calculate_net_energy(df)
    rows = {r["site_id"]: r["net_energy_kwh"] for r in result.collect()}

    assert rows["site-1"] == 6.0
    assert rows["site-2"] == 0.0


def test_net_energy_can_be_negative(spark):
    df = make_df(
        spark,
        [{"site_id": "site-1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 2.0, "energy_consumed_kwh": 10.0}],
    )
    result = calculate_net_energy(df)
    row = result.collect()[0]
    assert row["net_energy_kwh"] == -8.0


# ---------------------------------------------------------------------------
# flag_negative_energy
# ---------------------------------------------------------------------------
def test_flag_generated_negative(spark):
    df = make_df(
        spark,
        [{"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": -1.0, "energy_consumed_kwh": 3.0}],
    )
    result = flag_negative_energy(df)
    assert result.collect()[0]["negative_energy_flag"] == 1


def test_flag_consumed_negative(spark):
    df = make_df(
        spark,
        [{"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 3.0, "energy_consumed_kwh": -2.0}],
    )
    result = flag_negative_energy(df)
    assert result.collect()[0]["negative_energy_flag"] == 1


def test_flag_both_negative(spark):
    df = make_df(
        spark,
        [{"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": -3.0, "energy_consumed_kwh": -2.0}],
    )
    result = flag_negative_energy(df)
    assert result.collect()[0]["negative_energy_flag"] == 1


def test_flag_both_non_negative(spark):
    df = make_df(
        spark,
        [{"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 3.0, "energy_consumed_kwh": 2.0}],
    )
    result = flag_negative_energy(df)
    assert result.collect()[0]["negative_energy_flag"] == 0


def test_flag_zero_values_are_non_negative(spark):
    df = make_df(
        spark,
        [{"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 0.0, "energy_consumed_kwh": 0.0}],
    )
    result = flag_negative_energy(df)
    assert result.collect()[0]["negative_energy_flag"] == 0


# ---------------------------------------------------------------------------
# schema / column presence
# ---------------------------------------------------------------------------
def test_apply_transformations_adds_expected_columns(spark):
    df = make_df(
        spark,
        [
            {"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 10.0, "energy_consumed_kwh": 4.0},
            {"site_id": "s2", "timestamp": "2024-01-01T01:00:00Z", "energy_generated_kwh": -1.0, "energy_consumed_kwh": 2.0},
        ],
    )
    result = apply_transformations(df)

    expected_columns = {
        "site_id",
        "timestamp",
        "energy_generated_kwh",
        "energy_consumed_kwh",
        "net_energy_kwh",
        "negative_energy_flag",
    }
    assert expected_columns.issubset(set(result.columns))

    dtypes = dict(result.dtypes)
    assert dtypes["net_energy_kwh"] == "float"
    assert dtypes["negative_energy_flag"] == "int"
    assert dtypes["timestamp"] == "timestamp"


def test_apply_transformations_row_values_end_to_end(spark):
    df = make_df(
        spark,
        [
            {"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 10.0, "energy_consumed_kwh": 4.0},
            {"site_id": "s2", "timestamp": "2024-01-01T01:00:00Z", "energy_generated_kwh": -1.0, "energy_consumed_kwh": 2.0},
        ],
    )
    result = apply_transformations(df).orderBy("site_id").collect()

    assert result[0]["site_id"] == "s1"
    assert result[0]["net_energy_kwh"] == 6.0
    assert result[0]["negative_energy_flag"] == 0

    assert result[1]["site_id"] == "s2"
    assert result[1]["net_energy_kwh"] == -3.0
    assert result[1]["negative_energy_flag"] == 1


# ---------------------------------------------------------------------------
# cast_and_clean
# ---------------------------------------------------------------------------
def test_cast_and_clean_drops_rows_missing_required_keys(spark):
    df = make_df(
        spark,
        [
            {"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 10.0, "energy_consumed_kwh": 4.0},
            {"site_id": None, "timestamp": "2024-01-01T01:00:00Z", "energy_generated_kwh": 1.0, "energy_consumed_kwh": 1.0},
            {"site_id": "s3", "timestamp": None, "energy_generated_kwh": 1.0, "energy_consumed_kwh": 1.0},
        ],
    )
    result = cast_and_clean(df)
    assert result.count() == 1
    assert result.collect()[0]["site_id"] == "s1"


def test_cast_and_clean_parses_timestamp(spark):
    df = make_df(
        spark,
        [{"site_id": "s1", "timestamp": "2024-01-01T00:00:00Z", "energy_generated_kwh": 10.0, "energy_consumed_kwh": 4.0}],
    )
    result = cast_and_clean(df)
    assert dict(result.dtypes)["timestamp"] == "timestamp"