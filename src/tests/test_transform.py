"""
Unit tests for Energy ETL Pipeline transformations.
Runnable locally without AWS credentials using a local SparkSession.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, DoubleType
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("EnergyETL_UnitTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def net_energy_fn():
    def _fn(df):
        return df.withColumn("net_energy_kwh", F.col("energy_generated_kwh") - F.col("energy_consumed_kwh"))
    return _fn


@pytest.fixture(scope="session")
def flag_fn():
    def _fn(df):
        return df.withColumn(
            "negative_energy_flag",
            F.when((F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0), F.lit(1)).otherwise(F.lit(0))
        )
    return _fn


SCHEMA = StructType([
    StructField("site_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=True),
    StructField("energy_generated_kwh", FloatType(), nullable=True),
    StructField("energy_consumed_kwh", FloatType(), nullable=True),
])


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


class TestCalculateNetEnergy:
    def test_basic(self, spark, net_energy_fn):
        row = net_energy_fn(make_df(spark, [("s1", "2024-01-01", 100.0, 60.0)])).collect()[0]
        assert abs(row["net_energy_kwh"] - 40.0) < 1e-4

    def test_zero(self, spark, net_energy_fn):
        row = net_energy_fn(make_df(spark, [("s2", "2024-01-01", 50.0, 50.0)])).collect()[0]
        assert abs(row["net_energy_kwh"]) < 1e-4

    def test_negative_result(self, spark, net_energy_fn):
        row = net_energy_fn(make_df(spark, [("s3", "2024-01-01", 30.0, 80.0)])).collect()[0]
        assert abs(row["net_energy_kwh"] - (-50.0)) < 1e-4

    def test_null_generated(self, spark, net_energy_fn):
        df = spark.createDataFrame([("s4", "2024-01-01", None, 50.0)], schema=SCHEMA)
        assert net_energy_fn(df).collect()[0]["net_energy_kwh"] is None

    def test_null_consumed(self, spark, net_energy_fn):
        df = spark.createDataFrame([("s5", "2024-01-01", 100.0, None)], schema=SCHEMA)
        assert net_energy_fn(df).collect()[0]["net_energy_kwh"] is None

    def test_column_exists(self, spark, net_energy_fn):
        assert "net_energy_kwh" in net_energy_fn(make_df(spark, [("s6", "2024-01-01", 10.0, 5.0)])).columns


class TestFlagNegativeEnergy:
    def test_both_positive(self, spark, flag_fn):
        assert flag_fn(make_df(spark, [("s10", "2024-01-01", 100.0, 60.0)])).collect()[0]["negative_energy_flag"] == 0

    def test_both_negative(self, spark, flag_fn):
        assert flag_fn(make_df(spark, [("s11", "2024-01-01", -50.0, -30.0)])).collect()[0]["negative_energy_flag"] == 1

    def test_generated_negative(self, spark, flag_fn):
        assert flag_fn(make_df(spark, [("s12", "2024-01-01", -10.0, 50.0)])).collect()[0]["negative_energy_flag"] == 1

    def test_consumed_negative(self, spark, flag_fn):
        assert flag_fn(make_df(spark, [("s13", "2024-01-01", 80.0, -20.0)])).collect()[0]["negative_energy_flag"] == 1

    def test_zero_values(self, spark, flag_fn):
        assert flag_fn(make_df(spark, [("s14", "2024-01-01", 0.0, 0.0)])).collect()[0]["negative_energy_flag"] == 0

    def test_null_treated_as_non_negative(self, spark, flag_fn):
        df = spark.createDataFrame([("s15", "2024-01-01", None, None)], schema=SCHEMA)
        assert flag_fn(df).collect()[0]["negative_energy_flag"] == 0


class TestSchemaValidation:
    def test_all_columns_present(self, spark, net_energy_fn, flag_fn):
        df = flag_fn(net_energy_fn(make_df(spark, [("s20", "2024-01-01", 100.0, 60.0)])))
        expected = {"site_id", "timestamp", "energy_generated_kwh", "energy_consumed_kwh", "net_energy_kwh", "negative_energy_flag"}
        assert expected.issubset(set(df.columns))

    def test_net_energy_type(self, spark, net_energy_fn):
        df = net_energy_fn(make_df(spark, [("s21", "2024-01-01", 100.0, 60.0)]))
        field = next(f for f in df.schema.fields if f.name == "net_energy_kwh")
        assert field.dataType in (FloatType(), DoubleType())

    def test_flag_type(self, spark, flag_fn):
        df = flag_fn(make_df(spark, [("s22", "2024-01-01", 100.0, 60.0)]))
        field = next(f for f in df.schema.fields if f.name == "negative_energy_flag")
        assert field.dataType == IntegerType()

    def test_row_count_preserved(self, spark, net_energy_fn, flag_fn):
        rows = [("s23", "2024-01-01", 10.0, 5.0), ("s24", "2024-01-01", 20.0, 15.0)]
        df = flag_fn(net_energy_fn(make_df(spark, rows)))
        assert df.count() == 2

    def test_combined_correctness(self, spark, net_energy_fn, flag_fn):
        df = flag_fn(net_energy_fn(make_df(spark, [("s25", "2024-01-01", -10.0, 50.0)])))
        row = df.collect()[0]
        assert abs(row["net_energy_kwh"] - (-60.0)) < 1e-4
        assert row["negative_energy_flag"] == 1