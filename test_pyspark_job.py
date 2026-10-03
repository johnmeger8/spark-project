import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_clean_data")
        .getOrCreate()
    )

def test_valid_records_are_kept(spark):
    df = spark.createDataFrame([
        ("Ahmed", 100.0),
        ("Sara", 200.0),
    ], ["name", "amount"])

    result = clean_data(df)

    assert result.count() == 2

def test_records_with_non_positive_amount_are_removed(spark):
    df = spark.createDataFrame([
        ("Ahmed", 100.0),
        ("Sara", 0.0),
        ("Omar", -50.0),
    ], ["name", "amount"])

    result = clean_data(df)

    assert result.count() == 1

def test_records_with_null_names_are_removed(spark):
    df = spark.createDataFrame([
        ("Ahmed", 100.0),
        (None, 200.0),
    ], ["name", "amount"])

    result = clean_data(df)

    assert result.count() == 1

def test_amount_with_tax_is_calculated_correctly(spark):
    df = spark.createDataFrame([
        ("Ahmed", 100.0),
    ], ["name", "amount"])

    result = clean_data(df)

    row = result.collect()[0]

    assert row["amount_with_tax"] == pytest.approx(120.0)
