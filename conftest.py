"""
Shared pytest fixtures for the manufacturing ETL pipeline test suite.
Provides a local SparkSession so tests run without a real Databricks cluster.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-manufacturing-etl")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()
