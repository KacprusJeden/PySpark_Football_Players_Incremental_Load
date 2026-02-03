import pytest
from pyspark.sql import SparkSession
import sys
import os

@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-scd2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
