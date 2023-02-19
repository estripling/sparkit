from importlib import metadata

from pyspark.sql import SparkSession

import sparkit


def test_version():
    assert sparkit.__version__ == metadata.version("sparkit")


def test_spark_session(spark):
    assert isinstance(spark, SparkSession)
