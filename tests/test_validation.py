import pytest
from pyspark.sql import Row

import sparkit


def test_check_schema_equal(spark):
    lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])

    rgt_df__equal = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    rgt_df__different_type = spark.createDataFrame([Row(x=1, y="2"), Row(x=3, y="4")])
    rgt_df__different_size = spark.createDataFrame([Row(x=1), Row(x=3)])

    sparkit.check_schema_equal(lft_df, rgt_df__equal)

    with pytest.raises(sparkit.exception.SchemaMismatchError):
        sparkit.check_schema_equal(lft_df, rgt_df__different_type)

    with pytest.raises(sparkit.exception.SchemaMismatchError):
        sparkit.check_schema_equal(lft_df, rgt_df__different_size)


def test_is_schema_equal(spark):
    lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])

    rgt_df__equal = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    rgt_df__different_type = spark.createDataFrame([Row(x=1, y="2"), Row(x=3, y="4")])
    rgt_df__different_size = spark.createDataFrame([Row(x=1), Row(x=3)])

    assert sparkit.is_schema_equal(lft_df, rgt_df__equal)
    assert not sparkit.is_schema_equal(lft_df, rgt_df__different_type)
    assert not sparkit.is_schema_equal(lft_df, rgt_df__different_size)
