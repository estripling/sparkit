import pytest
from pyspark.sql import Row

import sparkit


def test_check_row_count_equal(spark):
    lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])

    rgt_df__equal = spark.createDataFrame([Row(x=1), Row(x=3)])
    rgt_df__different = spark.createDataFrame([Row(x=1)])

    assert sparkit.check_row_count_equal(lft_df, rgt_df__equal) is None

    with pytest.raises(sparkit.exception.RowCountMismatchError):
        sparkit.check_row_count_equal(lft_df, rgt_df__different)


def test_check_row_equal(spark):
    lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])

    rgt_df__equal = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    rgt_df__different = spark.createDataFrame([Row(x=1, y=7), Row(x=3, y=9)])

    assert sparkit.check_row_equal(lft_df, rgt_df__equal) is None

    with pytest.raises(sparkit.exception.RowMismatchError):
        sparkit.check_row_equal(lft_df, rgt_df__different)


def test_check_schema_equal(spark):
    lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])

    rgt_df__equal = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    rgt_df__different_type = spark.createDataFrame([Row(x=1, y="2"), Row(x=3, y="4")])
    rgt_df__different_size = spark.createDataFrame([Row(x=1), Row(x=3)])

    assert sparkit.check_schema_equal(lft_df, rgt_df__equal) is None

    with pytest.raises(sparkit.exception.SchemaMismatchError):
        sparkit.check_schema_equal(lft_df, rgt_df__different_type)

    with pytest.raises(sparkit.exception.SchemaMismatchError):
        sparkit.check_schema_equal(lft_df, rgt_df__different_size)


def test_is_row_count_equal(spark):
    lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])

    rgt_df__equal = spark.createDataFrame([Row(x=1), Row(x=3)])
    rgt_df__different = spark.createDataFrame([Row(x=1)])

    assert sparkit.is_row_count_equal(lft_df, rgt_df__equal)
    assert not sparkit.is_row_count_equal(lft_df, rgt_df__different)


def test_is_row_equal(spark):
    lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])

    rgt_df__equal = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    rgt_df__different = spark.createDataFrame([Row(x=1, y=7), Row(x=3, y=9)])

    assert sparkit.is_row_equal(lft_df, rgt_df__equal)
    assert not sparkit.is_row_equal(lft_df, rgt_df__different)


def test_is_schema_equal(spark):
    lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])

    rgt_df__equal = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    rgt_df__different_type = spark.createDataFrame([Row(x=1, y="2"), Row(x=3, y="4")])
    rgt_df__different_size = spark.createDataFrame([Row(x=1), Row(x=3)])

    assert sparkit.is_schema_equal(lft_df, rgt_df__equal)
    assert not sparkit.is_schema_equal(lft_df, rgt_df__different_type)
    assert not sparkit.is_schema_equal(lft_df, rgt_df__different_size)
