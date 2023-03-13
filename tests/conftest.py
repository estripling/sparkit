import pytest
from pyspark.sql import DataFrame, SparkSession


def assert_dataframe_equal(lft_df: DataFrame, rgt_df: DataFrame):
    """Assert that the left and right data frames are equal."""
    lft_schema = lft_df.schema.simpleString()
    rgt_schema = rgt_df.schema.simpleString()
    assert lft_schema == rgt_schema, "schema mismatch"

    assert lft_df.count() == rgt_df.count(), "row count mismatch"

    lft_rows = lft_df.subtract(rgt_df)
    rgt_rows = rgt_df.subtract(lft_df)
    assert (lft_rows.count() == 0) and (rgt_rows.count() == 0), "row mismatch"


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("spark-session-for-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
