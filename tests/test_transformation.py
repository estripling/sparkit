from pyspark.sql import Row

import sparkit


def test_count_nulls(spark):
    df = spark.createDataFrame(
        [
            Row(x=1, y=2, z=None),
            Row(x=4, y=None, z=6),
            Row(x=7, y=8, z=None),
            Row(x=10, y=None, z=None),
        ]
    )

    actual = sparkit.count_nulls(df, subset=["x", "z"])
    excepted = spark.createDataFrame([Row(x=0, z=3)])

    assert sparkit.is_dataframe_equal(actual, excepted)


def test_join(spark):
    df1 = spark.createDataFrame([Row(id=1, x="a"), Row(id=2, x="b")])
    df2 = spark.createDataFrame([Row(id=1, y="c"), Row(id=2, y="d")])
    df3 = spark.createDataFrame([Row(id=1, z="e"), Row(id=2, z="f")])

    actual = sparkit.join(df1, df2, df3, on="id")
    excepted = df1.join(df2, "id").join(df3, "id")

    assert sparkit.is_dataframe_equal(actual, excepted)


def test_union(spark):
    df1 = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    df2 = spark.createDataFrame([Row(x=5, y=6), Row(x=7, y=8)])
    df3 = spark.createDataFrame([Row(x=0, y=1), Row(x=2, y=3)])

    actual = sparkit.union(df1, df2, df3)
    excepted = df1.unionByName(df2).unionByName(df3)

    assert sparkit.is_dataframe_equal(actual, excepted)
