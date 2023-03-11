from pyspark.sql import Row

import sparkit


def test_union(spark):
    df1 = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    df2 = spark.createDataFrame([Row(x=5, y=6), Row(x=7, y=8)])
    df3 = spark.createDataFrame([Row(x=0, y=1), Row(x=2, y=3)])

    actual = sparkit.union(df1, df2, df3)
    excepted = df1.unionByName(df2).unionByName(df3)

    assert sparkit.is_dataframe_equal(actual, excepted)
