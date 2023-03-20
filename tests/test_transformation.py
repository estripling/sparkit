import pyspark.sql.functions as F
from pyspark.sql import Row
from tests.conftest import assert_dataframe_equal

import sparkit


def test_add_prefix(spark):
    df = spark.createDataFrame([Row(x=1, y=2)])

    # all columns
    actual = sparkit.add_prefix(df, "prefix_")
    excepted = spark.createDataFrame([Row(prefix_x=1, prefix_y=2)])
    assert_dataframe_equal(actual, excepted)

    # with column selection
    actual = sparkit.add_prefix(df, "prefix_", ["x"])
    excepted = spark.createDataFrame([Row(prefix_x=1, y=2)])
    assert_dataframe_equal(actual, excepted)

    # used as transformation function
    actual = df.transform(sparkit.add_prefix(prefix="prefix_", subset=["x"]))
    excepted = spark.createDataFrame([Row(prefix_x=1, y=2)])
    assert_dataframe_equal(actual, excepted)


def test_add_suffix(spark):
    df = spark.createDataFrame([Row(x=1, y=2)])

    # all columns
    actual = sparkit.add_suffix(df, "_suffix")
    excepted = spark.createDataFrame([Row(x_suffix=1, y_suffix=2)])
    assert_dataframe_equal(actual, excepted)

    # with column selection
    actual = sparkit.add_suffix(df, "_suffix", ["x"])
    excepted = spark.createDataFrame([Row(x_suffix=1, y=2)])
    assert_dataframe_equal(actual, excepted)

    # used as transformation function
    actual = df.transform(sparkit.add_suffix(suffix="_suffix", subset=["x"]))
    excepted = spark.createDataFrame([Row(x_suffix=1, y=2)])
    assert_dataframe_equal(actual, excepted)


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
    assert_dataframe_equal(actual, excepted)


def test_freq(spark):
    # single column
    frequencies = {"a": 3, "b": 1, "c": 1, "g": 2, "h": 1}
    df = spark.createDataFrame(
        [Row(x=v) for v, f in frequencies.items() for _ in range(f)]
    )

    excepted = spark.createDataFrame(
        [
            Row(x="a", frq=3, cml_frq=3, rel_frq=0.375, rel_cml_frq=0.375),
            Row(x="g", frq=2, cml_frq=5, rel_frq=0.25, rel_cml_frq=0.625),
            Row(x="b", frq=1, cml_frq=6, rel_frq=0.125, rel_cml_frq=0.75),
            Row(x="c", frq=1, cml_frq=7, rel_frq=0.125, rel_cml_frq=0.875),
            Row(x="h", frq=1, cml_frq=8, rel_frq=0.125, rel_cml_frq=1.0),
        ]
    )

    for columns in ["x", ["x"]]:
        actual = sparkit.freq(df, columns)
        assert_dataframe_equal(actual, excepted)

    # used as transformation function
    actual = df.transform(sparkit.freq(columns=["x"]))
    assert_dataframe_equal(actual, excepted)

    # multiple columns
    df = spark.createDataFrame(
        [
            Row(x="a", y=1),
            Row(x="c", y=1),
            Row(x="b", y=1),
            Row(x="g", y=2),
            Row(x="h", y=1),
            Row(x="a", y=1),
            Row(x="g", y=2),
            Row(x="a", y=2),
        ]
    )
    actual = sparkit.freq(df, ["x"])  # check single column again
    assert_dataframe_equal(actual, excepted)

    actual = sparkit.freq(df, ["x", "y"])
    excepted = spark.createDataFrame(
        [
            Row(x="a", y=1, frq=2, cml_frq=2, rel_frq=0.25, rel_cml_frq=0.25),
            Row(x="g", y=2, frq=2, cml_frq=4, rel_frq=0.25, rel_cml_frq=0.5),
            Row(x="a", y=2, frq=1, cml_frq=5, rel_frq=0.125, rel_cml_frq=0.625),
            Row(x="b", y=1, frq=1, cml_frq=6, rel_frq=0.125, rel_cml_frq=0.75),
            Row(x="c", y=1, frq=1, cml_frq=7, rel_frq=0.125, rel_cml_frq=0.875),
            Row(x="h", y=1, frq=1, cml_frq=8, rel_frq=0.125, rel_cml_frq=1.0),
        ]
    )
    assert_dataframe_equal(actual, excepted)


def test_join(spark):
    df1 = spark.createDataFrame([Row(id=1, x="a"), Row(id=2, x="b")])
    df2 = spark.createDataFrame([Row(id=1, y="c"), Row(id=2, y="d")])
    df3 = spark.createDataFrame([Row(id=1, z="e"), Row(id=2, z="f")])

    actual = sparkit.join(df1, df2, df3, on="id")
    excepted = df1.join(df2, "id").join(df3, "id")
    assert_dataframe_equal(actual, excepted)


def test_peek(spark):
    df = spark.createDataFrame([Row(x=1, y="a"), Row(x=3, y=None), Row(x=None, y="c")])
    actual = (
        df.transform(sparkit.peek(schema=True))
        .where(F.col("x").isNotNull())
        .transform(sparkit.peek)
    )
    excepted = df.where(F.col("x").isNotNull())
    assert_dataframe_equal(actual, excepted)


def test_union(spark):
    df1 = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    df2 = spark.createDataFrame([Row(x=5, y=6), Row(x=7, y=8)])
    df3 = spark.createDataFrame([Row(x=0, y=1), Row(x=2, y=3)])

    actual = sparkit.union(df1, df2, df3)
    excepted = df1.unionByName(df2).unionByName(df3)
    assert_dataframe_equal(actual, excepted)
