from datetime import date

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row
from tests.conftest import assert_dataframe_equal

import sparkit


def test_add_prefix(spark):
    df = spark.createDataFrame([Row(x=1, y=2)])

    # all columns
    actual = sparkit.add_prefix("prefix_", df)
    expected = spark.createDataFrame([Row(prefix_x=1, prefix_y=2)])
    assert_dataframe_equal(actual, expected)

    # with column selection
    actual = sparkit.add_prefix("prefix_", df, subset=["x"])
    expected = spark.createDataFrame([Row(prefix_x=1, y=2)])
    assert_dataframe_equal(actual, expected)

    # used as transformation function
    actual = df.transform(sparkit.add_prefix("prefix_", subset=["x"]))
    expected = spark.createDataFrame([Row(prefix_x=1, y=2)])
    assert_dataframe_equal(actual, expected)


def test_add_suffix(spark):
    df = spark.createDataFrame([Row(x=1, y=2)])

    # all columns
    actual = sparkit.add_suffix("_suffix", df)
    expected = spark.createDataFrame([Row(x_suffix=1, y_suffix=2)])
    assert_dataframe_equal(actual, expected)

    # with column selection
    actual = sparkit.add_suffix("_suffix", df, subset=["x"])
    expected = spark.createDataFrame([Row(x_suffix=1, y=2)])
    assert_dataframe_equal(actual, expected)

    # used as transformation function
    actual = df.transform(sparkit.add_suffix("_suffix", subset=["x"]))
    expected = spark.createDataFrame([Row(x_suffix=1, y=2)])
    assert_dataframe_equal(actual, expected)


def test_count_nulls(spark):
    df = spark.createDataFrame(
        [
            Row(x=1, y=2, z=None),
            Row(x=4, y=None, z=6),
            Row(x=7, y=8, z=None),
            Row(x=10, y=None, z=None),
        ]
    )

    actual = sparkit.count_nulls(df)
    expected = spark.createDataFrame([Row(x=0, y=2, z=3)])
    assert_dataframe_equal(actual, expected)

    actual = df.transform(sparkit.count_nulls)
    assert_dataframe_equal(actual, expected)

    actual = sparkit.count_nulls(df, subset=["x", "z"])
    expected = spark.createDataFrame([Row(x=0, z=3)])
    assert_dataframe_equal(actual, expected)

    actual = df.transform(sparkit.count_nulls(subset=["x", "z"]))
    assert_dataframe_equal(actual, expected)


def test_daterange(spark):
    df = spark.createDataFrame([Row(id=1), Row(id=3), Row(id=2), Row(id=2), Row(id=3)])
    expected = spark.createDataFrame(
        [Row(id=i, day=date(2023, 5, d)) for i in [1, 2, 3] for d in range(1, 8)]
    )

    sparkit.daterange()
    actual = sparkit.daterange(
        "id",
        "day",
        df,
        min_date="2023-05-01",
        max_date="2023-05-07",
    )
    assert_dataframe_equal(actual, expected)

    actual = df.transform(
        sparkit.daterange(
            "id",
            "day",
            min_date=date(2023, 5, 1),
            max_date=date(2023, 5, 7),
        )
    )
    assert_dataframe_equal(actual, expected)


def test_freq(spark):
    # single column
    frequencies = {"a": 3, "b": 1, "c": 1, "g": 2, "h": 1}
    df = spark.createDataFrame(
        [Row(x=v) for v, f in frequencies.items() for _ in range(f)]
    )

    expected = spark.createDataFrame(
        [
            Row(x="a", frq=3, cml_frq=3, rel_frq=0.375, rel_cml_frq=0.375),
            Row(x="g", frq=2, cml_frq=5, rel_frq=0.25, rel_cml_frq=0.625),
            Row(x="b", frq=1, cml_frq=6, rel_frq=0.125, rel_cml_frq=0.75),
            Row(x="c", frq=1, cml_frq=7, rel_frq=0.125, rel_cml_frq=0.875),
            Row(x="h", frq=1, cml_frq=8, rel_frq=0.125, rel_cml_frq=1.0),
        ]
    )

    for columns in ["x", ["x"]]:
        actual = sparkit.freq(columns, df)
        assert_dataframe_equal(actual, expected)

    # used as transformation function
    actual = df.transform(sparkit.freq(["x"]))
    assert_dataframe_equal(actual, expected)

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
    actual = sparkit.freq(["x"], df)  # check single column again
    assert_dataframe_equal(actual, expected)

    actual = sparkit.freq(["x", "y"], df)
    expected = spark.createDataFrame(
        [
            Row(x="a", y=1, frq=2, cml_frq=2, rel_frq=0.25, rel_cml_frq=0.25),
            Row(x="g", y=2, frq=2, cml_frq=4, rel_frq=0.25, rel_cml_frq=0.5),
            Row(x="a", y=2, frq=1, cml_frq=5, rel_frq=0.125, rel_cml_frq=0.625),
            Row(x="b", y=1, frq=1, cml_frq=6, rel_frq=0.125, rel_cml_frq=0.75),
            Row(x="c", y=1, frq=1, cml_frq=7, rel_frq=0.125, rel_cml_frq=0.875),
            Row(x="h", y=1, frq=1, cml_frq=8, rel_frq=0.125, rel_cml_frq=1.0),
        ]
    )
    assert_dataframe_equal(actual, expected)


def test_join(spark):
    df1 = spark.createDataFrame([Row(id=1, x="a"), Row(id=2, x="b")])
    df2 = spark.createDataFrame([Row(id=1, y="c"), Row(id=2, y="d")])
    df3 = spark.createDataFrame([Row(id=1, z="e"), Row(id=2, z="f")])

    actual = sparkit.join(df1, df2, df3, on="id")
    expected = df1.join(df2, "id").join(df3, "id")
    assert_dataframe_equal(actual, expected)


def test_peek(spark):
    df = spark.createDataFrame([Row(x=1, y="a"), Row(x=3, y=None), Row(x=None, y="c")])
    actual = (
        df.transform(sparkit.peek(schema=True))
        .where(F.col("x").isNotNull())
        .transform(sparkit.peek)
    )
    expected = df.where(F.col("x").isNotNull())
    assert_dataframe_equal(actual, expected)


def test_union(spark):
    df1 = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    df2 = spark.createDataFrame([Row(x=5, y=6), Row(x=7, y=8)])
    df3 = spark.createDataFrame([Row(x=0, y=1), Row(x=2, y=3)])

    actual = sparkit.union(df1, df2, df3)
    expected = df1.unionByName(df2).unionByName(df3)
    assert_dataframe_equal(actual, expected)


def test_with_endofweek_date(spark):
    df = spark.createDataFrame(
        [
            Row(day="2023-04-30"),
            Row(day="2023-05-01"),
            Row(day="2023-05-02"),
            Row(day="2023-05-03"),
            Row(day="2023-05-04"),
            Row(day="2023-05-05"),
            Row(day="2023-05-06"),
            Row(day="2023-05-07"),
            Row(day="2023-05-08"),
            Row(day=None),
        ]
    )
    actual = sparkit.with_endofweek_date("day", "endofweek", df)
    expected = spark.createDataFrame(
        [
            Row(day="2023-04-30", endofweek="2023-04-30"),
            Row(day="2023-05-01", endofweek="2023-05-07"),
            Row(day="2023-05-02", endofweek="2023-05-07"),
            Row(day="2023-05-03", endofweek="2023-05-07"),
            Row(day="2023-05-04", endofweek="2023-05-07"),
            Row(day="2023-05-05", endofweek="2023-05-07"),
            Row(day="2023-05-06", endofweek="2023-05-07"),
            Row(day="2023-05-07", endofweek="2023-05-07"),
            Row(day="2023-05-08", endofweek="2023-05-14"),
            Row(day=None, endofweek=None),
        ]
    )
    assert_dataframe_equal(actual, expected)

    actual = sparkit.with_endofweek_date(
        "day",
        "endofweek",
        df,
        last_weekday_name="Sat",
    )
    expected = spark.createDataFrame(
        [
            Row(day="2023-04-30", endofweek="2023-05-06"),
            Row(day="2023-05-01", endofweek="2023-05-06"),
            Row(day="2023-05-02", endofweek="2023-05-06"),
            Row(day="2023-05-03", endofweek="2023-05-06"),
            Row(day="2023-05-04", endofweek="2023-05-06"),
            Row(day="2023-05-05", endofweek="2023-05-06"),
            Row(day="2023-05-06", endofweek="2023-05-06"),
            Row(day="2023-05-07", endofweek="2023-05-13"),
            Row(day="2023-05-08", endofweek="2023-05-13"),
            Row(day=None, endofweek=None),
        ]
    )
    assert_dataframe_equal(actual, expected)

    df = spark.createDataFrame(
        [
            Row(day=date(2023, 4, 30)),
            Row(day=date(2023, 5, 1)),
            Row(day=date(2023, 5, 2)),
            Row(day=date(2023, 5, 3)),
            Row(day=date(2023, 5, 4)),
            Row(day=date(2023, 5, 5)),
            Row(day=date(2023, 5, 6)),
            Row(day=date(2023, 5, 7)),
            Row(day=date(2023, 5, 8)),
            Row(day=None),
        ]
    )
    actual = sparkit.with_endofweek_date("day", "endofweek", df)
    expected = spark.createDataFrame(
        [
            Row(day=date(2023, 4, 30), endofweek=date(2023, 4, 30)),
            Row(day=date(2023, 5, 1), endofweek=date(2023, 5, 7)),
            Row(day=date(2023, 5, 2), endofweek=date(2023, 5, 7)),
            Row(day=date(2023, 5, 3), endofweek=date(2023, 5, 7)),
            Row(day=date(2023, 5, 4), endofweek=date(2023, 5, 7)),
            Row(day=date(2023, 5, 5), endofweek=date(2023, 5, 7)),
            Row(day=date(2023, 5, 6), endofweek=date(2023, 5, 7)),
            Row(day=date(2023, 5, 7), endofweek=date(2023, 5, 7)),
            Row(day=date(2023, 5, 8), endofweek=date(2023, 5, 14)),
            Row(day=None, endofweek=None),
        ]
    )
    assert_dataframe_equal(actual, expected)


def test_with_index(spark):
    df = spark.createDataFrame(
        [
            Row(x="a"),
            Row(x="b"),
            Row(x="c"),
            Row(x="d"),
            Row(x="e"),
            Row(x="f"),
            Row(x="g"),
            Row(x="h"),
        ],
        schema=T.StructType([T.StructField("x", T.StringType(), True)]),
    )

    actual = sparkit.with_index(df)
    expected = spark.createDataFrame(
        [
            Row(idx=1, x="a"),
            Row(idx=2, x="b"),
            Row(idx=3, x="c"),
            Row(idx=4, x="d"),
            Row(idx=5, x="e"),
            Row(idx=6, x="f"),
            Row(idx=7, x="g"),
            Row(idx=8, x="h"),
        ],
        schema=T.StructType(
            [
                T.StructField("idx", T.IntegerType(), True),
                T.StructField("x", T.StringType(), True),
            ]
        ),
    )
    assert_dataframe_equal(actual, expected)


def test_with_startofweek_date(spark):
    df = spark.createDataFrame(
        [
            Row(day="2023-04-30"),
            Row(day="2023-05-01"),
            Row(day="2023-05-02"),
            Row(day="2023-05-03"),
            Row(day="2023-05-04"),
            Row(day="2023-05-05"),
            Row(day="2023-05-06"),
            Row(day="2023-05-07"),
            Row(day="2023-05-08"),
            Row(day=None),
        ]
    )
    actual = sparkit.with_startofweek_date("day", "startofweek", df)
    expected = spark.createDataFrame(
        [
            Row(day="2023-04-30", startofweek=date(2023, 4, 24)),
            Row(day="2023-05-01", startofweek=date(2023, 5, 1)),
            Row(day="2023-05-02", startofweek=date(2023, 5, 1)),
            Row(day="2023-05-03", startofweek=date(2023, 5, 1)),
            Row(day="2023-05-04", startofweek=date(2023, 5, 1)),
            Row(day="2023-05-05", startofweek=date(2023, 5, 1)),
            Row(day="2023-05-06", startofweek=date(2023, 5, 1)),
            Row(day="2023-05-07", startofweek=date(2023, 5, 1)),
            Row(day="2023-05-08", startofweek=date(2023, 5, 8)),
            Row(day=None, startofweek=None),
        ]
    )
    assert_dataframe_equal(actual, expected)

    actual = sparkit.with_startofweek_date(
        "day",
        "startofweek",
        df,
        last_weekday_name="Sat",
    )
    expected = spark.createDataFrame(
        [
            Row(day="2023-04-30", startofweek=date(2023, 4, 30)),
            Row(day="2023-05-01", startofweek=date(2023, 4, 30)),
            Row(day="2023-05-02", startofweek=date(2023, 4, 30)),
            Row(day="2023-05-03", startofweek=date(2023, 4, 30)),
            Row(day="2023-05-04", startofweek=date(2023, 4, 30)),
            Row(day="2023-05-05", startofweek=date(2023, 4, 30)),
            Row(day="2023-05-06", startofweek=date(2023, 4, 30)),
            Row(day="2023-05-07", startofweek=date(2023, 5, 7)),
            Row(day="2023-05-08", startofweek=date(2023, 5, 7)),
            Row(day=None, startofweek=None),
        ]
    )
    assert_dataframe_equal(actual, expected)

    df = spark.createDataFrame(
        [
            Row(day=date(2023, 4, 30)),
            Row(day=date(2023, 5, 1)),
            Row(day=date(2023, 5, 2)),
            Row(day=date(2023, 5, 3)),
            Row(day=date(2023, 5, 4)),
            Row(day=date(2023, 5, 5)),
            Row(day=date(2023, 5, 6)),
            Row(day=date(2023, 5, 7)),
            Row(day=date(2023, 5, 8)),
            Row(day=None),
        ]
    )
    actual = sparkit.with_startofweek_date("day", "startofweek", df)
    expected = spark.createDataFrame(
        [
            Row(day=date(2023, 4, 30), startofweek=date(2023, 4, 24)),
            Row(day=date(2023, 5, 1), startofweek=date(2023, 5, 1)),
            Row(day=date(2023, 5, 2), startofweek=date(2023, 5, 1)),
            Row(day=date(2023, 5, 3), startofweek=date(2023, 5, 1)),
            Row(day=date(2023, 5, 4), startofweek=date(2023, 5, 1)),
            Row(day=date(2023, 5, 5), startofweek=date(2023, 5, 1)),
            Row(day=date(2023, 5, 6), startofweek=date(2023, 5, 1)),
            Row(day=date(2023, 5, 7), startofweek=date(2023, 5, 1)),
            Row(day=date(2023, 5, 8), startofweek=date(2023, 5, 8)),
            Row(day=None, startofweek=None),
        ]
    )
    assert_dataframe_equal(actual, expected)


def test_with_weekday_name(spark):
    df = spark.createDataFrame(
        [
            Row(day="2023-05-01"),
            Row(day="2023-05-02"),
            Row(day="2023-05-03"),
            Row(day="2023-05-04"),
            Row(day="2023-05-05"),
            Row(day="2023-05-06"),
            Row(day="2023-05-07"),
            Row(day=None),
        ]
    )
    actual = sparkit.with_weekday_name("day", "weekday", df)
    expected = spark.createDataFrame(
        [
            Row(day="2023-05-01", weekday="Mon"),
            Row(day="2023-05-02", weekday="Tue"),
            Row(day="2023-05-03", weekday="Wed"),
            Row(day="2023-05-04", weekday="Thu"),
            Row(day="2023-05-05", weekday="Fri"),
            Row(day="2023-05-06", weekday="Sat"),
            Row(day="2023-05-07", weekday="Sun"),
            Row(day=None, weekday=None),
        ]
    )
    assert_dataframe_equal(actual, expected)

    actual = sparkit.with_weekday_name(
        "day",
        "weekday",
        df.withColumn("day", F.to_date("day", "yyyy-MM-dd")),
    )
    assert_dataframe_equal(
        actual,
        expected.withColumn("day", F.to_date("day", "yyyy-MM-dd")),
    )
