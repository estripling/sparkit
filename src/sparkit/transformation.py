import functools

import bumbag
import pyspark.sql.functions as F
import pyspark.sql.types as T
import toolz
from IPython import get_ipython
from IPython.display import HTML, display
from pyspark.sql import DataFrame, Window

__all__ = (
    "add_prefix",
    "add_suffix",
    "count_nulls",
    "daterange",
    "freq",
    "join",
    "peek",
    "union",
    "with_consecutive_integers",
    "with_endofweek_date",
    "with_startofweek_date",
    "with_weekday_name",
)


@toolz.curry
def add_prefix(prefix, dataframe, /, *, subset=None):
    """Add prefix to column names.

    Parameters
    ----------
    prefix : str
        Specify the prefix string.
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    subset : Iterable of str, default=None
        Specify a column selection. If None, all columns are selected.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with changed column names.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame([Row(x=1, y=2)])
    >>> sparkit.add_prefix("prefix_", df).show()
    +--------+--------+
    |prefix_x|prefix_y|
    +--------+--------+
    |       1|       2|
    +--------+--------+
    <BLANKLINE>
    """
    columns = subset or dataframe.columns
    for column in columns:
        dataframe = dataframe.withColumnRenamed(column, f"{prefix}{column}")
    return dataframe


@toolz.curry
def add_suffix(suffix, dataframe, /, *, subset=None):
    """Add suffix to column names.

    Parameters
    ----------
    suffix : str
        Specify the suffix string.
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    subset : Iterable of str, default=None
        Specify a column selection. If None, all columns are selected.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with changed column names.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame([Row(x=1, y=2)])
    >>> sparkit.add_suffix("_suffix", df).show()
    +--------+--------+
    |x_suffix|y_suffix|
    +--------+--------+
    |       1|       2|
    +--------+--------+
    <BLANKLINE>
    """
    columns = subset or dataframe.columns
    for column in columns:
        dataframe = dataframe.withColumnRenamed(column, f"{column}{suffix}")
    return dataframe


@toolz.curry
def count_nulls(dataframe, /, *, subset=None):
    """Count null values in data frame.

    Parameters
    ----------
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    subset : Iterable of str, default=None
        Specify a column selection. If None, all columns are selected.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with null value counts per column.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(
    ...     [
    ...         Row(x=1, y=2, z=None),
    ...         Row(x=4, y=None, z=6),
    ...         Row(x=10, y=None, z=None),
    ...     ]
    ... )
    >>> sparkit.count_nulls(df).show()
    +---+---+---+
    |  x|  y|  z|
    +---+---+---+
    |  0|  2|  2|
    +---+---+---+
    <BLANKLINE>
    """
    columns = subset or dataframe.columns
    return dataframe.agg(
        *[F.sum(F.isnull(c).cast(T.LongType())).alias(c) for c in columns]
    )


@toolz.curry
def daterange(id_column_name, new_column_name, dataframe, /, *, min_date, max_date):
    """Generate a date range for each distinct ID value.

    Parameters
    ----------
    id_column_name : str
        Specify the name of the ID column.
    new_column_name : str
        Specify the name of the new column to be added to the data frame.
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    min_date : str or datetime.date
        Specify the inclusive lower endpoint of the date range.
    max_date : str or datetime.date
        Specify the inclusive upper endpoint of the date range.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame in long format with consecutive dates between
        `min_date` and `max_date` for each distinct ID value.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(
    ...     [
    ...         Row(id=1),
    ...         Row(id=1),
    ...         Row(id=3),
    ...         Row(id=2),
    ...         Row(id=2),
    ...         Row(id=3),
    ...     ]
    ... )
    >>> (
    ...     sparkit.daterange(
    ...         "id",
    ...         "day",
    ...         df,
    ...         min_date="2023-05-01",
    ...         max_date="2023-05-03",
    ...     )
    ...     .orderBy("id", "day")
    ...     .show()
    ... )
    +---+----------+
    | id|       day|
    +---+----------+
    |  1|2023-05-01|
    |  1|2023-05-02|
    |  1|2023-05-03|
    |  2|2023-05-01|
    |  2|2023-05-02|
    |  2|2023-05-03|
    |  3|2023-05-01|
    |  3|2023-05-02|
    |  3|2023-05-03|
    +---+----------+
    <BLANKLINE>
    """
    return (
        dataframe.select(id_column_name)
        .distinct()
        .withColumn("min_date", F.to_date(F.lit(min_date), "yyyy-MM-dd"))
        .withColumn("max_date", F.to_date(F.lit(max_date), "yyyy-MM-dd"))
        .select(
            id_column_name,
            F.expr("sequence(min_date, max_date, interval 1 day)").alias(
                new_column_name
            ),
        )
        .withColumn(new_column_name, F.explode(new_column_name))
    )


@toolz.curry
def freq(columns, dataframe, /):
    """Compute value frequencies.

    Given a selection of columns, calculate for each distinct value:
     - the frequency (``frq``),
     - the cumulative frequency (``cml_frq``),
     - the relative frequency (``rel_frq``), and
     - the cumulative relative frequency (``rel_cml_frq``).

    Parameters
    ----------
    columns : Iterable of str or pyspark.sql.Column
        Specify the columns for which to compute the value frequency.
    dataframe : pyspark.sql.DataFrame
        Input data frame.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with value frequencies of specified columns.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(
    ...     [
    ...         Row(x="a"),
    ...         Row(x="c"),
    ...         Row(x="b"),
    ...         Row(x="g"),
    ...         Row(x="h"),
    ...         Row(x="a"),
    ...         Row(x="g"),
    ...         Row(x="a"),
    ...     ]
    ... )
    >>> sparkit.freq(["x"], df).show()
    +---+---+-------+-------+-----------+
    |  x|frq|cml_frq|rel_frq|rel_cml_frq|
    +---+---+-------+-------+-----------+
    |  a|  3|      3|  0.375|      0.375|
    |  g|  2|      5|   0.25|      0.625|
    |  b|  1|      6|  0.125|       0.75|
    |  c|  1|      7|  0.125|      0.875|
    |  h|  1|      8|  0.125|        1.0|
    +---+---+-------+-------+-----------+
    <BLANKLINE>
    """
    # Use F.lit(1) for an ungrouped specification
    win_sorted = Window.partitionBy(F.lit(1)).orderBy(F.desc("frq"), *columns)
    win_unsorted = Window.partitionBy(F.lit(1))
    return (
        dataframe.groupby(columns)
        .count()
        .withColumnRenamed("count", "frq")
        .withColumn("cml_frq", F.sum("frq").over(win_sorted))
        .withColumn("rel_frq", F.col("frq") / F.sum("frq").over(win_unsorted))
        .withColumn("rel_cml_frq", F.sum("rel_frq").over(win_sorted))
        .orderBy("cml_frq")
    )


def join(*dataframes, on, how="inner"):
    """Join multiple spark data frames on common key.

    Parameters
    ----------
    dataframes : Iterable of pyspark.sql.DataFrame
        Data frames to join.
    on : str or iterable of str
        Key(s) to join on.
    how : str, default="inner"
        Valid specification to join spark data frames.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame being the join of supplied data frames.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df1 = spark.createDataFrame([Row(id=1, x="a"), Row(id=2, x="b")])
    >>> df2 = spark.createDataFrame([Row(id=1, y="c"), Row(id=2, y="d")])
    >>> df3 = spark.createDataFrame([Row(id=1, z="e"), Row(id=2, z="f")])
    >>> sparkit.join(df1, df2, df3, on="id").show()
    +---+---+---+---+
    | id|  x|  y|  z|
    +---+---+---+---+
    |  1|  a|  c|  e|
    |  2|  b|  d|  f|
    +---+---+---+---+
    <BLANKLINE>
    """
    join = functools.partial(DataFrame.join, on=on, how=how)
    return functools.reduce(join, bumbag.flatten(dataframes))


@toolz.curry
def peek(dataframe, /, *, n=6, cache=False, schema=False, index=False):
    """Have a quick look at the data frame and return it.

    This function is handy when chaining data frame transformations.

    Parameters
    ----------
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    n : int, default=6
        Specify the number of rows to show. If `n <= 0`, no rows are shown.
    cache : bool, default=False
        Specify if data frame should be cached.
    schema : bool, default=False
        Specify if schema should be printed.
    index : bool, default=False
        Specify if a row index should be shown.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        The input data frame.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(
    ...     [
    ...         Row(x=1, y="a"),
    ...         Row(x=3, y=None),
    ...         Row(x=None, y="c"),
    ...     ]
    ... )
    >>> df.show()
    +----+----+
    |   x|   y|
    +----+----+
    |   1|   a|
    |   3|null|
    |null|   c|
    +----+----+
    <BLANKLINE>
    >>> filtered_df = (
    ...     df.transform(sparkit.peek)
    ...     .where("x IS NOT NULL")
    ...     .transform(sparkit.peek)
    ... )
    shape = (3, 2)
       x    y
     1.0    a
     3.0 None
    None    c
    shape = (2, 2)
     x    y
     1    a
     3 None
    """
    df = dataframe if dataframe.is_cached else dataframe.cache() if cache else dataframe

    if schema:
        df.printSchema()

    def fmt(x):
        return f"{x:,}".replace(",", "_")

    num_rows = fmt(df.count())
    num_cols = fmt(len(df.columns))
    print(f"shape = ({num_rows}, {num_cols})")

    if n > 0:
        pandas_df = df.limit(n).toPandas()
        pandas_df.index += 1

        is_inside_notebook = get_ipython() is not None

        df_repr = (
            pandas_df.to_html(index=index, na_rep="None", col_space="20px")
            if is_inside_notebook
            else pandas_df.to_string(index=index, na_rep="None")
        )

        display(HTML(df_repr)) if is_inside_notebook else print(df_repr)

    return df


def union(*dataframes):
    """Union multiple spark data frames by name.

    Parameters
    ----------
    dataframes : Iterable of pyspark.sql.DataFrame
        Data frames to union by name.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame containing the union of rows of supplied data frames.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df1 = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> df2 = spark.createDataFrame([Row(x=5, y=6), Row(x=7, y=8)])
    >>> df3 = spark.createDataFrame([Row(x=0, y=1), Row(x=2, y=3)])
    >>> sparkit.union(df1, df2, df3).show()
    +---+---+
    |  x|  y|
    +---+---+
    |  1|  2|
    |  3|  4|
    |  5|  6|
    |  7|  8|
    |  0|  1|
    |  2|  3|
    +---+---+
    <BLANKLINE>
    """
    return functools.reduce(DataFrame.unionByName, bumbag.flatten(dataframes))


@toolz.curry
def with_consecutive_integers(new_column_name, dataframe, /):
    """Add column with consecutive positive integers.

    Parameters
    ----------
    new_column_name : str
        Specify the name of the new column to be added to the data frame.
    dataframe : pyspark.sql.DataFrame
        Input data frame.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with a column of consecutive positive integers.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame([Row(x="a"), Row(x="b"), Row(x="c"), Row(x="d")])
    >>> sparkit.with_consecutive_integers("idx", df).show()
    +---+---+
    |  x|idx|
    +---+---+
    |  a|  1|
    |  b|  2|
    |  c|  3|
    |  d|  4|
    +---+---+
    <BLANKLINE>
    """
    win = Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id())
    return dataframe.withColumn(new_column_name, F.row_number().over(win))


@toolz.curry
def with_endofweek_date(
    date_column_name,
    new_column_name,
    dataframe,
    /,
    *,
    last_weekday_name="Sun",
):
    """Add column with the end of the week date.

    Parameters
    ----------
    date_column_name : str
        Specify the name of the date column.
    new_column_name : str
        Specify the name of the new column to be added to the data frame.
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    last_weekday_name : str, default="Sun"
        Specify the name of the last weekday.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with the end-of-week date column.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(
    ...     [
    ...         Row(day="2023-05-01"),
    ...         Row(day=None),
    ...         Row(day="2023-05-03"),
    ...         Row(day="2023-05-08"),
    ...         Row(day="2023-05-21"),
    ...     ],
    ... )
    >>> sparkit.with_endofweek_date("day", "endofweek", df).show()
    +----------+----------+
    |       day| endofweek|
    +----------+----------+
    |2023-05-01|2023-05-07|
    |      null|      null|
    |2023-05-03|2023-05-07|
    |2023-05-08|2023-05-14|
    |2023-05-21|2023-05-21|
    +----------+----------+
    <BLANKLINE>

    >>> sparkit.with_endofweek_date(
    ...     "day", "endofweek", df, last_weekday_name="Sat"
    ... ).show()
    +----------+----------+
    |       day| endofweek|
    +----------+----------+
    |2023-05-01|2023-05-06|
    |      null|      null|
    |2023-05-03|2023-05-06|
    |2023-05-08|2023-05-13|
    |2023-05-21|2023-05-27|
    +----------+----------+
    <BLANKLINE>
    """
    tmp_column = "weekday"
    return (
        dataframe.transform(with_weekday_name(date_column_name, tmp_column))
        .withColumn(
            new_column_name,
            F.when(F.col(tmp_column).isNull(), None)
            .when(F.col(tmp_column) == last_weekday_name, F.col(date_column_name))
            .otherwise(F.next_day(F.col(date_column_name), last_weekday_name)),
        )
        .drop(tmp_column)
    )


@toolz.curry
def with_startofweek_date(
    date_column_name,
    new_column_name,
    dataframe,
    /,
    *,
    last_weekday_name="Sun",
):
    """Add column with the start of the week date.

    Parameters
    ----------
    date_column_name : str
        Specify the name of the date column.
    new_column_name : str
        Specify the name of the new column to be added to the data frame.
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    last_weekday_name : str, default="Sun"
        Specify the name of the last weekday.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with the start-of-week date column.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(
    ...     [
    ...         Row(day="2023-05-01"),
    ...         Row(day=None),
    ...         Row(day="2023-05-03"),
    ...         Row(day="2023-05-08"),
    ...         Row(day="2023-05-21"),
    ...     ],
    ... )
    >>> sparkit.with_startofweek_date("day", "startofweek", df).show()
    +----------+-----------+
    |       day|startofweek|
    +----------+-----------+
    |2023-05-01| 2023-05-01|
    |      null|       null|
    |2023-05-03| 2023-05-01|
    |2023-05-08| 2023-05-08|
    |2023-05-21| 2023-05-15|
    +----------+-----------+
    <BLANKLINE>

    >>> sparkit.with_startofweek_date(
    ...     "day", "startofweek", df, last_weekday_name="Sat"
    ... ).show()
    +----------+-----------+
    |       day|startofweek|
    +----------+-----------+
    |2023-05-01| 2023-04-30|
    |      null|       null|
    |2023-05-03| 2023-04-30|
    |2023-05-08| 2023-05-07|
    |2023-05-21| 2023-05-21|
    +----------+-----------+
    <BLANKLINE>
    """
    tmp_column = "endofweek"
    with_endofweek = with_endofweek_date(
        date_column_name,
        tmp_column,
        last_weekday_name=last_weekday_name,
    )
    return (
        dataframe.transform(with_endofweek)
        .withColumn(new_column_name, F.date_sub(tmp_column, 6))
        .drop(tmp_column)
    )


@toolz.curry
def with_weekday_name(date_column_name, new_column_name, dataframe):
    """Add column with the name of the weekday.

    Parameters
    ----------
    date_column_name : str
        Specify the name of the date column.
    new_column_name : str
        Specify the name of the new column to be added to the data frame.
    dataframe : pyspark.sql.DataFrame
        Input data frame.

    Notes
    -----
    Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with the weekday column.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(
    ...     [Row(day="2023-05-01"), Row(day=None), Row(day="2023-05-03")]
    ... )
    >>> sparkit.with_weekday_name("day", "weekday", df).show()
    +----------+-------+
    |       day|weekday|
    +----------+-------+
    |2023-05-01|    Mon|
    |      null|   null|
    |2023-05-03|    Wed|
    +----------+-------+
    <BLANKLINE>
    """

    def determine_weekday(date_column):
        weekday_int = F.dayofweek(date_column)
        return (
            F.when(weekday_int == 1, "Sun")
            .when(weekday_int == 2, "Mon")
            .when(weekday_int == 3, "Tue")
            .when(weekday_int == 4, "Wed")
            .when(weekday_int == 5, "Thu")
            .when(weekday_int == 6, "Fri")
            .when(weekday_int == 7, "Sat")
            .otherwise(None)
        )

    return dataframe.withColumn(new_column_name, determine_weekday(date_column_name))
