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
    "freq",
    "join",
    "peek",
    "union",
)


@toolz.curry
def add_prefix(dataframe, prefix, subset=None):
    """Add prefix to column names.

    Parameters
    ----------
    dataframe : pyspark.sql.DataFrame
        The data frame for which the column names are to be changed.
    prefix : str
        The string to add before a column name.
    subset : Iterable of str, default=None
        Specify a column selection. If None, all columns are selected.

    Notes
    -----
    - Function is curried.

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
    >>> sparkit.add_prefix(df, "prefix_").show()
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


def add_suffix(dataframe, suffix, subset=None):
    """Add suffix to column names.

    Parameters
    ----------
    dataframe : pyspark.sql.DataFrame
        The data frame for which the column names are to be changed.
    suffix : str
        The string to add after a column name.
    subset : Iterable of str, default=None
        Specify a column selection. If None, all columns are selected.

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
    >>> sparkit.add_suffix(df, "_suffix").show()
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


def count_nulls(dataframe, subset=None):
    """Count null values in data frame.

    Parameters
    ----------
    dataframe : pyspark.sql.DataFrame
        Input data frame to count null values.
    subset : Iterable of str, default=None
        Specify a column selection. If None, all columns are selected.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with null values.

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
def freq(dataframe, columns):
    """Compute value frequencies.

    Given some columns, calculate for each distinct value:
     - the frequency (``frq``),
     - the cumulative frequency (``cml_frq``),
     - the relative frequency (``rel_frq``), and
     - the cumulative relative frequency (``rel_cml_frq``).

    Parameters
    ----------
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    columns : list of str or pyspark.sql.Column
        Specify the columns for which to compute the value frequency.

    Notes
    -----
    - Function is curried.

    Returns
    -------
    pyspark.sql.DataFrame
        A new data frame with value frequencies for specified columns.

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
    >>> sparkit.freq(df, columns=["x"]).show()
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
def peek(dataframe, n=3, cache=False, schema=False, index=False):
    """Have a quick look at the data frame and return it.

    This function is handy when chaining data frame transformations.

    Parameters
    ----------
    dataframe : pyspark.sql.DataFrame
        Input data frame.
    n : int, default=3
        Specify the number of rows to show. If `n <= 0`, no rows are shown.
    cache : bool, default=False
        Specify if data frame should be cached.
    schema : bool, default=False
        Specify if schema should be printed.
    index : bool, default=False
        Specify if a row index should be shown.

    Notes
    -----
    - Function is curried.

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
