import functools

import bumbag
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

__all__ = (
    "add_suffix",
    "count_nulls",
    "join",
    "union",
)


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
