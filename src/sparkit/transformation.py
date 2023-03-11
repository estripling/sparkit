import functools

import bumbag
from pyspark.sql import DataFrame

__all__ = (
    "join",
    "union",
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
