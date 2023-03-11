import functools

import bumbag
from pyspark.sql import DataFrame

__all__ = ("union",)


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
