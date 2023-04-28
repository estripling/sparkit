import bumbag
import pandas as pd

__all__ = (
    "join",
    "profile",
    "union",
)


def join(*dataframes):
    """Join multiple pandas data frames by index.

    Parameters
    ----------
    dataframes : Iterable of pandas.DataFrame and pandas.Series
        Data frames to join.
        Note that a pandas.Series is converted to a data frame.

    Returns
    -------
    pandas.DataFrame
        A new data frame being the join of supplied data frames.

    Examples
    --------
    >>> from sparkit import pandastools
    >>> df1 = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"])
    >>> df2 = pd.DataFrame([[5, 6], [7, 8]], columns=["c", "d"])
    >>> df = pandastools.join(df1, df2)
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
       a  b  c  d
    0  1  2  5  6
    1  3  4  7  8

    >>> df1 = pd.DataFrame([[1, 2], [3, 4]], index=[0, 1], columns=["a", "b"])
    >>> df2 = pd.DataFrame([[5, 6], [7, 8]], index=[0, 2], columns=["c", "d"])
    >>> df = join([df1, df2])
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
         a    b    c    d
    0  1.0  2.0  5.0  6.0
    1  3.0  4.0  NaN  NaN
    2  NaN  NaN  7.0  8.0

    >>> df1 = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"])
    >>> s1 = pd.Series([5, 6], name="c")
    >>> df = pandastools.join(df1, s1)
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
       a  b  c
    0  1  2  5
    1  3  4  6

    >>> s1 = pd.Series([1, 2])
    >>> s2 = pd.Series([3, 4])
    >>> s3 = pd.Series([5, 6])
    >>> df = pandastools.join([s1, s2], s3)
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
       0  0  0
    0  1  3  5
    1  2  4  6

    >>> s1 = pd.Series([1, 2], index=[0, 1], name="a")
    >>> s2 = pd.Series([3, 4], index=[1, 2], name="b")
    >>> s3 = pd.Series([5, 6], index=[2, 3], name="c")
    >>> df = pandastools.join(s1, s2, s3)
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
         a    b    c
    0  1.0  NaN  NaN
    1  2.0  3.0  NaN
    2  NaN  4.0  5.0
    3  NaN  NaN  6.0
    """
    return pd.concat(map(pd.DataFrame, bumbag.flatten(dataframes)), axis=1)


def profile(dataframe, /):
    """Profile pandas data frame.

    Parameters
    ----------
    dataframe : pandas.DataFrame
        Input data frame.

    Returns
    -------
    pandas.DataFrame
        A new data frame with summary statistics.
    """
    columns = [
        "type",
        "count",
        "isnull",
        "isnull%",
        "unique",
        "unique%",
        "mean",
        "std",
        "skewness",
        "kurtosis",
        "min",
        "5%",
        "25%",
        "50%",
        "75%",
        "95%",
        "max",
    ]
    return (
        join(
            dataframe.dtypes.apply(str).to_frame("type"),
            dataframe.count().to_frame("count"),
            dataframe.isnull().sum().to_frame("isnull"),
            dataframe.nunique().to_frame("unique"),
            dataframe.mean(numeric_only=True).to_frame("mean"),
            dataframe.std(numeric_only=True, ddof=1).to_frame("std"),
            dataframe.skew(numeric_only=True).to_frame("skewness"),
            dataframe.kurt(numeric_only=True).to_frame("kurtosis"),
            dataframe.min(numeric_only=True).to_frame("min"),
            dataframe.quantile(0.05, numeric_only=True).to_frame("5%"),
            dataframe.quantile(0.25, numeric_only=True).to_frame("25%"),
            dataframe.quantile(0.50, numeric_only=True).to_frame("50%"),
            dataframe.quantile(0.75, numeric_only=True).to_frame("75%"),
            dataframe.quantile(0.95, numeric_only=True).to_frame("95%"),
            dataframe.max(numeric_only=True).to_frame("max"),
        )
        .assign(
            pct_isnull=lambda df: df["isnull"] / dataframe.shape[0],
            pct_unique=lambda df: df["unique"] / dataframe.shape[0],
        )
        .rename(columns={"pct_isnull": "isnull%", "pct_unique": "unique%"})
        .loc[:, columns]
    )


def union(*dataframes):
    """Union multiple pandas data frames by name.

    Parameters
    ----------
    dataframes : Iterable of pandas.DataFrame and pandas.Series
        Data frames to union by name.
        Note that a pandas.Series is converted to a data frame.

    Returns
    -------
    pandas.DataFrame
        A new data frame containing the union of rows of supplied data frames.

    Examples
    --------
    >>> from sparkit import pandastools
    >>> df1 = pd.DataFrame([[1, 2], [3, 4]])
    >>> df2 = pd.DataFrame([[5, 6], [7, 8]])
    >>> df = pandastools.union(df1, df2)
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
       0  1
    0  1  2
    1  3  4
    0  5  6
    1  7  8

    >>> df1 = pd.DataFrame([[1, 2], [3, 4]], index=[0, 1])
    >>> df2 = pd.DataFrame([[5, 6], [7, 8]], index=[0, 2])
    >>> df = pandastools.union([df1, df2])
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
       0  1
    0  1  2
    1  3  4
    0  5  6
    2  7  8

    >>> df1 = pd.DataFrame([[1, 2], [3, 4]], index=[0, 1], columns=["a", "b"])
    >>> df2 = pd.DataFrame([[5, 6], [7, 8]], index=[0, 2], columns=["c", "d"])
    >>> df = pandastools.union([df1, df2])
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
         a    b    c    d
    0  1.0  2.0  NaN  NaN
    1  3.0  4.0  NaN  NaN
    0  NaN  NaN  5.0  6.0
    2  NaN  NaN  7.0  8.0

    >>> df1 = pd.DataFrame([[1, 2], [3, 4]])
    >>> s1 = pd.Series([5, 6])
    >>> df = pandastools.union(df1, s1)
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
       0    1
    0  1  2.0
    1  3  4.0
    0  5  NaN
    1  6  NaN

    >>> s1 = pd.Series([1, 2])
    >>> s2 = pd.Series([3, 4])
    >>> s3 = pd.Series([5, 6])
    >>> df = pandastools.union([s1, s2], s3)
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
       0
    0  1
    1  2
    0  3
    1  4
    0  5
    1  6

    >>> s1 = pd.Series([1, 2], index=[0, 1], name="a")
    >>> s2 = pd.Series([3, 4], index=[1, 2], name="b")
    >>> s3 = pd.Series([5, 6], index=[2, 3], name="c")
    >>> df = pandastools.union(s1, s2, s3)
    >>> isinstance(df, pd.DataFrame)
    True
    >>> df
         a    b    c
    0  1.0  NaN  NaN
    1  2.0  NaN  NaN
    1  NaN  3.0  NaN
    2  NaN  4.0  NaN
    2  NaN  NaN  5.0
    3  NaN  NaN  6.0
    """
    return pd.concat(map(pd.DataFrame, bumbag.flatten(dataframes)), axis=0)
