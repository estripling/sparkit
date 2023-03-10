from sparkit import exception

__all__ = (
    "check_schema_equal",
    "is_schema_equal",
)


def check_schema_equal(lft_df, rgt_df):
    """Validate that the schemas of the left and right data frames are equal.

    Parameters
    ----------
    lft_df : pyspark.sql.DataFrame
        Left data frame.
    rgt_df : pyspark.sql.DataFrame
        Right data frame.

    Returns
    -------
    NoneType
        A None value if schemas are equal.

    Raises
    ------
    sparkit.exception.SchemaMismatchError
        If schemas are not equal.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> sparkit.check_schema_equal(lft_df, rgt_df) is None
    True

    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1), Row(x=3)])
    >>> try:
    ...     sparkit.check_schema_equal(lft_df, rgt_df)
    ... except sparkit.exception.SchemaMismatchError as error:
    ...     print(error)
    ...
    num_character_differences=10
    struct<x:bigint,y:bigint>
                   ||||||||||
    struct<x:bigint>
    """
    # only check column name and type - ignore nullable property
    lft_schema = lft_df.schema.simpleString()
    rgt_schema = rgt_df.schema.simpleString()

    if lft_schema != rgt_schema:
        raise exception.SchemaMismatchError(lft_schema, rgt_schema)


def is_schema_equal(lft_df, rgt_df):
    """Evaluate if the schemas of the left and right data frames are equal.

    Parameters
    ----------
    lft_df : pyspark.sql.DataFrame
        Left data frame.
    rgt_df : pyspark.sql.DataFrame
        Right data frame.

    Returns
    -------
    bool
        ``True`` if schemas are equal else ``False``.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> sparkit.is_schema_equal(lft_df, rgt_df)
    True

    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1), Row(x=3)])
    >>> sparkit.is_schema_equal(lft_df, rgt_df)
    False
    """
    try:
        check_schema_equal(lft_df, rgt_df)
        return True
    except exception.SchemaMismatchError:
        return False
