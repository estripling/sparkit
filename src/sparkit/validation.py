from sparkit import exception

__all__ = (
    "check_row_count_equal",
    "check_row_equal",
    "check_schema_equal",
    "is_row_count_equal",
    "is_row_equal",
    "is_schema_equal",
)


def check_row_count_equal(lft_df, rgt_df):
    """Validate that the row counts of the left and right data frames are equal.

    Parameters
    ----------
    lft_df : pyspark.sql.DataFrame
        Left data frame.
    rgt_df : pyspark.sql.DataFrame
        Right data frame.

    Returns
    -------
    NoneType
        A None value if row counts are equal.

    Raises
    ------
    sparkit.exception.RowCountMismatchError
        If row counts are not equal.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> sparkit.check_row_count_equal(lft_df, rgt_df) is None
    True

    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1)])
    >>> try:
    ...     sparkit.check_row_count_equal(lft_df, rgt_df)
    ... except sparkit.exception.RowCountMismatchError as error:
    ...     print(error)
    ...
    lft_row_count=2
    rgt_row_count=1
    lft_row_count - rgt_row_count=1
    lft_row_count / rgt_row_count=2
    """
    lft_row_count = lft_df.count()
    rgt_row_count = rgt_df.count()

    if lft_row_count != rgt_row_count:
        raise exception.RowCountMismatchError(lft_row_count, rgt_row_count)


def check_row_equal(lft_df, rgt_df):
    """Validate that the rows of the left and right data frames are equal.

    Parameters
    ----------
    lft_df : pyspark.sql.DataFrame
        Left data frame.
    rgt_df : pyspark.sql.DataFrame
        Right data frame.

    Returns
    -------
    NoneType
        A None value if rows are equal.

    Raises
    ------
    sparkit.exception.RowMismatchError
        If rows are not equal.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> sparkit.check_row_equal(lft_df, rgt_df) is None
    True

    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=3, y=4), Row(x=5, y=6), Row(x=7, y=8)])
    >>> try:
    ...     sparkit.check_row_equal(lft_df, rgt_df)
    ... except sparkit.exception.RowMismatchError as error:
    ...     print(error)
    ...
    lft_count=1 rgt_count=2
    """
    lft_rows_not_in_rgt = lft_df.subtract(rgt_df)
    rgt_rows_not_in_lft = rgt_df.subtract(lft_df)

    lft_count = lft_rows_not_in_rgt.count()
    rgt_count = rgt_rows_not_in_lft.count()

    is_equal = (lft_count == 0) and (rgt_count == 0)

    if not is_equal:
        raise exception.RowMismatchError(
            lft_rows_not_in_rgt,
            rgt_rows_not_in_lft,
            lft_count,
            rgt_count,
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


def is_row_count_equal(lft_df, rgt_df):
    """Evaluate if the row counts of the left and right data frames are equal.

    Parameters
    ----------
    lft_df : pyspark.sql.DataFrame
        Left data frame.
    rgt_df : pyspark.sql.DataFrame
        Right data frame.

    Returns
    -------
    bool
        ``True`` if row counts are equal else ``False``.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> sparkit.is_row_count_equal(lft_df, rgt_df)
    True

    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1)])
    >>> sparkit.is_row_count_equal(lft_df, rgt_df)
    False
    """
    try:
        check_row_count_equal(lft_df, rgt_df)
        return True
    except exception.RowCountMismatchError:
        return False


def is_row_equal(lft_df, rgt_df):
    """Evaluate if the rows of the left and right data frames are equal.

    Parameters
    ----------
    lft_df : pyspark.sql.DataFrame
        Left data frame.
    rgt_df : pyspark.sql.DataFrame
        Right data frame.

    Returns
    -------
    bool
        ``True`` if rows are equal else ``False``.

    Examples
    --------
    >>> import sparkit
    >>> from pyspark.sql import Row, SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> sparkit.is_row_equal(lft_df, rgt_df)
    True

    >>> lft_df = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
    >>> rgt_df = spark.createDataFrame([Row(x=3, y=4), Row(x=5, y=6), Row(x=7, y=8)])
    >>> sparkit.is_row_equal(lft_df, rgt_df)
    False
    """
    try:
        check_row_equal(lft_df, rgt_df)
        return True
    except exception.RowMismatchError:
        return False


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
