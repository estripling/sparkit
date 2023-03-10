import bumbag


class SparkitError(Exception):
    """A base class for sparkit exceptions."""

    pass


class RowCountMismatchError(SparkitError):
    """Exception raised for mismatching row counts.

    Parameters
    ----------
    lft_row_count : int
        Left row count.
    rgt_row_count : int
        Right row count.
    """

    def __init__(self, lft_row_count, rgt_row_count):
        self.lft_row_count = lft_row_count
        self.rgt_row_count = rgt_row_count
        self.difference = lft_row_count - rgt_row_count
        self.ratio = lft_row_count / rgt_row_count
        self.message = (
            f"{lft_row_count=:,}\n"
            + f"{rgt_row_count=:,}\n"
            + f"{lft_row_count - rgt_row_count=:,}\n"
            + f"{lft_row_count / rgt_row_count=:g}"
        )
        super().__init__(self.message)


class SchemaMismatchError(SparkitError):
    """Exception raised for mismatching schemas.

    Parameters
    ----------
    lft_schema : str
        Left schema.
    rgt_schema : str
        Right schema.
    """

    def __init__(self, lft_schema, rgt_schema):
        self.lft_schema = lft_schema
        self.rgt_schema = rgt_schema
        msg = bumbag.highlight_string_differences(lft_schema, rgt_schema)
        num_character_differences = sum(c == "|" for c in msg.splitlines()[1])
        self.message = f"{num_character_differences=}\n{msg}"
        super().__init__(self.message)
