import bumbag


class SparkitError(Exception):
    """A base class for sparkit exceptions."""

    pass


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
