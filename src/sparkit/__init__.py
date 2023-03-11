from importlib import metadata

__version__ = metadata.version("sparkit")

from .transformation import *
from .validation import *

del (
    metadata,
    transformation,
    validation,
)
