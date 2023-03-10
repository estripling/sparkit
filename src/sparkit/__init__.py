from importlib import metadata

__version__ = metadata.version("sparkit")

from .core import *
from .validation import *

del (
    metadata,
    core,
    validation,
)
