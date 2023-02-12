from importlib import metadata

__version__ = metadata.version("sparkit")

from .core import *

del (core,)
