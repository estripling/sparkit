from importlib import metadata

import sparkit


def test_version():
    assert sparkit.__version__ == metadata.version("sparkit")
