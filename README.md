<p align="center">
<img src="https://raw.githubusercontent.com/estripling/sparkit/main/docs/source/_static/logo.png" width="480" alt="The sparkit logo.">
</p>

<p align="center">
<a href="https://pypi.org/project/sparkit"><img alt="pypi" src="https://img.shields.io/pypi/v/sparkit"></a>
<a href="https://readthedocs.org/projects/sparkit/?badge=latest"><img alt="docs" src="https://readthedocs.org/projects/sparkit/badge/?version=latest"></a>
<a href="https://github.com/estripling/sparkit/actions/workflows/ci.yml"><img alt="ci status" src="https://github.com/estripling/sparkit/actions/workflows/ci.yml/badge.svg?branch=main"></a>
<a href="https://codecov.io/gh/estripling/sparkit"><img alt="coverage" src="https://codecov.io/github/estripling/sparkit/coverage.svg?branch=main"></a>
<a href="https://github.com/estripling/sparkit/blob/main/LICENSE"><img alt="license" src="https://img.shields.io/pypi/l/sparkit"></a>
</p>

## About

A package for PySpark utility functions:

- [Documentation](https://sparkit.readthedocs.io/en/stable/index.html)
- [Example usage](https://sparkit.readthedocs.io/en/stable/example.html)
- [API Reference](https://sparkit.readthedocs.io/en/stable/autoapi/sparkit/index.html)

## Installation

`sparkit` is available on [PyPI](https://pypi.org/project/sparkit/) for Python 3.8+ and Spark 3 (Java 11):

```console
pip install sparkit
```

## Examples

[`union`](https://sparkit.readthedocs.io/en/stable/autoapi/sparkit/index.html#sparkit.union) multiple data frames by name (pass single and / or an iterable of data frames):

```python
>>> import sparkit
>>> from pyspark.sql import Row, SparkSession
>>> spark = SparkSession.builder.getOrCreate()
>>> df1 = spark.createDataFrame([Row(x=1, y=2), Row(x=3, y=4)])
>>> df2 = spark.createDataFrame([Row(x=5, y=6), Row(x=7, y=8)])
>>> df3 = spark.createDataFrame([Row(x=0, y=1), Row(x=2, y=3)])
>>> df4 = spark.createDataFrame([Row(x=5, y=3), Row(x=9, y=6)])
>>> sparkit.union(df1, [df2, df3], df4).show()
+---+---+
|  x|  y|
+---+---+
|  1|  2|
|  3|  4|
|  5|  6|
|  7|  8|
|  0|  1|
|  2|  3|
|  5|  3|
|  9|  6|
+---+---+
```

## Contributing to sparkit

Your contribution is greatly appreciated!
See the following links to help you get started:

- [Contributing Guide](https://sparkit.readthedocs.io/en/latest/contributing.html)
- [Developer Guide](https://sparkit.readthedocs.io/en/latest/developers.html)
- [Contributor Code of Conduct](https://sparkit.readthedocs.io/en/latest/conduct.html)

## License

`sparkit` was created by sparkit Developers.
It is licensed under the terms of the BSD 3-Clause license.
