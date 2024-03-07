# wherobots-python-dbapi-driver

Python DB-API implementation for Wherobots DB. This package implements a
PEP-0249 compatible driver to programmatically connect to a Wherobots DB
runtime and execute Spatial SQL queries.

## Installation

If you use [Poetry](https://python-poetry.org) in your project, add the
dependency with `poetry add`:

```
$ poetry add wherobots-python-dbapi-driver
```

Otherwise, just `pip install` it:

```
$ pip install wherobots-python-dbapi-driver
```

## Usage

```python
from wherobots import Region, Runtime
from wherobots.db import connect

with connect(
    api_key='...',
    runtime=Runtime.SEDONA,
    region=Region.AWS_US_WEST_2) as conn:
    with conn.cursor() as curs:
        curs.execute("SHOW SCHEMAS IN wherobots_open_data")
        print(curs.fetchone())
```
