# wherobots-python-dbapi-driver

Python DB-API implementation for Wherobots DB. This package implements a
PEP-0249 compatible driver to programmatically connect to a Wherobots DB
runtime and execute Spatial SQL queries.

## Installation

If you use [Poetry](https://python-poetry.org) in your project, add the
dependency with `poetry add`:

```
$ poetry add git+https://github.com/wherobots/wherobots-python-dbapi-driver
```

Otherwise, just `pip install` it:

```
$ pip install git+https://github.com/wherobots/wherobots-python-dbapi-driver
```

## Usage

```python
from wherobots.db import connect
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime

with connect(
        api_key='...',
        runtime=Runtime.SEDONA,
        region=Region.AWS_US_WEST_2) as conn:
    curr = conn.cursor()
    curr.execute("SHOW SCHEMAS IN wherobots_open_data")
    results = curr.fetchall()
    print(results)
```

The `Cursor` also supports the context manager protocol, so you can use
it within a `with` statement when needed:

```python
with connect(...) as conn:
    with conn.cursor() as curr:
        curr.execute(...)
        results = curr.fetchall()
```

It also implements the `close()` method, as suggested by the PEP-2049
specification, to support situations where the cursor is wrapped in a
`contextmanager.closing()`.
