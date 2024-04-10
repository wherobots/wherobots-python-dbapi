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
from contextlib import closing
import tabulate

from wherobots.db import connect
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime

with connect(
        api_key='...',
        runtime=Runtime.SEDONA,
        region=Region.AWS_US_WEST_2) as conn:
    with closing(conn.cursor()) as curr:
        curr.execute("SHOW SCHEMAS IN wherobots_open_data")
        results = curr.fetchall()
        print(tabulate.tabulate(results, headers="keys", tablefmt="pretty"))
```
