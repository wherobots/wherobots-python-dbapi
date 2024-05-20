# wherobots-python-dbapi

Python DB-API implementation for Wherobots DB. This package implements a
PEP-0249 compatible driver to programmatically connect to a Wherobots DB
runtime and execute Spatial SQL queries.

## Installation

If you use [Poetry](https://python-poetry.org) in your project, add the
dependency with `poetry add`:

```
$ poetry add wherobots-python-dbapi
```

Otherwise, just `pip install` it:

```
$ pip install wherobots-python-dbapi
```

## Usage

### Basic usage

Basic usage follows the typical pattern of establishing the connection,
acquiring a cursor, and executing SQL queries through it:

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

The `Cursor` supports the context manager protocol, so you can use it
within a `with` statement when needed:

```python
with connect(...) as conn:
    with conn.cursor() as curr:
        curr.execute(...)
        results = curr.fetchall()
```

It also implements the `close()` method, as suggested by the PEP-2049
specification, to support situations where the cursor is wrapped in a
`contextmanager.closing()`.

### Runtime and region selection

You can chose the Wherobots runtime you want to use using the `runtime`
parameter, passing in one of the `Runtime` enum values. For more
information on runtime sizing and selection, please consult the
[Wherobots product documentation](https://docs.wherobots.com).

The only supported Wherobots compute region for now is `aws-us-west-2`,
in AWS's Oregon (`us-west-2`) region.

### Advanced parameters

The `connect()` method takes some additional parameters that advanced
users may find useful:

* `results_format`: one of the `ResultsFormat` enum values;
    Arrow encoding is the default and most efficient format for
    receiving query results.
* `data_compression`: one of the `DataCompression` enum values; Brotli
    compression is the default and the most efficient compression
    algorithm for receiving query results.
* `geometry_representation`: one of the `GeometryRepresentation` enum
    values; selects the encoding of geometry columns returned to the
    client application. The default is EWKT (string) and the most
    convenient for human inspection while still being usable by
    libraries like Shapely.
