# wherobots-python-dbapi

Python DB-API implementation for Wherobots DB. This package implements a
PEP-0249 compatible driver to programmatically connect to a Wherobots DB
runtime and execute Spatial SQL queries.

## Installation

To add this library as a dependency in your Python project, use `uv add`
or `poetry add` as appropriate:

```
# For uv-managed projects
$ uv add wherobots-python-dbapi

# For poetry-managed projects
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
        runtime=Runtime.TINY,
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

You must also specify in which region your SQL session should execute
into. Wherobots Cloud supports the following compute regions:

* `aws-us-east-1`: AWS US East 1 (N. Virginia)
* `aws-us-west-2`: AWS US West 2 (Oregon)
* `aws-eu-west-1`: AWS EU West 1 (Ireland)

> [!IMPORTANT]
> The `aws-us-west-2` region is available to all Wherobots Cloud users
> and customers; other regions are currently reserved to Professional
> Edition customers.

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
* `session_type`: `"single"` or `"multi"`; if set to `"single"`, then each call
    to `connect()` establishes an exclusive connection to a distinct and dedicated
    Wherobots runtime; if set to "multi", then multiple `connect()` calls with the
    same arguments and credentials will connect to the same shared Wherobots runtime;
    `"single"` is the default.

    Consider multi-session for potential cost savings, but be mindful of
    performance impacts from shared resources. You might need to adjust
    cluster size if slowdowns occur, which could affect overall cost.
