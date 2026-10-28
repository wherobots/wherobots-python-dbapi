# wherobots-python-dbapi

Python DB-API implementation for Wherobots DB. This package implements a
PEP-0249 compatible driver to programmatically connect to a Wherobots DB
runtime and execute Spatial SQL queries.

## PEP 249 DB-API 2.0 Compliance

This driver implements the [PEP 249](https://peps.python.org/pep-0249/)
Python Database API Specification v2.0. Its module-level globals are
defined in `wherobots.db.driver` and are not re-exported from `wherobots.db`:

| Global | Value | Meaning |
|---|---|---|
| `apilevel` | `"2.0"` | Supports DB-API 2.0 |
| `threadsafety` | `1` | Threads may share the module, but not connections |
| `paramstyle` | `"pyformat"` | Uses `%(name)s` named parameter markers |

### Parameterized queries

Use `%(name)s` markers in your SQL and pass a dictionary of parameter
values. The driver automatically quotes and escapes values based on
their Python type (strings are single-quoted, `None` becomes `NULL`,
booleans become `TRUE`/`FALSE`, and numeric types are passed through
unquoted):

```python
curr.execute(
    "SELECT * FROM places WHERE id = %(id)s AND category = %(cat)s",
    parameters={"id": 42, "cat": "restaurant"},
)
# Produces: ... WHERE id = 42 AND category = 'restaurant'
```

Literal `%` characters in SQL (e.g. `LIKE` wildcards) do not need
escaping and work alongside parameters:

```python
curr.execute(
    "SELECT * FROM places WHERE name LIKE '%coffee%' AND city = %(city)s",
    parameters={"city": "Seattle"},
)
```

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

`runtime` and `region` are optional and accept either the provided enums (handy
for autocomplete) or a plain string — strings are passed to the API as-is, so
new or BYOC regions (e.g. `region="byoc-acme-us-east-1"`) work without an SDK
upgrade. When omitted, your organization's configured defaults are used:

```python
with connect(api_key='...') as conn:  # uses your org's default runtime + region
    ...
```

The `Cursor` supports the context manager protocol, so you can use it
within a `with` statement when needed:

```python
with connect(...) as conn:
    with conn.cursor() as curr:
        curr.execute(...)
        results = curr.fetchall()
```

It also implements the `close()` method, as suggested by the PEP 249
specification, to support situations where the cursor is wrapped in a
`contextmanager.closing()`.

### Storing results in cloud storage

For large query results, you can store them directly in cloud storage
instead of retrieving them over the connection. This is useful when
results are too large to transfer efficiently, or when you want to
process them later with other tools.

```python
from wherobots.db import connect, Store, StorageFormat
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime

with connect(
        api_key='...',
        runtime=Runtime.TINY,
        region=Region.AWS_US_WEST_2) as conn:
    curr = conn.cursor()

    # Store results with a presigned URL for easy download
    curr.execute(
        "SELECT * FROM wherobots_open_data.overture.places LIMIT 1000",
        store=Store.for_download()
    )
    store_result = curr.get_store_result()
    print(f"Results stored at: {store_result.result_uri}")
    print(f"Size: {store_result.size} bytes")
```

The `Store` class supports the following options:

* `format` (required): output format - `StorageFormat.PARQUET`,
    `StorageFormat.CSV`, or `StorageFormat.GEOJSON`
* `single`: if `True`, write results to a single file instead of
    multiple partitioned files (default: `False`)
* `generate_presigned_url`: if `True`, generate a presigned URL for
    downloading results (default: `False`)

Use `Store.for_download()` as a convenient shorthand for storing results
as a single Parquet file with a presigned URL.

#### Store options

You can pass format-specific Spark write options through the `options`
parameter. These correspond to the options available in Spark's
`DataFrameWriter` and are applied after the server's default options,
allowing you to override them.

```python
# CSV without headers and a custom delimiter
store = Store.for_download(
    format=StorageFormat.CSV,
    options={"header": "false", "delimiter": "|"},
)

# GeoJSON preserving null fields
store = Store.for_download(
    format=StorageFormat.GEOJSON,
    options={"ignoreNullFields": "false"},
)
```

### Execution progress

You can monitor the progress of running queries by registering a
progress handler on the connection.

```python
from wherobots.db import connect, ProgressInfo
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime

def on_progress(info: ProgressInfo) -> None:
    print(f"{info.tasks_completed}/{info.tasks_total} tasks "
          f"({info.tasks_active} active)")

with connect(
        api_key='...',
        runtime=Runtime.TINY,
        region=Region.AWS_US_WEST_2) as conn:
    conn.set_progress_handler(on_progress)
    curr = conn.cursor()
    curr.execute("SELECT ...")
    results = curr.fetchall()
```

The handler receives a `ProgressInfo` object with `execution_id`,
`tasks_total`, `tasks_completed`, and `tasks_active` fields. Pass
`None` to `set_progress_handler()` to disable progress reporting.

Progress events are best-effort and may not be available for all query
types or server versions. The handler is simply not invoked when no
progress information is available.

### Recovering from a lost connection

If the WebSocket to the SQL session drops while queries are running (a
network path failure, a keepalive timeout), the connection reconnects to
the same SQL session by itself and re-attaches every pending query, so
their results still arrive: there's nothing for your code to do. It keeps
trying, with backoff, for up to `reconnect_timeout_seconds` (60 seconds by
default; see below). It never starts a new session, and gives up at once
if the session refuses the connection, for example because it's gone.
While it reconnects, `execute()` and cancelling (closing a cursor, or
executing on it) wait for it to finish.

A re-attached query can still fail:

* If the SQL session doesn't know the execution (the statement didn't
    reach it before the connection dropped, or the session restarted),
    fetching raises `ExecutionNotFoundError`, a subclass of
    `OperationalError`.
* If the SQL session cancelled the execution while the connection was
    down (for example, its grace period for a lost connection ran out, or
    another client cancelled it), fetching raises
    `ExecutionCancelledError`, also an `OperationalError` subclass: its
    outcome, including whether a write committed, is unknown. An execution
    that you cancel yourself, by closing the cursor or executing on it,
    completes with an empty result, as usual.
* An older SQL session that doesn't support attaching still answers with
    the execution's state, and the driver polls it for the outcome (see
    `stale_query_probe_seconds` below). If such a session reports no
    location for a stored result, because it predates reporting it or
    because the stored result is empty, the driver can't tell which, and
    fails the query with an `OperationalError`. With probes disabled, a
    running execution on such a session fails right away.

If the connection can't reconnect in time, or reconnecting is turned off
(`reconnect_timeout_seconds=None`), fetching a pending query's outcome
raises `ConnectionLostError`, another `OperationalError` subclass. The
query may still have run, or be running, on the SQL session: its outcome,
including whether a write committed, is unknown. The error's
`execution_id` and `session_id` attributes identify it, and a cursor's
`execution_id` property holds the ID of its current or last execution.
(When `execute()` itself raises `ConnectionLostError`, the connection was
already lost and the statement was never sent.)

### Runtime and region selection

You can chose the Wherobots runtime you want to use using the `runtime`
parameter, passing in one of the `Runtime` enum values. For more
information on runtime sizing and selection, please consult the
[Wherobots product documentation](https://docs.wherobots.com).

You can also choose the region your SQL session runs in with the
`region` parameter. When omitted, your organization's configured default
region is used. Wherobots Cloud supports the following compute regions:

* `aws-us-east-1`: AWS US East 1 (N. Virginia)
* `aws-us-east-2`: AWS US East 2 (Ohio)
* `aws-us-west-2`: AWS US West 2 (Oregon)
* `aws-eu-west-1`: AWS EU West 1 (Ireland)
* `aws-ap-south-1`: AWS AP South 1 (Mumbai)

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
* `version`: one of the WherobotsDB runtime versions that is available
    to you, if you need to pin your usage to a particular, supported
    WherobotsDB version. Defaults to the latest, most-optimized version
    of WherobotsDB available to your subscription.
* `session_type`: `"single"` or `"multi"`; if set to `"single"`, then
    each call to `connect()` establishes an exclusive connection to a
    distinct and dedicated Wherobots runtime; if set to "multi", then
    multiple `connect()` calls with the same arguments and credentials
    will connect to the same shared Wherobots runtime; `"multi"` is the
    default.

    Consider multi-session for potential cost savings, but be mindful of
    performance impacts from shared resources. You might need to adjust
    cluster size if slowdowns occur, which could affect overall cost.
* `force_new`: passing `force_new=True` forces Wherobots Cloud to create
    and start a new SQL Session runtime for this connection instead of
    attempting to reuse an existing, available one. Note that this can
    severely impact the delay in obtaining a connection to your runtime.
* `shutdown_after_inactive_seconds`: how long the runtime waits and
    stays running after all clients have disconnected. This delay gives
    an opportunity for clients to reconnect to a previously-established
    runtime without having to start a new one.

    If you're using a simple "connect-query-disconnect" pattern from
    your application, you can set this parameter to a value greater than
    your expected time between queries and effectively get a continuously
    running SQL session runtime without any complex connection management
    in your application.
* `cancel_event`: a `threading.Event` that, when set, causes the
    connection attempt to abort promptly with an `InterfaceError`. This
    is useful when `connect()` is running in a background thread and the
    caller needs to interrupt it (e.g. on client disconnect or timeout).
    The event is checked before each HTTP request, between retry
    attempts, and before the WebSocket handshake.
* `session_key`: keeps this connection on SQL sessions of its own. The
    API reuses a session only for connections that ask with the same key,
    and never one opened without a key, so a heavy query elsewhere never
    holds the session your queries run on. When a new session would exceed
    your organization's quota, the API shares another of your sessions
    under another key instead, with or without `session_key`, and the
    connection's `session_fallback` is `"quota"` (it is `None` otherwise).
    `connection.session_id` names the session.
* `stale_query_probe_seconds`: how long a running query may go without
    any message from the SQL session before the driver asks the session
    for its state again, in case the query's completion event was lost.
    Repeat probes back off to at most 8 times this interval. Defaults to
    30 seconds; pass `None` to disable probing.
* `reconnect_timeout_seconds`: how long the connection keeps trying to
    reconnect to the same SQL session after losing its WebSocket (see
    "Recovering from a lost connection" above). Defaults to 60 seconds,
    which fits in the SQL session's 120-second grace period for the
    queries of a lost connection; pass `None` to turn reconnecting off.
