import math
import queue
import re
from typing import Any, Callable, List, Tuple, Dict

from .errors import NotSupportedError, ProgrammingError
from .models import ExecutionResult, Store, StoreResult

# Matches pyformat parameter markers: %(name)s
_PYFORMAT_RE = re.compile(r"%\(([^)]+)\)s")


def _quote_value(value: Any) -> str:
    """Convert a Python value to a SQL literal string.

    Handles quoting and escaping so that the interpolated SQL is syntactically
    correct and safe from trivial injection.
    """
    if value is None:
        return "NULL"
    # bool must be checked before int because bool is a subclass of int
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            raise ProgrammingError(
                f"Cannot convert float value {value!r} to SQL literal"
            )
        return str(value)
    if isinstance(value, bytes):
        return "X'" + value.hex() + "'"
    # Everything else (str, date, datetime, etc.) is treated as a string literal
    return "'" + str(value).replace("'", "''") + "'"


def _substitute_parameters(operation: str, parameters: Dict[str, Any] | None) -> str:
    """Substitute pyformat parameters into a SQL operation string.

    Uses regex to match only %(name)s tokens, leaving literal percent
    characters (e.g. SQL LIKE wildcards) untouched.  Values are quoted
    according to their Python type so the resulting SQL is syntactically
    correct (see :func:`_quote_value`).
    """
    if not parameters:
        return operation

    def replacer(match: re.Match) -> str:
        key = match.group(1)
        if key not in parameters:
            raise ProgrammingError(
                f"Parameter '{key}' not found in provided parameters"
            )
        return _quote_value(parameters[key])

    return _PYFORMAT_RE.sub(replacer, operation)


_TYPE_MAP = {
    "object": "STRING",
    "int64": "NUMBER",
    "float64": "NUMBER",
    "datetime64[ns]": "DATETIME",
    "timedelta[ns]": "DATETIME",
    "bool": "NUMBER",  # Assuming boolean is stored as number
    "bytes": "BINARY",
}


class Cursor:
    def __init__(self, exec_fn, cancel_fn, attach_fn=None) -> None:
        self.__exec_fn = exec_fn
        self.__cancel_fn = cancel_fn
        self.__attach_fn = attach_fn

        self.__queue: queue.Queue = queue.Queue()
        self.__results: list[Any] | None = None
        self.__store_result: StoreResult | None = None
        self.__complete: bool = False
        self.__error: Exception | None = None
        self.__current_execution_id: str | None = None
        self.__current_row: int = 0

        # Description and row count are set by the last executed operation.
        # Their default values are defined by PEP-0249.
        self.__description: List[Tuple] | None = None
        self.__rowcount: int = -1

        # Array-size is also defined by PEP-0249 and is expected to be read/writable.
        self.arraysize: int = 1

    @property
    def description(self) -> List[Tuple] | None:
        return self.__description

    @property
    def rowcount(self) -> int:
        return self.__rowcount

    @property
    def execution_id(self) -> str | None:
        """The ID of this cursor's current or last execution.

        ``None`` before the first ``execute()`` or ``attach()``, and after one
        that was rejected before submission. After a
        :class:`~wherobots.db.errors.ConnectionLostError`, pass it to
        ``attach()`` on a cursor of a reconnected connection.
        """
        return self.__current_execution_id

    def __in_flight_execution_id(self) -> str | None:
        """The current execution's id if its result has not yet arrived.

        Once a terminal result has been fetched (``__complete``) or is
        waiting in the queue, the execution is finished and must not be
        cancelled.

        Results are delivered from the connection's reader thread, so one
        may arrive between the ``empty()`` check and a cancellation that
        follows it. That race is benign: the cancel targets an execution
        that already completed — a server-side no-op, same as when a query
        finishes while a legitimate cancel is in flight — and the delivered
        result sits in this execution's own queue, so it can never be
        observed by a later execution's fetches.
        """
        if (
            self.__current_execution_id is not None
            and not self.__complete
            and self.__queue.empty()
        ):
            return self.__current_execution_id
        return None

    def __get_results(self) -> List[Tuple[Any, ...]] | None:
        if not self.__current_execution_id:
            raise ProgrammingError("No query has been executed yet")
        if self.__complete:
            if self.__error:
                raise self.__error
            return self.__results

        execution_result = self.__queue.get()
        if not isinstance(execution_result, ExecutionResult):
            raise ProgrammingError("Unexpected result type")

        # Whatever the outcome — rows, store export, empty result, or error —
        # the execution has reached a terminal state. ``__results`` alone
        # cannot signal this: store-backed and empty executions never set it.
        self.__complete = True

        if execution_result.error:
            self.__error = execution_result.error
            raise self.__error

        self.__store_result = execution_result.store_result
        results = execution_result.results

        # Results is None when results are stored in cloud storage
        if results is None:
            return None

        self.__rowcount = len(results)
        self.__results = results
        if not results.empty:
            self.__description = [
                (
                    col_name,  # name
                    _TYPE_MAP.get(str(results[col_name].dtype), "STRING"),  # type_code
                    None,  # display_size
                    results[col_name].memory_usage(),  # internal_size
                    None,  # precision
                    None,  # scale
                    True,  # null_ok; Assuming all columns can accept NULL values
                )
                for col_name in results.columns
            ]

        return self.__results

    def execute(
        self,
        operation: str,
        parameters: Dict[str, Any] | None = None,
        store: Store | None = None,
    ) -> None:
        self.__start(
            lambda handler: self.__exec_fn(
                _substitute_parameters(operation, parameters), handler, store
            )
        )

    def attach(self, execution_id: str, store: Store | None = None) -> None:
        """Re-attach to an execution already on the SQL session, by its ID.

        Use it to recover a query whose connection was lost: open a new
        connection to the same session with ``Connection.reconnect()``, then
        attach one of its cursors to ``ConnectionLostError.execution_id``.
        Afterwards, fetch the outcome as after ``execute()``.

        ``store`` must be the ``Store`` the execution was started with, if
        any: it tells the driver to expect a stored result. Like ``execute()``,
        this first cancels the cursor's own in-flight execution, and from then
        on the cursor owns the attached one: closing the cursor, or executing
        on it, before the outcome arrives cancels it.

        Fetching raises :class:`~wherobots.db.errors.ExecutionNotFoundError`
        if the session doesn't know the execution, for example because it ran
        on a different session or the session restarted. An older SQL session
        that doesn't support attaching still answers with the execution's
        state; the driver then polls it for the outcome (see
        ``stale_query_probe_seconds``). If such a session reports no location
        for a stored result (it predates reporting it, or the result is
        empty), the driver can't tell which, and fails the query with an
        ``OperationalError``.
        """
        if self.__attach_fn is None:
            raise NotSupportedError("This cursor can't attach to executions")
        if not isinstance(execution_id, str) or not execution_id:
            raise ProgrammingError("execution_id must be a non-empty string")
        if execution_id == self.__in_flight_execution_id():
            # Cancelling it first, as a new execution would, would cancel the
            # very execution being attached to.
            raise ProgrammingError(
                f"This cursor is already waiting for execution {execution_id}"
            )
        attach_fn = self.__attach_fn
        self.__start(lambda handler: attach_fn(execution_id, handler, store))

    def __start(self, submit: Callable[[Callable[[Any], None]], str]) -> None:
        """Reset for a new execution, then submit it with its result handler."""
        in_flight = self.__in_flight_execution_id()
        if in_flight:
            self.__cancel_fn(in_flight)

        # Each execution gets its own queue, and the handler closes over it:
        # a late result from a superseded execution lands in the orphaned
        # queue and can never be observed by fetches of the current one.
        self.__queue = queue.Queue()
        self.__results = None
        self.__store_result = None
        self.__complete = False
        self.__error = None
        self.__current_row = 0
        self.__rowcount = -1
        self.__description = None

        # A rejected submission must not leave the previous execution ID paired
        # with this new empty queue (which would make a later fetch wait forever).
        self.__current_execution_id = None
        self.__current_execution_id = submit(self.__queue.put)

    def get_store_result(self) -> StoreResult | None:
        """Get the store result for the last executed query.

        Returns the StoreResult containing the URI and size of the stored
        results, or None if the query was not configured to store results.

        This method blocks until the query completes.
        """
        if not self.__current_execution_id:
            raise ProgrammingError("No query has been executed yet")

        # Ensure we've waited for the result
        self.__get_results()
        return self.__store_result

    def executemany(
        self, operation: str, seq_of_parameters: List[Dict[str, Any]]
    ) -> None:
        raise NotImplementedError

    def fetchone(self) -> Any:
        results = self.__get_results()[self.__current_row :]
        if len(results) == 0:
            return None
        self.__current_row += 1
        return results[0]

    def fetchmany(self, size: int = None) -> List[Any]:
        size = size or self.arraysize
        results = self.__get_results()[self.__current_row : self.__current_row + size]
        self.__current_row += size
        return results

    def fetchall(self) -> List[Any]:
        return self.__get_results()[self.__current_row :]

    def close(self) -> None:
        """Close the cursor."""
        in_flight = self.__in_flight_execution_id()
        if in_flight:
            self.__cancel_fn(in_flight)

    def __iter__(self):
        return self

    def __next__(self) -> None:
        raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
