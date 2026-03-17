import math
import queue
import re
from typing import Any, List, Tuple, Dict

from .errors import ProgrammingError
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
    def __init__(self, exec_fn, cancel_fn) -> None:
        self.__exec_fn = exec_fn
        self.__cancel_fn = cancel_fn

        self.__queue: queue.Queue = queue.Queue()
        self.__results: list[Any] | None = None
        self.__store_result: StoreResult | None = None
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

    def __on_execution_result(self, result) -> None:
        self.__queue.put(result)

    def __get_results(self) -> List[Tuple[Any, ...]] | None:
        if not self.__current_execution_id:
            raise ProgrammingError("No query has been executed yet")
        if self.__results is not None:
            return self.__results

        execution_result = self.__queue.get()
        if not isinstance(execution_result, ExecutionResult):
            raise ProgrammingError("Unexpected result type")

        if execution_result.error:
            raise execution_result.error

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
        if self.__current_execution_id:
            self.__cancel_fn(self.__current_execution_id)

        self.__results = None
        self.__store_result = None
        self.__current_row = 0
        self.__rowcount = -1
        self.__description = None

        self.__current_execution_id = self.__exec_fn(
            _substitute_parameters(operation, parameters),
            self.__on_execution_result,
            store,
        )

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
        if self.__results is None and self.__current_execution_id:
            self.__cancel_fn(self.__current_execution_id)

    def __iter__(self):
        return self

    def __next__(self) -> None:
        raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
