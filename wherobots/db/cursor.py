import queue
from typing import Any, Optional, List, Tuple, Dict

from .errors import DatabaseError, ProgrammingError

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
        self.__results: Optional[list[Any]] = None
        self.__current_execution_id: Optional[str] = None
        self.__current_row: int = 0

        # Description and row count are set by the last executed operation.
        # Their default values are defined by PEP-0249.
        self.__description: Optional[List[Tuple]] = None
        self.__rowcount: int = -1

        # Array-size is also defined by PEP-0249 and is expected to be read/writable.
        self.arraysize: int = 1

    @property
    def description(self) -> Optional[List[Tuple]]:
        return self.__description

    @property
    def rowcount(self) -> int:
        return self.__rowcount

    def __on_execution_result(self, result) -> None:
        self.__queue.put(result)

    def __get_results(self) -> Optional[List[Tuple[Any, ...]]]:
        if not self.__current_execution_id:
            raise ProgrammingError("No query has been executed yet")
        if self.__results is not None:
            return self.__results

        result = self.__queue.get()
        if isinstance(result, DatabaseError):
            raise result

        self.__rowcount = len(result)
        self.__results = result
        if not result.empty:
            self.__description = [
                (
                    col_name,  # name
                    _TYPE_MAP.get(str(result[col_name].dtype), "STRING"),  # type_code
                    None,  # display_size
                    result[col_name].memory_usage(),  # internal_size
                    None,  # precision
                    None,  # scale
                    True,  # null_ok; Assuming all columns can accept NULL values
                )
                for col_name in result.columns
            ]

        return self.__results

    def execute(self, operation: str, parameters: Dict[str, Any] = None) -> None:
        if self.__current_execution_id:
            self.__cancel_fn(self.__current_execution_id)

        self.__results = None
        self.__current_row = 0
        self.__rowcount = -1
        self.__description = None

        self.__current_execution_id = self.__exec_fn(
            operation % (parameters or {}), self.__on_execution_result
        )

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
