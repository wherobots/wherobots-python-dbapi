import queue
from typing import Any, Optional, List, Tuple

from .errors import ProgrammingError, DatabaseError

_TYPE_MAP = {
    "string": "STRING",
    "int32": "NUMBER",
    "int64": "NUMBER",
    "float32": "NUMBER",
    "float64": "NUMBER",
    "double": "NUMBER",
    "bool": "NUMBER",  # Assuming boolean is stored as number
    "bytes": "BINARY",
    "struct": "STRUCT",
    "list": "LIST",
    "geometry": "GEOMETRY"
}


class Cursor:

    def __init__(self, exec_fn, cancel_fn):
        print("\n       Running Wherobots-python-DBAPI __init__()\n")
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
        print("\n       Running Wherobots-python-DBAPI description()\n")
        print(f"\n      self.__description: {self.__description}\n")
        return self.__description

    @property
    def rowcount(self) -> int:
        print("\n       Running Wherobots-python-DBAPI rowcount()\n")
        return self.__rowcount

    def __on_execution_result(self, result) -> None:
        print("\n       Running Wherobots-python-DBAPI __on_execution_result()\n")
        self.__queue.put(result)

    def __get_results(self) -> Optional[List[Tuple[Any, ...]]]:
        print("\n       Running Wherobots-python-DBAPI __get_results()\n")
        if not self.__current_execution_id:
            raise ProgrammingError("No query has been executed yet")
        if self.__results is not None:
            print(f"\n       1 - type(self.__results): {type(self.__results)}\n")
            print(f"\n       1 - self.__results: {self.__results}\n")
            print(f"\n       1 - self.__rowcount: {self.__rowcount}\n")
            print(f"\n       1 - self.__current_row: {self.__current_row}\n")
            print(f"\n       1 - self.__description: {self.__description}\n")
            return self.__results

        columns, column_types, rows = self.__queue.get()
        print(f"\n       2 - type(rows): {type(rows)}\n")
        print(f"\n       2 - rows: {rows}\n")
        if isinstance(rows, DatabaseError):
            raise rows

        self.__rowcount = len(rows)
        self.__results = rows
        if rows:
            self.__description = [
                (
                    col_name,  # name
                    _TYPE_MAP.get(str(column_types[i]), "STRING"),  # type_code
                    None,  # display_size
                    None,  # internal_size
                    None,  # precision
                    None,  # scale
                    True,  # null_ok; Assuming all columns can accept NULL values
                )
                for i, col_name in enumerate(columns)
            ]
        print(f"\n       2 - self.__rowcount: {self.__rowcount}\n")
        print(f"\n       2 - self.__current_row: {self.__current_row}\n")
        print(f"\n       2 - self.__description: {self.__description}\n")
        return self.__results

    def execute(self, operation: str, parameters: dict[str, Any] = None):
        print("\n       Running Wherobots-python-DBAPI execute()\n")
        if self.__current_execution_id:
            self.__cancel_fn(self.__current_execution_id)

        self.__results = None
        self.__current_row = 0
        self.__rowcount = -1
        self.__description = None

        sql = operation.format(**(parameters or {}))
        self.__current_execution_id = self.__exec_fn(sql, self.__on_execution_result)

    def executemany(self, operation: str, seq_of_parameters: list[dict[str, Any]]):
        raise NotImplementedError

    def fetchone(self):
        print("\n       Running Wherobots-python-DBAPI fetchone()\n")
        results = self.__get_results()[self.__current_row :]
        if not results:
            print("\n       results is empty; returns None\n")
            return None
        self.__current_row += 1
        print(f"\n       results[0]: {results[0]}\n")
        return results[0]

    def fetchmany(self, size: int = None):
        print("\n       Running Wherobots-python-DBAPI fetchmany()\n")
        size = size or self.arraysize
        results = self.__get_results()[self.__current_row : self.__current_row + size]
        self.__current_row += size
        return results

    def fetchall(self):
        print("\n       Running Wherobots-python-DBAPI fetchall()\n")
        return self.__get_results()[self.__current_row :]

    def close(self):
        """Close the cursor."""
        pass

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
