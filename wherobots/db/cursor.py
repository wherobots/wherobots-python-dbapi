import logging
import queue
from typing import Any, Optional, List, Tuple
import re

from .errors import ProgrammingError, DatabaseError

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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
        logger.info(f"__init__() - running...")
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
        logger.info(f"description() - running...")
        # logger.info(f"description() - self.__description: {self.__description}")
        return self.__description

    @property
    def rowcount(self) -> int:
        logger.info(f"rowcount() - running...")
        return self.__rowcount

    def __on_execution_result(self, result) -> None:
        logger.info(f"__on_execution_result() - running...")
        # logger.info(f"__on_execution_result() - result - {result}")
        self.__queue.put(result)

    def __get_results(self) -> Optional[List[Tuple[Any, ...]]]:
        logger.info(f"__get_results() - running...")
        if not self.__current_execution_id:
            raise ProgrammingError("__get_results() - No query has been executed yet")
        if self.__results is not None:
            # logger.info(f"__get_results() - 1 - type(self.__results): {type(self.__results)}")
            # logger.info(f"__get_results() - 1 - self.__results: {self.__results}")
            # logger.info(f"__get_results() - 1 - self.__rowcount: {self.__rowcount}")
            # logger.info(f"__get_results() - 1 - self.__current_row: {self.__current_row}")
            # logger.info(f"__get_results() - 1 - self.__description: {self.__description}")
            return self.__results

        # logger.info(f"__get_results() - self.__queue.get() - {self.__queue.get()}")
        # raise ProgrammingError("breaking!!")

        result = self.__queue.get()
        if isinstance(result, DatabaseError):
            raise result

        columns, column_types, rows = result
        self.__rowcount = len(rows)
        self.__results = rows

        # logger.info(f"__get_results() - 2 - type(rows): {type(rows)}")
        # logger.info(f"__get_results() - 2 - rows: {rows}")
        if isinstance(rows, DatabaseError):
            raise DatabaseError

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
        # logger.info(f"__get_results() - 2 - self.__rowcount: {self.__rowcount}")
        # logger.info(f"__get_results() - 2 - self.__current_row: {self.__current_row}")
        # logger.info(f"__get_results() - 2 - self.__description: {self.__description}")
        return self.__results

    def execute(self, operation: str, parameters: dict[str, Any] = None):
        logger.info(f"execute() - running...")
        if self.__current_execution_id:
            self.__cancel_fn(self.__current_execution_id)

        self.__results = None
        self.__current_row = 0
        self.__rowcount = -1
        self.__description = None

        sql = operation.format(**(parameters or {}))
        # sql = self._modify_statement(sql)
        # sql = self._sanitize_query(sql)
        logger.info(f"execute() - sql - {sql}")
        self.__current_execution_id = self.__exec_fn(sql, self.__on_execution_result)

    def executemany(self, operation: str, seq_of_parameters: list[dict[str, Any]]):
        logger.info(f"executemany() - running...")
        raise NotImplementedError

    def fetchone(self):
        logger.info(f"fetchone() - running...")
        results = self.__get_results()[self.__current_row :]
        if not results:
            logger.info(f"fetchone() - results is empty; returns None")
            return None
        self.__current_row += 1
        # logger.info(f"fetchone() - results[0]: {results[0]}")
        return results[0]

    def fetchmany(self, size: int = None):
        logger.info(f"fetchmany() - running...")
        size = size or self.arraysize
        results = self.__get_results()[self.__current_row : self.__current_row + size]
        self.__current_row += size
        return results

    def fetchall(self):
        logger.info(f"fetchall() - running...")
        return self.__get_results()[self.__current_row :]

    def close(self):
        logger.info(f"close() - running...")
        """Close the cursor."""
        pass

    def __iter__(self):
        logger.info(f"__iter__() - running...")
        return self

    def __next__(self):
        logger.info(f"__next__() - running...")
        raise StopIteration

    def __enter__(self):
        logger.info(f"__enter__() - running...")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info(f"__exit__() - running...")
        self.close()
