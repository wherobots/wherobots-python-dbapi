from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
import json
import logging
import uuid
from typing import Any

from .constants import ExecutionState, RequestKind, EventKind
from .errors import ProgrammingError, OperationalError


class Cursor:

    def __init__(self, send_func, recv_func):
        self.__send_func = send_func
        self.__recv_func = recv_func

        self.__current_execution_id: str | None = None
        self.__current_execution_state: ExecutionState = ExecutionState.IDLE
        self.__current_execution_results: Future[list[Any]] | None = None
        self.__current_row: int = 0

        self.__executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="wherobots-sql-cursor"
        )

        # Description and row count are set by the last executed operation.
        # Their default values are defined by PEP-0249.
        self.__description: str | None = None
        self.__rowcount: int = -1

        # Array-size is also defined by PEP-0249 and is expected to be read/writable.
        self.arraysize: int = 1

    @property
    def description(self) -> str | None:
        return self.__description

    @property
    def rowcount(self) -> int:
        return self.__rowcount

    def close(self):
        self.__executor.shutdown()

    def __send_request(self, request):
        return self.__send_func(json.dumps(request))

    def execute(self, operation: str, parameters: dict[str, Any] = None):
        self.__current_execution_id = str(uuid.uuid4())
        self.__current_execution_state = ExecutionState.EXECUTION_REQUESTED
        self.__current_row = 0

        def _execute(request):
            """This function is executed in a separate thread to send the request, wait for, and gather the results."""
            logging.info("Executing SQL: %s", request["statement"])
            self.__send_request(request)
            return self.__gather_results()

        sql = operation.format(**(parameters or {}))
        exec_request = {
            "kind": RequestKind.EXECUTE_SQL.value,
            "execution_id": self.__current_execution_id,
            "statement": sql,
        }

        if self.__current_execution_results:
            self.__current_execution_results.cancel()
        self.__current_execution_results = self.__executor.submit(
            _execute, exec_request
        )

    def executemany(self, operation: str, seq_of_parameters: list[dict[str, Any]]):
        raise NotImplementedError

    def __get_results(self) -> list[Any] | None:
        if not self.__current_execution_results:
            raise ProgrammingError("No query has been executed yet")
        try:
            return self.__current_execution_results.result()
        except CancelledError:
            raise ProgrammingError(
                "Query execution was cancelled while waiting for results"
            )

    def __gather_results(self):
        """
        Reads from the SQL session for updates on the current execution state. Once the query has completed
        successfully, requests and processes the results.
        """
        while not self.__current_execution_state.is_terminal_state():
            response = json.loads(self.__recv_func())
            logging.debug("Received response: %s", response)

            kind = EventKind[response["kind"].upper()]
            match kind:
                case EventKind.STATE_UPDATED:
                    self.__current_execution_state = ExecutionState[
                        response["state"].upper()
                    ]
                    logging.info(
                        "Query %s is %s.",
                        self.__current_execution_id,
                        self.__current_execution_state,
                    )

                    match self.__current_execution_state:
                        case ExecutionState.SUCCEEDED:
                            results_request = {
                                "kind": RequestKind.RETRIEVE_RESULTS.value,
                                "execution_id": self.__current_execution_id,
                                "results_format": "json",
                            }
                            logging.info(
                                "Requesting results from %s ...",
                                self.__current_execution_id,
                            )
                            self.__send_request(results_request)
                            self.__current_execution_state = (
                                ExecutionState.RESULTS_REQUESTED
                            )
                        case ExecutionState.FAILED:
                            raise OperationalError("Execution failed")
                case EventKind.EXECUTION_RESULT:
                    self.__current_execution_state = ExecutionState.COMPLETED
                    results_format = response["results_format"]
                    logging.info(
                        "Received %s results from %s.",
                        results_format,
                        self.__current_execution_id,
                    )
                    if results_format != "json":
                        raise OperationalError(
                            f"Unsupported results format {results_format}"
                        )
                    # TODO: When full results are sent, we won't need to wrap them in a list to simulate a row.
                    return [json.loads(response["results"])]

    def fetchone(self):
        results = self.__get_results()[self.__current_row :]
        if not results:
            return None
        self.__current_row += 1
        return results[0]

    def fetchmany(self, size: int = None):
        size = size or self.arraysize
        results = self.__get_results()[self.__current_row : self.__current_row + size]
        self.__current_row += size
        return results

    def fetchall(self):
        return self.__get_results()[self.__current_row :]

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration
