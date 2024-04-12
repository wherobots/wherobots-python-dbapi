import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Any

import cbor2
import pyarrow

from wherobots.db.constants import (
    RequestKind,
    EventKind,
    ExecutionState,
    ResultsFormat,
    DataCompression,
)
from wherobots.db.cursor import Cursor
from wherobots.db.errors import NotSupportedError, DatabaseError, OperationalError


_DEFAULT_RESULTS_FORMAT = ResultsFormat.ARROW
_DEFAULT_DATA_COMPRESSION = DataCompression.BROTLI


@dataclass
class Query:
    sql: str
    execution_id: str
    state: ExecutionState
    handler: Callable[[Any], None]


class Connection:
    """
    A PEP-0249 compatible Connection object for Wherobots DB.

    The connection is backed by the WebSocket connected to the Wherobots SQL session instance.
    Transactions are not supported, so commit() and rollback() raise NotSupportedError.

    This class handles all the interactions with the remote SQL session, and the details of the
    Wherobots Spatial SQL API protocol. It supports multiple concurrent cursors, each one executing
    a single query at a time.

    A background thread listens for events from the SQL session, and handles update to the
    corresponding query state. Queries are tracked by their unique execution ID.

    Note: the Connection object MUST be used as a context manager.
    """

    def __init__(self, ws):
        self.__ws = ws
        self.__queries: dict[str, Query] = {}

    def __enter__(self):
        self.__executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="wherobots-sql-connection"
        )
        self.__executor.submit(self.__listen)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.__ws.close()
        self.__executor.shutdown(wait=True)

    def commit(self):
        raise NotSupportedError

    def rollback(self):
        raise NotSupportedError

    def cursor(self) -> Cursor:
        return Cursor(self.__execute_sql, self.__cancel_query)

    def __listen(self):
        """Main background loop listening for messages from the SQL session.

        The code in this method is purposefully defensive to avoid unexpected situations killing the thread.
        """
        while True:
            message = self.__recv()

            execution_id = message.get("execution_id")
            if not execution_id:
                continue

            query = self.__queries.get(execution_id)
            if not query:
                logging.warning(
                    "Received %s event for unknown execution ID %s", kind, execution_id
                )
                continue

            kind = message.get("kind")
            match kind:
                case EventKind.STATE_UPDATED:
                    try:
                        query.state = ExecutionState[message["state"].upper()]
                        logging.info("Query %s is now %s.", execution_id, query.state)
                    except KeyError:
                        logging.warning(
                            "Invalid state update message for %s", execution_id
                        )
                        continue

                    # Incoming state transitions are handled here.
                    match query.state:
                        case ExecutionState.SUCCEEDED:
                            self.__request_results(execution_id)
                        case ExecutionState.FAILED:
                            query.handler(OperationalError("Query execution failed"))

                case EventKind.EXECUTION_RESULT:
                    results = message.get("results")
                    if not results or not isinstance(results, dict):
                        logging.warning("Got no results back from %s.", execution_id)
                        continue

                    result_bytes = results.get("result_bytes")
                    result_format = results.get("format")
                    result_compression = results.get("compression")
                    logging.info(
                        "Received %d bytes of %s-compressed %s results from %s.",
                        len(result_bytes),
                        result_compression,
                        result_format,
                        execution_id,
                    )

                    query.state = ExecutionState.COMPLETED
                    match result_format:
                        case ResultsFormat.JSON:
                            query.handler(json.loads(result_bytes.decode("utf-8")))
                        case ResultsFormat.ARROW:
                            buffer = pyarrow.py_buffer(result_bytes)
                            stream = pyarrow.input_stream(buffer, result_compression)
                            with pyarrow.ipc.open_stream(stream) as reader:
                                query.handler(reader.read_pandas())
                        case _:
                            query.handler(
                                OperationalError(
                                    f"Unsupported results format {result_format}"
                                )
                            )
                case _:
                    logging.warning("Received unknown %s event!", kind)

    def __send(self, message: dict[str, Any]) -> None:
        logging.debug("Sending %s", message)
        self.__ws.send(json.dumps(message))

    def __recv(self) -> dict[str, Any]:
        frame = self.__ws.recv()
        if isinstance(frame, str):
            message = json.loads(frame)
        elif isinstance(frame, bytes):
            message = cbor2.loads(frame)
        else:
            raise ValueError("Unexpected frame type received")
        logging.debug("Received message: %s", message)
        return message

    def __execute_sql(self, sql: str, handler: Callable[[Any], None]) -> str:
        """Triggers the execution of the given SQL query."""
        execution_id = str(uuid.uuid4())
        request = {
            "kind": RequestKind.EXECUTE_SQL.value,
            "execution_id": execution_id,
            "statement": sql,
        }

        self.__queries[execution_id] = Query(
            sql=sql,
            execution_id=execution_id,
            state=ExecutionState.EXECUTION_REQUESTED,
            handler=handler,
        )
        self.__send(request)
        return execution_id

    def __request_results(self, execution_id: str) -> None:
        query = self.__queries.get(execution_id)
        if not query:
            return

        # TODO: Switch to Arrow encoding of results when supported.
        request = {
            "kind": RequestKind.RETRIEVE_RESULTS.value,
            "execution_id": execution_id,
            "format": _DEFAULT_RESULTS_FORMAT.value,
            "compression": _DEFAULT_DATA_COMPRESSION.value,
        }
        query.state = ExecutionState.RESULTS_REQUESTED
        logging.info("Requesting results from %s ...", execution_id)
        self.__send(request)

    def __cancel_query(self, execution_id: str) -> None:
        query = self.__queries.pop(execution_id)
        if query:
            logging.info("Cancelled query %s.", execution_id)
            # TODO: when protocol supports it, send cancellation request.
