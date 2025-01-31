import json
import logging
import textwrap
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Union, Dict

import pandas
import pyarrow
import cbor2
import websockets.exceptions
import websockets.protocol
import websockets.sync.client

from wherobots.db.constants import (
    DEFAULT_READ_TIMEOUT_SECONDS,
    RequestKind,
    EventKind,
    ExecutionState,
    ResultsFormat,
    DataCompression,
    GeometryRepresentation,
)
from wherobots.db.cursor import Cursor
from wherobots.db.errors import NotSupportedError, OperationalError


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
    """

    def __init__(
        self,
        ws: websockets.sync.client.ClientConnection,
        read_timeout: float = DEFAULT_READ_TIMEOUT_SECONDS,
        results_format: Union[ResultsFormat, None] = None,
        data_compression: Union[DataCompression, None] = None,
        geometry_representation: Union[GeometryRepresentation, None] = None,
    ):
        self.__ws = ws
        self.__read_timeout = read_timeout
        self.__results_format = results_format
        self.__data_compression = data_compression
        self.__geometry_representation = geometry_representation

        self.__queries: dict[str, Query] = {}
        self.__thread = threading.Thread(
            target=self.__main_loop, daemon=True, name="wherobots-connection"
        )
        self.__thread.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        self.__ws.close()

    def commit(self) -> None:
        raise NotSupportedError

    def rollback(self) -> None:
        raise NotSupportedError

    def cursor(self) -> Cursor:
        return Cursor(self.__execute_sql, self.__cancel_query)

    def __main_loop(self) -> None:
        """Main background loop listening for messages from the SQL session."""
        logging.info("Starting background connection handling loop...")
        while self.__ws.protocol.state < websockets.protocol.State.CLOSING:
            try:
                self.__listen()
            except TimeoutError:
                # Expected, retry next time
                continue
            except websockets.exceptions.ConnectionClosedOK:
                logging.info("Connection closed; stopping main loop.")
                return
            except Exception as e:
                logging.exception("Error handling message from SQL session", exc_info=e)

    def __listen(self) -> None:
        """Waits for the next message from the SQL session and processes it.

        The code in this method is purposefully defensive to avoid unexpected situations killing the thread.
        """
        message = self.__recv()
        kind = message.get("kind")
        execution_id = message.get("execution_id")
        if not kind or not execution_id:
            # Invalid event.
            return

        query = self.__queries.get(execution_id)
        if not query:
            logging.warning(
                "Received %s event for unknown execution ID %s", kind, execution_id
            )
            return

        # Incoming state transitions are handled here.
        if kind == EventKind.STATE_UPDATED or kind == EventKind.EXECUTION_RESULT:
            try:
                query.state = ExecutionState[message["state"].upper()]
                logging.info("Query %s is now %s.", execution_id, query.state)
            except KeyError:
                logging.warning("Invalid state update message for %s", execution_id)
                return

            if query.state == ExecutionState.SUCCEEDED:
                # On a state_updated event telling us the query succeeded,
                # ask for results.
                if kind == EventKind.STATE_UPDATED:
                    self.__request_results(execution_id)
                    return

                # Otherwise, process the results from the execution_result event.
                results = message.get("results")
                if not results or not isinstance(results, dict):
                    logging.warning("Got no results back from %s.", execution_id)
                    return

                query.state = ExecutionState.COMPLETED
                query.handler(self._handle_results(execution_id, results))
            elif query.state == ExecutionState.CANCELLED:
                logging.info(
                    "Query %s has been cancelled; returning empty results.",
                    execution_id,
                )
                query.handler(pandas.DataFrame())
                self.__queries.pop(execution_id)
            elif query.state == ExecutionState.FAILED:
                # Don't do anything here; the ERROR event is coming with more
                # details.
                pass
        elif kind == EventKind.ERROR:
            query.state = ExecutionState.FAILED
            error = message.get("message")
            query.handler(OperationalError(error))
        else:
            logging.warning("Received unknown %s event!", kind)

    def _handle_results(self, execution_id: str, results: Dict[str, Any]) -> Any:
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

        if result_format == ResultsFormat.JSON:
            return json.loads(result_bytes.decode("utf-8"))
        elif result_format == ResultsFormat.ARROW:
            buffer = pyarrow.py_buffer(result_bytes)
            stream = pyarrow.input_stream(buffer, result_compression)
            with pyarrow.ipc.open_stream(stream) as reader:
                return reader.read_pandas()
        else:
            return OperationalError(f"Unsupported results format {result_format}")

    def __send(self, message: Dict[str, Any]) -> None:
        request = json.dumps(message)
        logging.debug("Request: %s", request)
        self.__ws.send(request)

    def __recv(self) -> Dict[str, Any]:
        frame = self.__ws.recv(timeout=self.__read_timeout)
        if isinstance(frame, str):
            message = json.loads(frame)
        elif isinstance(frame, bytes):
            message = cbor2.loads(frame)
        else:
            raise ValueError("Unexpected frame type received")
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

        logging.info(
            "Executing SQL query %s: %s", execution_id, textwrap.shorten(sql, width=60)
        )
        self.__send(request)
        return execution_id

    def __request_results(self, execution_id: str) -> None:
        query = self.__queries.get(execution_id)
        if not query:
            return

        request = {
            "kind": RequestKind.RETRIEVE_RESULTS.value,
            "execution_id": execution_id,
        }
        if self.__results_format:
            request["format"] = self.__results_format.value
        if self.__data_compression:
            request["compression"] = self.__data_compression.value
        if self.__geometry_representation:
            request["geometry"] = self.__geometry_representation.value

        query.state = ExecutionState.RESULTS_REQUESTED
        logging.info("Requesting results from %s ...", execution_id)
        self.__send(request)

    def __cancel_query(self, execution_id: str) -> None:
        """Cancels the query with the given execution ID."""
        query = self.__queries.get(execution_id)
        if not query:
            return

        request = {
            "kind": RequestKind.CANCEL.value,
            "execution_id": execution_id,
        }
        logging.info("Cancelling query %s...", execution_id)
        self.__send(request)
