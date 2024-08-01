import json
import logging
import textwrap
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Union

import cbor2
import pyarrow
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

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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
        logger.info(f"__init__() - running...")
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
        logger.info(f"__enter__() - running...")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info(f"__exit__() - running...")
        self.close()

    def close(self):
        logger.info(f"close() - running...")
        self.__ws.close()

    def commit(self):
        pass

    def rollback(self):
        pass
    def cursor(self) -> Cursor:
        logger.info(f"cursor() - running...")
        return Cursor(self.__execute_sql, self.__cancel_query)

    def __main_loop(self):
        logger.info(f"__main_loop() - running...")
        """Main background loop listening for messages from the SQL session."""
        logging.info("__main_loop() - Starting background connection handling loop...")
        while self.__ws.protocol.state < websockets.protocol.State.CLOSING:
            try:
                self.__listen()
            except TimeoutError:
                # Expected, retry next time
                continue
            except websockets.exceptions.ConnectionClosedOK:
                logging.info("__main_loop() - Connection closed; stopping main loop.")
                return
            except Exception as e:
                logging.exception("__main_loop() - Error handling message from SQL session", exc_info=e)

    def __listen(self):
        """Waits for the next message from the SQL session and processes it.

        The code in this method is purposefully defensive to avoid unexpected situations killing the thread.
        """
        logger.info(f"__listen() - running...")
        message = self.__recv()
        logger.info(f"__listen() - message - {message}")
        kind = message.get("kind")
        logger.info(f"__listen() - kind - {kind}")
        execution_id = message.get("execution_id")
        logger.info(f"__listen() - execution_id - {execution_id}")
        if not kind or not execution_id:
            logger.info(f"__listen() - Invalid event")
            # Invalid event.
            return

        query = self.__queries.get(execution_id)
        if not query:
            logger.info(f"__listen() - Logging not query")
            logging.warning(
                "Received %s event for unknown execution ID %s", kind, execution_id
            )
            return

        if kind == EventKind.STATE_UPDATED:
            logger.info(f"__listen() - kind is EventKind.STATE_UPDATED - {EventKind.STATE_UPDATED}")
            try:
                query.state = ExecutionState[message["state"].upper()]
                logging.info("Query %s is now %s.", execution_id, query.state)
            except KeyError:
                logging.warning("__listen() - Invalid state update message for %s", execution_id)
                return

            # Incoming state transitions are handled here.
            if query.state == ExecutionState.SUCCEEDED:
                self.__request_results(execution_id)
            elif query.state == ExecutionState.FAILED:
                # Don't do anything here; the ERROR event is coming with more
                # details.
                pass

        elif kind == EventKind.EXECUTION_RESULT:
            logger.info(f"__listen() - kind is EventKind.EXECUTION_RESULT - {EventKind.EXECUTION_RESULT}")
            results = message.get("results")
            # logger.info(f"__listen() - results - {results}")
            if not results or not isinstance(results, dict):
                logging.warning("__listen() - Got no results back from %s.", execution_id)
                return

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
            if result_format == ResultsFormat.JSON:
                data = json.loads(result_bytes.decode("utf-8"))
                columns = data["columns"]
                column_types = data.get("column_types")
                rows = data["rows"]
                query.handler((columns, column_types, rows))
            elif result_format == ResultsFormat.ARROW:
                buffer = pyarrow.py_buffer(result_bytes)
                stream = pyarrow.input_stream(buffer, result_compression)
                with pyarrow.ipc.open_stream(stream) as reader:
                    schema = reader.schema
                    columns = schema.names
                    column_types = [field.type for field in schema]
                    rows = reader.read_all().to_pandas().values.tolist()
                    query.handler((columns, column_types, rows))
            else:
                query.handler(
                    OperationalError(f"Unsupported results format {result_format}")
                )
        elif kind == EventKind.ERROR:
            logger.info(f"__listen() - kind is EventKind.ERROR - {EventKind.ERROR}")
            query.state = ExecutionState.FAILED
            error = message.get("message")
            logger.info(f"__listen() - error - {error}")
            query.handler(OperationalError(error))
        else:
            logging.warning("Received unknown %s event!", kind)

    def __send(self, message: dict[str, Any]) -> None:
        logger.info(f"__main_loop() - running...")
        request = json.dumps(message)
        logging.debug("Request: %s", request)
        self.__ws.send(request)

    def __recv(self) -> dict[str, Any]:
        logger.info(f"__recv() - running...")
        frame = self.__ws.recv(timeout=self.__read_timeout)
        logger.info(f"__recv() - frame - {frame}")
        if isinstance(frame, str):
            logger.info(f"__recv() - frame instance 'str'")
            message = json.loads(frame)
        elif isinstance(frame, bytes):
            logger.info(f"__recv() - frame instance 'bytes'")
            message = cbor2.loads(frame)
        else:
            logger.info(f"__recv() - raising ValueError")
            raise ValueError("__recv() - Unexpected frame type received")
        # logger.info(f"__recv() - message - {message}")
        return message

    def __execute_sql(self, sql: str, handler: Callable[[Any], None]) -> str:
        logger.info(f"__execute_sql() - running...")
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
        logger.info(f"__request_results() - running...")
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
        logger.info(f"__cancel_query() - running...")
        query = self.__queries.pop(execution_id)
        if query:
            logging.info("__cancel_query() - Cancelled query %s.", execution_id)
            # TODO: when protocol supports it, send cancellation request.
