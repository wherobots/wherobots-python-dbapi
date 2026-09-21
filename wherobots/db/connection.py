import json
import logging
import textwrap
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict

from wherobots.db.redaction import get_statement_type, redact_sql

import pandas
import pyarrow
import cbor2
import websockets.exceptions
import websockets.sync.client

from .constants import DEFAULT_READ_TIMEOUT_SECONDS
from ._transport import abort_connection
from .cursor import Cursor
from .errors import NotSupportedError, OperationalError
from .models import ExecutionResult, ProgressInfo, Store, StoreResult
from .types import (
    RequestKind,
    EventKind,
    ExecutionState,
    ResultsFormat,
    DataCompression,
    GeometryRepresentation,
)


ProgressHandler = Callable[[ProgressInfo], None]
"""A callable invoked with a :class:`ProgressInfo` on every progress event."""


class _TransportError(Exception):
    """An I/O failure raised while receiving from the WebSocket."""


@dataclass
class Query:
    sql: str
    execution_id: str
    state: ExecutionState
    handler: Callable[[Any], None]
    store: Store | None = None


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
        results_format: ResultsFormat | None = None,
        data_compression: DataCompression | None = None,
        geometry_representation: GeometryRepresentation | None = None,
        session_id: str | None = None,
        on_connection_lost: Callable[[], None] | None = None,
    ):
        self.__ws = ws
        self.__read_timeout = read_timeout
        self.__results_format = results_format
        self.__data_compression = data_compression
        self.__geometry_representation = geometry_representation
        self.__progress_handler: ProgressHandler | None = None

        self.__session_id = session_id
        # Internal notification hook: schedule optional diagnostics, never wait
        # for remote results here. All cursor failures are delivered first.
        self.__on_connection_lost = on_connection_lost
        self.__lock = threading.Lock()
        self.__send_lock = threading.Lock()
        self.__shutdown_done = threading.Event()
        self.__shutdown_owner: int | None = None
        self.__closed = False
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
        """Abort the transport, fail pending work, and wait up to 1s for the reader.

        Closing doesn't imply that server-side writes were rolled back. A
        decoder or callback can outlive the bounded reader join.
        """
        deadline = time.monotonic() + 1.0
        self.__fail_pending(notify=False)
        # A handler may close its own connection during terminal delivery.
        if self.__shutdown_owner == threading.get_ident():
            return
        self.__shutdown_done.wait(max(0.0, deadline - time.monotonic()))
        if self.__thread is not threading.current_thread():
            self.__thread.join(timeout=max(0.0, deadline - time.monotonic()))

    def commit(self) -> None:
        raise NotSupportedError

    def rollback(self) -> None:
        raise NotSupportedError

    def cursor(self) -> Cursor:
        return Cursor(self.__execute_sql, self.__cancel_query)

    def set_progress_handler(self, handler: ProgressHandler | None) -> None:
        """Register a callback invoked for execution progress events.

        When a handler is set, every ``execute_sql`` request automatically
        includes ``enable_progress_events: true`` so the SQL session streams
        progress updates for running queries.

        Pass ``None`` to disable progress reporting.

        This follows the `sqlite3 Connection.set_progress_handler()
        <https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.set_progress_handler>`_
        pattern (PEP 249 vendor extension).
        """
        self.__progress_handler = handler

    def __main_loop(self) -> None:
        """Main background loop listening for messages from the SQL session."""
        logging.info("Starting background connection handling loop...")
        try:
            self.__receive_loop()
        finally:
            self.__fail_pending()

    def __receive_loop(self) -> None:
        # recv drains buffered results before raising ConnectionClosed.
        while True:
            if self.__shutdown_done.is_set():
                return
            try:
                self.__listen()
            except TimeoutError:
                # Expected, retry next time
                continue
            except websockets.exceptions.ConnectionClosed:
                logging.info("Connection closed; stopping main loop.")
                return
            except _TransportError:
                logging.exception("SQL session transport failed; stopping main loop")
                return
            except Exception as e:
                logging.exception("Error handling message from SQL session", exc_info=e)

    def __connection_error(self, execution_id: str) -> OperationalError:
        message = (
            f"SQL connection lost (session={self.__session_id or 'unknown'}, "
            f"execution={execution_id}). Commit outcome is unknown; "
            "verify the operation before retrying writes."
        )
        return OperationalError(message)

    def __fail_pending(self, notify: bool = True) -> None:
        # Stop admission first. Do not wait for __send_lock: its owner may be
        # blocked in network I/O. __closed means closing until shutdown_done.
        with self.__lock:
            if self.__closed:
                return
            self.__closed = True
            self.__shutdown_owner = threading.get_ident()
        try:
            abort_connection(self.__ws)
        except OSError:
            # The adapter still closes the socket object in its finally block.
            logging.exception("Socket shutdown failed; socket was closed")
        with self.__lock:
            pending = list(self.__queries.values())
            self.__queries.clear()
        try:
            for query in pending:
                try:
                    query.handler(
                        ExecutionResult(
                            error=self.__connection_error(query.execution_id)
                        )
                    )
                except Exception:
                    logging.exception(
                        "Could not deliver connection failure to query handler"
                    )
        finally:
            self.__shutdown_owner = None
            self.__shutdown_done.set()
        if notify and pending and self.__on_connection_lost is not None:
            try:
                self.__on_connection_lost()
            except Exception:
                logging.debug("Could not schedule session diagnostics")

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

        # Progress events are independent of the query state machine and don't
        # require a tracked query — the handler is connection-level.
        if kind == EventKind.EXECUTION_PROGRESS:
            handler = self.__progress_handler
            if handler is None:
                return
            try:
                handler(
                    ProgressInfo(
                        execution_id=execution_id,
                        tasks_total=message.get("tasks_total", 0),
                        tasks_completed=message.get("tasks_completed", 0),
                        tasks_active=message.get("tasks_active", 0),
                    )
                )
            except Exception:
                logging.exception("Progress handler raised an exception")
            return

        query = self.__queries.get(execution_id)
        if not query:
            logging.warning(
                "Received %s event for unknown execution ID %s", kind, execution_id
            )
            return

        def complete_query(result: ExecutionResult) -> None:
            # Terminal delivery: stop tracking the query first. Keeping it in
            # __queries would retain its handler — and the results the handler
            # references — for the connection's lifetime (WBC-922).
            with self.__lock:
                claimed = self.__queries.pop(execution_id, None)
            if claimed is not None:
                claimed.handler(result)

        def fail_query(action: str, error: Exception) -> None:
            # This is a query-local failure, not evidence of transport loss.
            # Exception text may contain result data; report only its type.
            query.state = ExecutionState.FAILED
            message = (
                f"Could not {action} SQL results "
                f"(session={self.__session_id or 'unknown'}, "
                f"execution={execution_id}; {type(error).__name__}). "
                "The statement may have completed; verify the operation before "
                "retrying writes."
            )
            logging.error("%s", message)
            complete_query(ExecutionResult(error=OperationalError(message)))

        # Incoming state transitions are handled here.
        if kind == EventKind.STATE_UPDATED or kind == EventKind.EXECUTION_RESULT:
            try:
                query.state = ExecutionState[message["state"].upper()]
                logging.info("Query %s is now %s.", execution_id, query.state)
            except (KeyError, AttributeError, TypeError) as error:
                fail_query("interpret", error)
                return

            if query.state == ExecutionState.SUCCEEDED:
                # On a state_updated event telling us the query succeeded,
                # check if results are stored in cloud storage or need to be fetched.
                if kind == EventKind.STATE_UPDATED:
                    result_uri = message.get("result_uri")
                    if result_uri:
                        # Results are stored in cloud storage
                        store_result = StoreResult(
                            result_uri=result_uri,
                            size=message.get("size"),
                        )
                        logging.info(
                            "Query %s results stored at: %s (size: %s)",
                            execution_id,
                            result_uri,
                            store_result.size,
                        )
                        query.state = ExecutionState.COMPLETED
                        complete_query(ExecutionResult(store_result=store_result))
                        return

                    if query.store is not None:
                        # Store was configured but produced no results (empty result set)
                        logging.info(
                            "Query %s completed with store configured but no results to store.",
                            execution_id,
                        )
                        query.state = ExecutionState.COMPLETED
                        complete_query(ExecutionResult())
                        return

                    # No store configured, request results normally
                    try:
                        self.__request_results(execution_id)
                    except Exception as error:
                        # Transport failures are handled by __send; a local
                        # retrieval failure must not orphan this execution.
                        fail_query("request", error)
                    return

                # Otherwise, process the results from the execution_result event.
                results = message.get("results")
                if results is None or results == {}:
                    logging.warning("Got no results back from %s.", execution_id)
                    query.state = ExecutionState.COMPLETED
                    complete_query(ExecutionResult())
                    return

                try:
                    if not isinstance(results, dict):
                        raise TypeError("Expected a result object")
                    decoded = self._handle_results(execution_id, results)
                except Exception as error:
                    # Even OSError/TimeoutError here belong to decoding, not
                    # recv. Claim and deliver exactly once, including if close
                    # concurrently claims this execution.
                    fail_query("decode", error)
                else:
                    query.state = ExecutionState.COMPLETED
                    complete_query(ExecutionResult(results=decoded))
            elif query.state == ExecutionState.CANCELLED:
                logging.info(
                    "Query %s has been cancelled; returning empty results.",
                    execution_id,
                )
                complete_query(ExecutionResult(results=pandas.DataFrame()))
            elif query.state == ExecutionState.FAILED:
                # Don't do anything here; the ERROR event is coming with more
                # details.
                pass
        elif kind == EventKind.ERROR:
            query.state = ExecutionState.FAILED
            error = message.get("message")
            complete_query(ExecutionResult(error=OperationalError(error)))
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
            raise NotSupportedError("Unsupported results format")

    def __send(self, message: Dict[str, Any], query: Query | None = None) -> None:
        # Serialization and redaction are local work. Fail before registration,
        # without poisoning unrelated cursors or misreporting a transport loss.
        request = json.dumps(message)
        # Only compute the redacted request (json.dumps + sqlparse parse) when
        # DEBUG is actually enabled; the log argument is evaluated eagerly, so an
        # unguarded call would redact on every request even with DEBUG off.
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug("Request: %s", self.__redacted_request(message))
        with self.__send_lock:
            with self.__lock:
                if self.__closed:
                    if query is not None:
                        raise self.__connection_error(query.execution_id)
                    return
                if query is not None:
                    self.__queries[query.execution_id] = query
                elif message.get("execution_id") not in self.__queries:
                    return
            try:
                self.__ws.send(request)
            except (websockets.exceptions.ConnectionClosed, OSError):
                pass  # Terminate outside the send gate, before delivering errors.
            except Exception:
                # API/programming errors aren't evidence of connection loss.
                if query is not None:
                    with self.__lock:
                        self.__queries.pop(query.execution_id, None)
                raise
            else:
                return
        self.__fail_pending()

    @staticmethod
    def __redacted_request(message: Dict[str, Any]) -> str:
        """Serialize a request for logging with any SQL statement redacted.

        The wire payload (sent verbatim by ``__send``) carries the raw
        ``statement``; this driver is embedded by other services, so logging it
        -- even at DEBUG -- would leak raw SQL into their log streams (WBC-139).
        """
        statement = message.get("statement")
        if isinstance(statement, str):
            message = {**message, "statement": redact_sql(statement)}
        return json.dumps(message)

    def __recv(self) -> Dict[str, Any]:
        try:
            frame = self.__ws.recv(timeout=self.__read_timeout)
        except TimeoutError:
            raise  # Idle polls are expected, not terminal I/O failures.
        except OSError as e:
            # Distinguish transport I/O failures from OSErrors raised later by
            # protocol parsing or result decoding; only the former are terminal.
            raise _TransportError from e
        if isinstance(frame, str):
            message = json.loads(frame)
        elif isinstance(frame, bytes):
            message = cbor2.loads(frame)
        else:
            raise ValueError("Unexpected frame type received")
        return message

    def __execute_sql(
        self,
        sql: str,
        handler: Callable[[Any], None],
        store: Store | None = None,
    ) -> str:
        """Triggers the execution of the given SQL query."""
        execution_id = str(uuid.uuid4())
        request = {
            "kind": RequestKind.EXECUTE_SQL.value,
            "execution_id": execution_id,
            "statement": sql,
        }

        if self.__progress_handler is not None:
            request["enable_progress_events"] = True

        if store:
            request["store"] = store.to_dict()

        # Redact literal values before logging: this driver is embedded by other
        # services, so raw SQL here would leak into their log streams (WBC-139).
        logging.info(
            "Executing SQL query %s (%s): %s",
            execution_id,
            get_statement_type(sql),
            textwrap.shorten(redact_sql(sql), width=200),
        )
        self.__send(
            request,
            Query(
                sql=sql,
                execution_id=execution_id,
                state=ExecutionState.EXECUTION_REQUESTED,
                handler=handler,
                store=store,
            ),
        )
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
