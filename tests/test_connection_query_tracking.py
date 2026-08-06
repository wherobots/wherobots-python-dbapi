"""Tests for connection-level query lifecycle tracking (WBC-922).

A query must be removed from the connection's tracking dict as soon as its
terminal result is delivered, on every terminal path — success with a store
result, success with an empty result, cancellation, and error. Retaining
completed queries pins their handlers (and any results those reference) for
the connection's lifetime.
"""

import json
import queue
from unittest.mock import MagicMock

import cbor2
import pyarrow

from wherobots.db.connection import Connection, Query
from wherobots.db.models import ExecutionResult, Store
from wherobots.db.types import ExecutionState, StorageFormat


def _make_connection():
    """Create a Connection with a mocked WebSocket."""
    mock_ws = MagicMock()
    # Prevent the background thread from running the main loop
    mock_ws.protocol.state = 4  # CLOSED state, so __main_loop exits immediately
    return Connection(mock_ws)


def _track_query(conn, execution_id="exec-1", state=ExecutionState.RUNNING, store=None):
    """Register a query on the connection and return its result queue."""
    result_queue = queue.Queue()
    query = Query(
        sql="SELECT 1",
        execution_id=execution_id,
        state=state,
        handler=result_queue.put,
        store=store,
    )
    conn._Connection__queries[execution_id] = query
    return result_queue


def _deliver(conn, message):
    """Feed one message through the connection's listener."""
    conn._Connection__ws.recv.return_value = json.dumps(message)
    conn._Connection__listen()


def _deliver_binary(conn, message):
    """Feed one CBOR-encoded message (used for binary result payloads)."""
    conn._Connection__ws.recv.return_value = cbor2.dumps(message)
    conn._Connection__listen()


class TestTerminalDeliveryStopsTracking:
    """Each terminal path must pop the query from __queries."""

    def test_store_result_success_is_untracked(self):
        conn = _make_connection()
        result_queue = _track_query(
            conn, store=Store.for_download(format=StorageFormat.GEOJSON)
        )

        _deliver(
            conn,
            {
                "kind": "state_updated",
                "execution_id": "exec-1",
                "state": "succeeded",
                "result_uri": "s3://results/exec-1",
                "size": 42,
            },
        )

        result = result_queue.get(timeout=1)
        assert result.store_result.result_uri == "s3://results/exec-1"
        assert "exec-1" not in conn._Connection__queries

    def test_empty_store_success_is_untracked(self):
        conn = _make_connection()
        result_queue = _track_query(
            conn, store=Store.for_download(format=StorageFormat.GEOJSON)
        )

        _deliver(
            conn,
            {
                "kind": "state_updated",
                "execution_id": "exec-1",
                "state": "succeeded",
                "result_uri": None,
                "size": None,
            },
        )

        result = result_queue.get(timeout=1)
        assert isinstance(result, ExecutionResult)
        assert "exec-1" not in conn._Connection__queries

    def test_empty_execution_result_is_untracked(self):
        conn = _make_connection()
        result_queue = _track_query(conn)

        _deliver(
            conn,
            {
                "kind": "execution_result",
                "execution_id": "exec-1",
                "state": "succeeded",
                "results": None,
            },
        )

        result = result_queue.get(timeout=1)
        assert result.results is None
        assert "exec-1" not in conn._Connection__queries

    def test_json_results_success_is_untracked(self):
        """The succeeded path with an actual JSON payload delivers decoded
        rows and stops tracking the query."""
        conn = _make_connection()
        result_queue = _track_query(conn)

        _deliver_binary(
            conn,
            {
                "kind": "execution_result",
                "execution_id": "exec-1",
                "state": "succeeded",
                "results": {
                    "result_bytes": b'[{"x": 1}, {"x": 2}]',
                    "format": "json",
                },
            },
        )

        result = result_queue.get(timeout=1)
        assert result.results == [{"x": 1}, {"x": 2}]
        assert "exec-1" not in conn._Connection__queries

    def test_arrow_results_success_is_untracked(self):
        """The succeeded path with an actual Arrow IPC payload delivers a
        DataFrame and stops tracking the query."""
        conn = _make_connection()
        result_queue = _track_query(conn)

        table = pyarrow.table({"x": [1, 2, 3]})
        sink = pyarrow.BufferOutputStream()
        with pyarrow.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)

        _deliver_binary(
            conn,
            {
                "kind": "execution_result",
                "execution_id": "exec-1",
                "state": "succeeded",
                "results": {
                    "result_bytes": sink.getvalue().to_pybytes(),
                    "format": "arrow",
                },
            },
        )

        result = result_queue.get(timeout=1)
        assert result.results["x"].tolist() == [1, 2, 3]
        assert "exec-1" not in conn._Connection__queries

    def test_cancelled_query_is_untracked(self):
        conn = _make_connection()
        result_queue = _track_query(conn)

        _deliver(
            conn,
            {
                "kind": "state_updated",
                "execution_id": "exec-1",
                "state": "cancelled",
            },
        )

        result = result_queue.get(timeout=1)
        assert result.results.empty
        assert "exec-1" not in conn._Connection__queries

    def test_errored_query_is_untracked(self):
        conn = _make_connection()
        result_queue = _track_query(conn)

        _deliver(
            conn,
            {
                "kind": "error",
                "execution_id": "exec-1",
                "message": "boom",
            },
        )

        result = result_queue.get(timeout=1)
        assert result.error is not None
        assert "exec-1" not in conn._Connection__queries

    def test_cancel_of_untracked_query_is_noop(self):
        """Cancelling an execution that already completed (and was popped)
        must not send anything over the wire."""
        conn = _make_connection()
        conn._Connection__ws.send.reset_mock()

        conn._Connection__cancel_query("exec-gone")

        conn._Connection__ws.send.assert_not_called()

    def test_non_terminal_state_update_keeps_tracking(self):
        """A running-state update is not terminal; the query stays tracked."""
        conn = _make_connection()
        _track_query(conn, state=ExecutionState.EXECUTION_REQUESTED)

        _deliver(
            conn,
            {
                "kind": "state_updated",
                "execution_id": "exec-1",
                "state": "running",
            },
        )

        assert "exec-1" in conn._Connection__queries

    def test_failed_state_keeps_tracking_until_error_event(self):
        """A failed-state update is not terminal by itself — the query must
        stay tracked so the follow-up error event can deliver the message."""
        conn = _make_connection()
        result_queue = _track_query(conn)

        _deliver(
            conn,
            {
                "kind": "state_updated",
                "execution_id": "exec-1",
                "state": "failed",
            },
        )

        assert "exec-1" in conn._Connection__queries
        assert result_queue.empty()

        _deliver(
            conn,
            {
                "kind": "error",
                "execution_id": "exec-1",
                "message": "boom",
            },
        )

        result = result_queue.get(timeout=1)
        assert "boom" in str(result.error)
        assert "exec-1" not in conn._Connection__queries
