"""Tests for the empty store results fix.

These tests verify that:
1. A store-configured query with empty results (result_uri=null) completes
   promptly instead of hanging.
2. An execution_result event with results=null unblocks the cursor instead
   of hanging forever.
"""

import json
import queue
import threading
from unittest.mock import MagicMock, patch

import pytest

from wherobots.db.connection import Connection, Query
from wherobots.db.cursor import Cursor
from wherobots.db.models import ExecutionResult, Store, StoreResult
from wherobots.db.types import EventKind, ExecutionState, StorageFormat


class TestEmptyStoreResults:
    """Tests for the primary fix: store configured, result_uri is null."""

    def _make_connection_and_cursor(self):
        """Create a Connection with a mocked WebSocket and return (connection, cursor)."""
        mock_ws = MagicMock()
        # Prevent the background thread from running the main loop
        mock_ws.protocol.state = 4  # CLOSED state, so __main_loop exits immediately
        conn = Connection(mock_ws)
        cursor = conn.cursor()
        return conn, cursor

    def test_store_configured_empty_results_completes(self):
        """When a store-configured query succeeds with result_uri=null,
        the cursor should receive an empty ExecutionResult (not hang)."""
        result_queue = queue.Queue()

        def handler(result):
            result_queue.put(result)

        store = Store.for_download(format=StorageFormat.GEOJSON)
        query = Query(
            sql="SELECT * FROM t WHERE 1=0",
            execution_id="exec-1",
            state=ExecutionState.RUNNING,
            handler=handler,
            store=store,
        )

        conn, _ = self._make_connection_and_cursor()
        conn._Connection__queries["exec-1"] = query

        # Simulate receiving a state_updated message with result_uri=null
        message = {
            "kind": "state_updated",
            "execution_id": "exec-1",
            "state": "succeeded",
            "result_uri": None,
            "size": None,
        }
        conn._Connection__ws.recv.return_value = json.dumps(message)
        conn._Connection__listen()

        # The handler should have been called
        result = result_queue.get(timeout=1)
        assert isinstance(result, ExecutionResult)
        assert result.results is None
        assert result.error is None
        assert result.store_result is None
        assert query.state == ExecutionState.COMPLETED

    def test_store_configured_with_result_uri_produces_store_result(self):
        """When a store-configured query succeeds with result_uri set,
        the cursor should receive an ExecutionResult with store_result."""
        result_queue = queue.Queue()

        def handler(result):
            result_queue.put(result)

        store = Store.for_download(format=StorageFormat.GEOJSON)
        query = Query(
            sql="SELECT * FROM t",
            execution_id="exec-2",
            state=ExecutionState.RUNNING,
            handler=handler,
            store=store,
        )

        conn, _ = self._make_connection_and_cursor()
        conn._Connection__queries["exec-2"] = query

        message = {
            "kind": "state_updated",
            "execution_id": "exec-2",
            "state": "succeeded",
            "result_uri": "https://presigned-url.example.com/results.geojson",
            "size": 12345,
        }
        conn._Connection__ws.recv.return_value = json.dumps(message)
        conn._Connection__listen()

        result = result_queue.get(timeout=1)
        assert isinstance(result, ExecutionResult)
        assert result.results is None
        assert result.error is None
        assert result.store_result is not None
        assert result.store_result.result_uri == "https://presigned-url.example.com/results.geojson"
        assert result.store_result.size == 12345
        assert query.state == ExecutionState.COMPLETED

    def test_no_store_configured_calls_request_results(self):
        """When no store is configured and result_uri is null,
        __request_results should be called (existing behavior)."""
        result_queue = queue.Queue()

        def handler(result):
            result_queue.put(result)

        query = Query(
            sql="SELECT 1",
            execution_id="exec-3",
            state=ExecutionState.RUNNING,
            handler=handler,
            store=None,  # No store configured
        )

        conn, _ = self._make_connection_and_cursor()
        conn._Connection__queries["exec-3"] = query

        message = {
            "kind": "state_updated",
            "execution_id": "exec-3",
            "state": "succeeded",
            "result_uri": None,
            "size": None,
        }
        conn._Connection__ws.recv.return_value = json.dumps(message)

        with patch.object(conn, "_Connection__request_results") as mock_request:
            conn._Connection__listen()
            mock_request.assert_called_once_with("exec-3")

        # Handler should NOT have been called (waiting for execution_result)
        assert result_queue.empty()


class TestDefensiveNullResults:
    """Tests for the defensive fix: execution_result with results=null."""

    def _make_connection_and_cursor(self):
        mock_ws = MagicMock()
        mock_ws.protocol.state = 4
        conn = Connection(mock_ws)
        cursor = conn.cursor()
        return conn, cursor

    def test_null_results_in_execution_result_unblocks_cursor(self):
        """When execution_result arrives with results=null,
        the cursor should be unblocked with an empty ExecutionResult."""
        result_queue = queue.Queue()

        def handler(result):
            result_queue.put(result)

        query = Query(
            sql="SELECT 1",
            execution_id="exec-4",
            state=ExecutionState.RESULTS_REQUESTED,
            handler=handler,
            store=None,
        )

        conn, _ = self._make_connection_and_cursor()
        conn._Connection__queries["exec-4"] = query

        # Simulate execution_result with results=null
        # (this is what the server sends for store-only executions
        # when retrieve_results is erroneously called)
        message = {
            "kind": "execution_result",
            "execution_id": "exec-4",
            "state": "succeeded",
            "results": None,
        }
        conn._Connection__ws.recv.return_value = json.dumps(message)
        conn._Connection__listen()

        result = result_queue.get(timeout=1)
        assert isinstance(result, ExecutionResult)
        assert result.results is None
        assert result.error is None
        assert result.store_result is None
        assert query.state == ExecutionState.COMPLETED

    def test_empty_dict_results_in_execution_result_unblocks_cursor(self):
        """When execution_result arrives with results={} (empty dict),
        the cursor should be unblocked with an empty ExecutionResult."""
        result_queue = queue.Queue()

        def handler(result):
            result_queue.put(result)

        query = Query(
            sql="SELECT 1",
            execution_id="exec-5",
            state=ExecutionState.RESULTS_REQUESTED,
            handler=handler,
            store=None,
        )

        conn, _ = self._make_connection_and_cursor()
        conn._Connection__queries["exec-5"] = query

        message = {
            "kind": "execution_result",
            "execution_id": "exec-5",
            "state": "succeeded",
            "results": {},
        }
        conn._Connection__ws.recv.return_value = json.dumps(message)
        conn._Connection__listen()

        result = result_queue.get(timeout=1)
        assert isinstance(result, ExecutionResult)
        assert result.results is None
        assert result.error is None
        assert query.state == ExecutionState.COMPLETED
