"""Reconnecting by itself after a lost transport, and re-attaching queries."""

import itertools
import json
import logging
import queue
import threading
import time
from unittest.mock import MagicMock, patch

import cbor2
import pandas
import pyarrow
import pytest
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from wherobots.db import (
    ConnectionLostError,
    ExecutionCancelledError,
    ExecutionNotFoundError,
)
from wherobots.db.connection import Connection
from wherobots.db.driver import connect_direct
from wherobots.db.errors import InterfaceError
from wherobots.db.models import Store, StoreResult
from wherobots.db.types import StorageFormat

PROBE = 0.05
"""Probe interval used throughout: small, so the tests stay fast."""

READ_TIMEOUT = 0.01

NOT_FOUND = "Execution not found"

STORE = Store.for_download(format=StorageFormat.PARQUET)

RESULT_URI = "https://presigned.example.com/results.parquet"

WS_CONNECT = "wherobots.db.driver.websockets.sync.client.connect"


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    monkeypatch.setattr("wherobots.db.connection._RECONNECT_FIRST_DELAY_SECONDS", 0.01)
    monkeypatch.setattr("wherobots.db.connection._RECONNECT_MAX_DELAY_SECONDS", 0.05)


class Session:
    """A fake SQL session transport.

    Records every request. ``on_retrieve`` answers ``retrieve_results``
    requests (attaches and probes). With ``fail_sends``, a send raises as on a
    dead socket.
    """

    def __init__(self, on_retrieve=None):
        self.incoming = queue.Queue()
        self.sent = []
        self.on_retrieve = on_retrieve
        self.fail_sends = False
        self.close_timeout = 1.0
        self.socket = MagicMock()
        self.socket.shutdown.side_effect = lambda how: self.close()

    def recv(self, timeout):
        try:
            value = self.incoming.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError from None
        if isinstance(value, Exception):
            raise value
        # CBOR, as sql-session sends results: it carries their bytes.
        return cbor2.dumps(value)

    def send(self, value):
        if self.fail_sends:
            raise OSError("broken pipe")
        message = json.loads(value)
        self.sent.append(message)
        if message["kind"] == "retrieve_results" and self.on_retrieve is not None:
            self.on_retrieve(self, message)

    def close(self):
        self.incoming.put(ConnectionClosedOK(None, None))

    def drop(self):
        """The network path fails: the keepalive gives up on the connection."""
        self.incoming.put(ConnectionClosedError(None, None))

    def put(self, **message):
        self.incoming.put(message)

    def kinds(self):
        return [message["kind"] for message in list(self.sent)]

    def requests(self, kind, execution_id=None):
        return [
            message
            for message in list(self.sent)
            if message["kind"] == kind
            and (execution_id is None or message["execution_id"] == execution_id)
        ]


def answer(session, request, state="running", **fields):
    """Answer a retrieve_results as sql-session does: execution_result."""
    fields.setdefault("results", None)
    session.put(
        kind="execution_result",
        execution_id=request["execution_id"],
        state=state,
        **fields,
    )


def new_session(state="running", **fields):
    """A session that supports attaching: it acknowledges each attach."""
    return Session(
        on_retrieve=lambda s, m: answer(
            s, m, state, attached=bool(m.get("attach")), **fields
        )
    )


class Factory:
    """A transport factory handing out ``transports`` in turn.

    Each is a transport to return, an exception to raise, or a callable
    producing either. Records the time limit of every call.
    """

    def __init__(self, *transports):
        self.transports = iter(transports)
        self.calls = []

    def __call__(self, timeout):
        self.calls.append(timeout)
        transport = next(self.transports, OSError("connection refused"))
        if callable(transport) and not isinstance(transport, Session):
            transport = transport()
        if isinstance(transport, Exception):
            raise transport
        return transport


def open_connection(session, factory, **kwargs):
    kwargs.setdefault("stale_query_probe_seconds", PROBE)
    kwargs.setdefault("session_id", "session-1")
    kwargs.setdefault("reconnect_timeout_seconds", 3.0)
    return Connection(
        session, read_timeout=READ_TIMEOUT, transport_factory=factory, **kwargs
    )


def arrow_results(**columns):
    """A ``results`` payload carrying ``columns`` as an Arrow IPC stream."""
    table = pyarrow.table(columns)
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return {"result_bytes": sink.getvalue().to_pybytes(), "format": "arrow"}


def wait_until(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def tracked(conn, execution_id):
    return conn._Connection__queries.get(execution_id)


def outcome(cursor, timeout=3.0):
    """Wait for the cursor's terminal result, then hand it back to the cursor."""
    result = cursor._Cursor__queue.get(timeout=timeout)
    cursor._Cursor__queue.put(result)
    return result


def reader(conn):
    return conn._Connection__thread


def in_thread(fn):
    thread = threading.Thread(target=fn, daemon=True)
    thread.start()
    return thread


# Recovering pending queries


def test_lost_query_completes_over_the_new_transport(caplog):
    caplog.set_level(logging.INFO)
    lost, again = Session(), new_session("running")
    factory = Factory(again)
    conn = open_connection(lost, factory)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM t", store=STORE)
        execution_id = cursor.execution_id
        lost.drop()
        assert wait_until(lambda: again.requests("retrieve_results"))
        assert again.sent[0] == {
            "kind": "retrieve_results",
            "execution_id": execution_id,
            "attach": True,
        }
        again.put(
            kind="state_updated",
            execution_id=execution_id,
            state="succeeded",
            result_uri=RESULT_URI,
            size=7,
        )
        assert outcome(cursor).error is None
        assert cursor.get_store_result() == StoreResult(RESULT_URI, 7)
        assert len(factory.calls) == 1
        assert lost.socket.shutdown.called
    finally:
        conn.close()
    messages = [r.getMessage() for r in caplog.records]
    assert (
        "Lost the connection to SQL session session-1 with 1 pending queries; "
        "reconnecting for up to 3s ..."
    ) in messages
    assert (
        "Reconnected to SQL session session-1; re-attaching 1 pending queries."
    ) in messages


def test_result_lost_with_the_connection_arrives_in_the_attach_reply():
    # The query succeeded, but the terminal event went to the lost connection.
    again = Session(
        on_retrieve=lambda s, m: answer(
            s, m, "succeeded", attached=True, results=arrow_results(x=[1, 2])
        )
    )
    lost = Session()
    conn = open_connection(lost, Factory(again))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT x FROM t")
        lost.drop()
        assert outcome(cursor).error is None
        assert list(cursor.fetchall()["x"]) == [1, 2]
    finally:
        conn.close()


def test_every_pending_query_is_reattached():
    lost, again = Session(), new_session("running")
    conn = open_connection(lost, Factory(again))
    try:
        cursors = [conn.cursor() for _ in range(3)]
        for cursor in cursors:
            cursor.execute("SELECT 1", store=STORE)
        lost.drop()
        ids = [cursor.execution_id for cursor in cursors]
        assert wait_until(lambda: len(again.requests("retrieve_results")) == 3)
        assert {m["execution_id"] for m in again.requests("retrieve_results")} == (
            set(ids)
        )
        for execution_id in ids:
            again.put(
                kind="state_updated",
                execution_id=execution_id,
                state="succeeded",
                result_uri=f"{RESULT_URI}?{execution_id}",
            )
        for cursor in cursors:
            assert outcome(cursor).error is None
            assert cursor.get_store_result().result_uri.endswith(cursor.execution_id)
    finally:
        conn.close()


def test_reattached_query_is_polled_on_an_older_session():
    # An older session ignores the attach: its events go to the lost
    # connection, so only polling tells the outcome.
    replies = itertools.chain(["running"], itertools.repeat("succeeded"))

    def on_retrieve(s, m):
        state = next(replies)
        results = arrow_results(x=[3]) if state == "succeeded" else None
        s.put(
            kind="execution_result",
            execution_id=m["execution_id"],
            state=state,
            results=results,
        )

    lost, again = Session(), Session(on_retrieve=on_retrieve)
    conn = open_connection(lost, Factory(again))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT x FROM t")
        lost.drop()
        assert outcome(cursor).error is None
        assert list(cursor.fetchall()["x"]) == [3]
        assert len(again.requests("retrieve_results")) >= 2
    finally:
        conn.close()


def test_reattached_query_unknown_to_the_session_raises_not_found():
    again = Session(
        on_retrieve=lambda s, m: s.put(
            kind="error", execution_id=m["execution_id"], message=NOT_FOUND
        )
    )
    lost = Session()
    conn = open_connection(lost, Factory(again))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        lost.drop()
        error = outcome(cursor).error
        assert isinstance(error, ExecutionNotFoundError)
        assert error.execution_id == cursor.execution_id
        assert str(error) == (
            "Execution not found: SQL session session-1 has no execution "
            f"{cursor.execution_id} after reconnecting. The statement didn't "
            "reach the session before the connection was lost, the session "
            "restarted, or the session no longer tracks it."
        )
    finally:
        conn.close()


def test_query_cancelled_by_the_session_while_disconnected_raises():
    # E.g. the session's grace period for orphaned executions ran out.
    lost, again = Session(), new_session("cancelled")
    conn = open_connection(lost, Factory(again))
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE t SET x = 1")
        lost.drop()
        error = outcome(cursor).error
        assert isinstance(error, ExecutionCancelledError)
        assert error.execution_id == cursor.execution_id
    finally:
        conn.close()


def test_a_failed_send_hands_its_query_to_the_reconnect():
    again = Session(
        on_retrieve=lambda s, m: s.put(
            kind="error", execution_id=m["execution_id"], message=NOT_FOUND
        )
    )
    lost = Session()
    lost.fail_sends = True
    conn = open_connection(lost, Factory(again))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")  # Doesn't raise: the query is re-attached.
        assert wait_until(lambda: again.requests("retrieve_results"))
        assert again.sent[0]["execution_id"] == cursor.execution_id
        assert isinstance(outcome(cursor).error, ExecutionNotFoundError)
        assert reader(conn).is_alive()
    finally:
        conn.close()


# Eagerness, and sends while reconnecting


def test_reconnects_eagerly_with_nothing_pending():
    lost, again = Session(), Session()
    factory = Factory(again)
    conn = open_connection(lost, factory)
    try:
        lost.drop()
        assert wait_until(lambda: conn._Connection__ws is again)
        assert factory.calls
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        assert again.requests("execute_sql", cursor.execution_id)
        assert not lost.requests("execute_sql")
    finally:
        conn.close()


def test_execute_waits_for_the_reconnect_and_follows_the_reattaches():
    release = threading.Event()
    entered = threading.Event()
    again = new_session("running")

    def slow_connect():
        entered.set()
        release.wait(3)
        return again

    lost = Session()
    conn = open_connection(lost, Factory(slow_connect))
    try:
        first = conn.cursor()
        first.execute("SELECT 1", store=STORE)
        lost.drop()
        assert entered.wait(3)
        second = conn.cursor()
        executing = in_thread(lambda: second.execute("SELECT 2"))
        executing.join(0.1)
        assert executing.is_alive()  # Waiting for the reconnect.
        release.set()
        executing.join(3)
        assert not executing.is_alive()
        assert again.kinds()[:2] == ["retrieve_results", "execute_sql"]
        assert again.sent[0]["execution_id"] == first.execution_id
        assert again.sent[1]["execution_id"] == second.execution_id
        assert not lost.requests("execute_sql", second.execution_id)
    finally:
        release.set()
        conn.close()


def test_cancel_waits_for_the_reconnect_and_follows_the_attach():
    release = threading.Event()
    entered = threading.Event()
    again = new_session("running")

    def slow_connect():
        entered.set()
        release.wait(3)
        return again

    lost = Session()
    conn = open_connection(lost, Factory(slow_connect))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        execution_id = cursor.execution_id
        lost.drop()
        assert entered.wait(3)
        closing = in_thread(cursor.close)
        closing.join(0.1)
        assert closing.is_alive()
        # Not before the cancel goes out: a cancellation the session reports
        # meanwhile isn't this connection's.
        assert tracked(conn, execution_id).cancel_requested is False
        release.set()
        closing.join(3)
        assert not closing.is_alive()
        assert again.kinds()[:2] == ["retrieve_results", "cancel"]
        assert tracked(conn, execution_id).cancel_requested is True
        again.put(kind="state_updated", execution_id=execution_id, state="cancelled")
        result = outcome(cursor)
        assert result.error is None
        assert isinstance(result.results, pandas.DataFrame)
        assert result.results.empty
    finally:
        release.set()
        conn.close()


def test_a_cancel_sent_before_the_loss_is_sent_again_after_the_attach():
    lost, again = Session(), new_session("running")
    conn = open_connection(lost, Factory(again))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        execution_id = cursor.execution_id
        cursor.close()
        assert lost.requests("cancel", execution_id)
        lost.drop()
        assert wait_until(lambda: len(again.sent) >= 2)
        assert again.kinds()[:2] == ["retrieve_results", "cancel"]
        assert again.sent[1]["execution_id"] == execution_id
    finally:
        conn.close()


# Giving up


def test_pending_queries_fail_once_reconnecting_runs_out_of_time():
    lost = Session()
    factory = Factory()  # Every attempt fails.
    conn = open_connection(lost, factory, reconnect_timeout_seconds=0.3)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        started = time.monotonic()
        lost.drop()
        error = outcome(cursor).error
        elapsed = time.monotonic() - started
        assert isinstance(error, ConnectionLostError)
        assert error.execution_id == cursor.execution_id
        assert 0.25 <= elapsed < 2.0
        assert len(factory.calls) >= 2
        # Each attempt gets only the time left.
        assert all(0 < limit <= 0.3 for limit in factory.calls)
        assert wait_until(lambda: not reader(conn).is_alive())
        with pytest.raises(ConnectionLostError):
            conn.cursor().execute("SELECT 2")
    finally:
        conn.close()


def test_a_refused_reconnect_fails_pending_queries_at_once():
    lost = Session()
    factory = Factory(InterfaceError("SQL session refused the connection (HTTP 404)"))
    conn = open_connection(lost, factory, reconnect_timeout_seconds=30)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        lost.drop()
        assert isinstance(outcome(cursor).error, ConnectionLostError)
        assert len(factory.calls) == 1
    finally:
        conn.close()


def test_a_transport_lost_again_at_once_continues_the_same_recovery():
    def dropping():
        session = Session()
        session.drop()
        return session

    lost = Session()
    factory = Factory(*[dropping] * 1000)
    conn = open_connection(lost, factory, reconnect_timeout_seconds=0.3)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        lost.drop()
        # Without a shared deadline, this would reconnect forever.
        assert isinstance(outcome(cursor).error, ConnectionLostError)
        assert 2 <= len(factory.calls) < 100
    finally:
        conn.close()


def test_without_reconnecting_pending_queries_fail_at_once():
    lost = Session()
    factory = Factory(Session())
    conn = open_connection(lost, factory, reconnect_timeout_seconds=None)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        lost.drop()
        assert isinstance(outcome(cursor).error, ConnectionLostError)
        assert factory.calls == []
    finally:
        conn.close()


@pytest.mark.parametrize("value", [0, -1, float("nan"), "soon", True])
def test_an_invalid_reconnect_timeout_disables_reconnecting(value, caplog):
    lost = Session()
    factory = Factory(Session())
    conn = open_connection(lost, factory, reconnect_timeout_seconds=value)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        lost.drop()
        assert isinstance(outcome(cursor).error, ConnectionLostError)
        assert factory.calls == []
    finally:
        conn.close()
    assert any(
        "Invalid reconnect_timeout_seconds" in r.getMessage() for r in caplog.records
    )


# close() while reconnecting


def test_close_while_backing_off_fails_pending_queries_and_stops():
    lost = Session()
    factory = Factory()  # Every attempt fails.
    conn = open_connection(lost, factory, reconnect_timeout_seconds=30)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    lost.drop()
    assert wait_until(lambda: len(factory.calls) >= 2)
    started = time.monotonic()
    conn.close()
    assert time.monotonic() - started < 2.0
    assert isinstance(outcome(cursor).error, ConnectionLostError)
    assert wait_until(lambda: not reader(conn).is_alive())


def test_close_while_connecting_discards_the_new_transport():
    again = new_session("running")
    entered = threading.Event()

    def connect_during_close():
        entered.set()
        assert wait_until(lambda: conn._Connection__closed)
        return again

    lost = Session()
    conn = open_connection(lost, Factory(connect_during_close))
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    lost.drop()
    assert entered.wait(3)
    conn.close()
    assert isinstance(outcome(cursor).error, ConnectionLostError)
    assert wait_until(lambda: not reader(conn).is_alive())
    assert again.socket.shutdown.called
    assert again.sent == []


def test_an_execute_waiting_for_the_reconnect_fails_when_closed():
    release = threading.Event()
    entered = threading.Event()

    def slow_connect():
        entered.set()
        release.wait(3)
        return Session()

    lost = Session()
    conn = open_connection(lost, Factory(slow_connect))
    try:
        lost.drop()
        assert entered.wait(3)
        errors = []

        def execute():
            try:
                conn.cursor().execute("SELECT 1")
            except Exception as error:
                errors.append(error)

        executing = in_thread(execute)
        executing.join(0.1)
        assert executing.is_alive()
        conn.close()
        release.set()
        executing.join(3)
        assert len(errors) == 1
        assert isinstance(errors[0], ConnectionLostError)
    finally:
        release.set()
        conn.close()


# The transport factory from connect_direct()


def test_connect_direct_reconnects_to_the_same_websocket():
    first, again = Session(), new_session("running")
    headers = {"X-API-Key": "key"}
    with patch(WS_CONNECT, side_effect=[first, again]) as ws_connect:
        conn = connect_direct(
            "wss://compute.example.com/sql/org/session-id",
            headers=headers,
            read_timeout=READ_TIMEOUT,
            session_id="session-1",
            stale_query_probe_seconds=PROBE,
            reconnect_timeout_seconds=5,
        )
        try:
            headers["X-API-Key"] = "changed"
            cursor = conn.cursor()
            cursor.execute("SELECT 1", store=STORE)
            first.drop()
            assert wait_until(lambda: again.requests("retrieve_results"))
            initial, reconnect = ws_connect.call_args_list
            assert reconnect.kwargs["uri"] == initial.kwargs["uri"]
            # What the connection was opened with, not the caller's later edit.
            assert reconnect.kwargs["additional_headers"] == {"X-API-Key": "key"}
            # A single attempt, bounded by the time left to reconnect in.
            assert 0 < reconnect.kwargs["open_timeout"] <= 5
            for key in ("max_size", "close_timeout", "ssl"):
                assert reconnect.kwargs[key] == initial.kwargs[key]
        finally:
            conn.close()


def invalid_status(status):
    return InvalidStatus(Response(status, "", Headers()))


@pytest.mark.parametrize("status", [401, 403, 404])
def test_connect_direct_gives_up_when_the_session_refuses(status):
    first = Session()
    with patch(WS_CONNECT, side_effect=[first, invalid_status(status)]) as ws_connect:
        conn = connect_direct(
            "wss://compute/sql",
            read_timeout=READ_TIMEOUT,
            reconnect_timeout_seconds=30,
        )
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            first.drop()
            assert isinstance(outcome(cursor).error, ConnectionLostError)
            assert ws_connect.call_count == 2
        finally:
            conn.close()


@pytest.mark.parametrize("status", [429, 502, 503])
def test_connect_direct_retries_a_transient_refusal(status):
    first, again = Session(), new_session("running")
    with patch(
        WS_CONNECT, side_effect=[first, invalid_status(status), again]
    ) as ws_connect:
        conn = connect_direct(
            "wss://compute/sql",
            read_timeout=READ_TIMEOUT,
            reconnect_timeout_seconds=30,
        )
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            first.drop()
            assert wait_until(lambda: again.requests("retrieve_results"))
            assert ws_connect.call_count == 3
        finally:
            conn.close()


def test_connect_direct_does_not_reconnect_once_cancelled():
    cancel = threading.Event()
    first = Session()
    with patch(WS_CONNECT, side_effect=[first]) as ws_connect:
        conn = connect_direct(
            "wss://compute/sql",
            read_timeout=READ_TIMEOUT,
            cancel_event=cancel,
            reconnect_timeout_seconds=30,
        )
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cancel.set()
            first.drop()
            assert isinstance(outcome(cursor).error, ConnectionLostError)
            assert ws_connect.call_count == 1
        finally:
            conn.close()
