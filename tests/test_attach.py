"""Re-attaching pending queries after a reconnect: handling the replies."""

import json
import logging
import queue
import time
from unittest.mock import MagicMock, patch

import cbor2
import pyarrow
import pytest
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from wherobots.db import (
    ConnectionLostError,
    ExecutionCancelledError,
    ExecutionNotFoundError,
    OperationalError,
)
from wherobots.db.connection import Connection
from wherobots.db.models import Store, StoreResult
from wherobots.db.types import (
    DataCompression,
    ExecutionState,
    GeometryRepresentation,
    ResultsFormat,
    StorageFormat,
)

PROBE = 0.05
"""Probe interval used throughout: small, so the tests stay fast."""

SLOW_PROBE = 0.2
"""For assertions that something does *not* happen within N."""

READ_TIMEOUT = 0.01

NOT_FOUND = "Execution not found"

STORE = Store.for_download(format=StorageFormat.PARQUET)

RESULT_URI = "https://presigned.example.com/results.parquet"

EID = "exec-pending-when-lost"
"""The execution pending when the connection lost its transport."""


class Session:
    """A fake SQL session transport.

    Records every request with its send time. ``on_retrieve`` answers
    ``retrieve_results`` requests: attaches and probes.
    """

    def __init__(self, on_retrieve=None):
        self.incoming = queue.Queue()
        self.sent = []
        self.on_retrieve = on_retrieve
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
        if isinstance(value, bytes):
            return value
        return json.dumps(value)

    def send(self, value):
        message = json.loads(value)
        self.sent.append((time.monotonic(), message))
        if message["kind"] == "retrieve_results" and self.on_retrieve is not None:
            self.on_retrieve(self, message)

    def close(self):
        self.incoming.put(ConnectionClosedOK(None, None))

    def put(self, **message):
        self.incoming.put(message)

    def requests(self, kind, execution_id=None):
        return [
            (sent_at, message)
            for sent_at, message in list(self.sent)
            if message["kind"] == kind
            and (execution_id is None or message["execution_id"] == execution_id)
        ]

    def retrieves(self, execution_id=None):
        return [m for _, m in self.requests("retrieve_results", execution_id)]

    def retrieve_times(self, execution_id=None):
        return [t for t, _ in self.requests("retrieve_results", execution_id)]


def answer(session, request, state="running", **fields):
    """Answer a retrieve_results as sql-session does: execution_result."""
    session.put(
        kind="execution_result",
        execution_id=request["execution_id"],
        state=state,
        results=None,
        **fields,
    )


def new_session(state="running", **fields):
    """A session that supports attaching: it acknowledges each attach."""
    return Session(
        on_retrieve=lambda s, m: answer(
            s, m, state, attached=bool(m.get("attach")), **fields
        )
    )


def old_session(state="running", **fields):
    """A session that predates attaching: it ignores the attach flag."""
    return Session(on_retrieve=lambda s, m: answer(s, m, state, **fields))


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


def open_connection(session, **kwargs):
    kwargs.setdefault("stale_query_probe_seconds", PROBE)
    kwargs.setdefault("session_id", "session-1")
    return Connection(session, read_timeout=READ_TIMEOUT, **kwargs)


def reattach(session, store=None, **kwargs):
    """A connection whose query EID was pending when its transport was lost.

    The connection reconnects to ``session``, its only transport to come, and
    re-attaches the query there. Returns the connection and the query's
    cursor.
    """
    transports = iter([session])

    def transport_factory(timeout):
        transport = next(transports, None)
        if transport is None:
            raise OSError("connection refused")
        return transport

    kwargs.setdefault("reconnect_timeout_seconds", 0.5)
    lost = Session()
    conn = open_connection(lost, transport_factory=transport_factory, **kwargs)
    cursor = conn.cursor()
    with patch("wherobots.db.connection.uuid.uuid4", return_value=EID):
        cursor.execute("SELECT * FROM t", store=store)
    lost.incoming.put(ConnectionClosedError(None, None))
    return conn, cursor


def tracked(conn, execution_id):
    return conn._Connection__queries.get(execution_id)


def outcome(cursor, timeout=3.0):
    """Wait for the cursor's terminal result, then hand it back to the cursor.

    Fetching through the cursor would block forever on a regression; this
    fails the test instead, and leaves the cursor's own fetches working.
    """
    result = cursor._Cursor__queue.get(timeout=timeout)
    cursor._Cursor__queue.put(result)
    return result


def deliveries(cursor):
    return cursor._Cursor__queue.qsize()


# ConnectionLostError and Cursor.execution_id


@pytest.mark.parametrize(
    "loss", [ConnectionClosedError(None, None), OSError("transport lost")]
)
def test_transport_loss_raises_connection_lost_error_with_ids(loss):
    session = Session()
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1", store=STORE)
        session.incoming.put(loss)
        error = outcome(cursor).error
        assert isinstance(error, ConnectionLostError)
        assert isinstance(error, OperationalError)
        assert error.session_id == "session-1"
        assert error.execution_id == cursor.execution_id
        assert str(error) == (
            f"SQL connection lost (session=session-1, "
            f"execution={cursor.execution_id}). Commit outcome is unknown; "
            "verify the operation before retrying writes."
        )
        with pytest.raises(ConnectionLostError):
            cursor.get_store_result()
        # Submitting on the lost connection raises it too.
        with pytest.raises(ConnectionLostError) as exc:
            conn.cursor().execute("SELECT 2")
        assert exc.value.session_id == "session-1"
    finally:
        conn.close()


def test_connection_lost_error_without_a_session_id():
    session = Session()
    conn = Connection(session, read_timeout=READ_TIMEOUT)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    conn.close()
    error = outcome(cursor).error
    assert isinstance(error, ConnectionLostError)
    assert error.session_id is None
    assert error.execution_id == cursor.execution_id
    assert "session=unknown" in str(error)


def test_execution_id_tracks_the_current_execution():
    session = Session()
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        assert cursor.execution_id is None
        cursor.execute("SELECT 1")
        first = session.requests("execute_sql")[-1][1]["execution_id"]
        assert cursor.execution_id == first
        session.put(kind="state_updated", execution_id=first, state="cancelled")
        outcome(cursor)
        # Still the last execution's once it has completed.
        assert cursor.execution_id == first
        bad = MagicMock()
        bad.to_dict.return_value = {"invalid": object()}
        with pytest.raises(TypeError):
            cursor.execute("SELECT 2", store=bad)
        assert cursor.execution_id is None
    finally:
        conn.close()


# Re-attaching on a session that supports it


def test_attach_request_carries_the_connection_settings():
    session = new_session()
    conn, _ = reattach(
        session,
        store=STORE,
        results_format=ResultsFormat.JSON,
        data_compression=DataCompression.BROTLI,
        geometry_representation=GeometryRepresentation.WKB,
    )
    try:
        assert wait_until(session.retrieves)
        assert session.retrieves()[0] == {
            "kind": "retrieve_results",
            "execution_id": EID,
            "format": "json",
            "compression": "brotli",
            "geometry": "wkb",
            "attach": True,
        }
        assert session.requests("execute_sql") == []
    finally:
        conn.close()


def test_attach_on_new_session_completes_from_pushed_event(caplog):
    caplog.set_level(logging.INFO)
    session = new_session("running")
    conn, cursor = reattach(session, store=STORE)
    try:
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        assert tracked(conn, EID).attach.acknowledged
        assert deliveries(cursor) == 0
        assert f"Attached to execution {EID}" in caplog.text
        assert "doesn't support attaching" not in caplog.text
        # The session pushes the terminal event to the new owner.
        session.put(
            kind="state_updated",
            execution_id=EID,
            state="succeeded",
            result_uri=RESULT_URI,
            size=5,
        )
        assert outcome(cursor).error is None
        assert cursor.get_store_result() == StoreResult(RESULT_URI, 5)
        assert tracked(conn, EID) is None
    finally:
        conn.close()


@pytest.mark.parametrize("attached", [True, None])
def test_attach_reply_with_result_uri_completes(attached):
    fields = {"attached": attached} if attached else {}
    session = Session(
        on_retrieve=lambda s, m: answer(
            s, m, "succeeded", result_uri=RESULT_URI, size=9, **fields
        )
    )
    conn, cursor = reattach(session, store=STORE)
    try:
        assert outcome(cursor).error is None
        assert cursor.get_store_result() == StoreResult(RESULT_URI, 9)
        assert tracked(conn, EID) is None
    finally:
        conn.close()


def test_attach_reply_with_rows_completes():
    def on_retrieve(s, m):
        s.incoming.put(
            cbor2.dumps(
                {
                    "kind": "execution_result",
                    "execution_id": m["execution_id"],
                    "state": "succeeded",
                    "results": arrow_results(x=[1, 2]),
                    "attached": True,
                }
            )
        )

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session)
    try:
        assert outcome(cursor).error is None
        assert cursor.fetchall()["x"].tolist() == [1, 2]
        assert cursor.rowcount == 2
    finally:
        conn.close()


def test_acknowledged_empty_store_result_completes_empty():
    session = new_session("succeeded")
    conn, cursor = reattach(session, store=STORE)
    try:
        result = outcome(cursor)
        assert result.error is None
        assert result.store_result is None
        assert cursor.get_store_result() is None
        assert tracked(conn, EID) is None
    finally:
        conn.close()


@pytest.mark.parametrize("pushed_first", [True, False])
def test_pushed_terminal_event_racing_the_attach_reply_delivers_once(pushed_first):
    def on_retrieve(s, m):
        pushed = dict(
            kind="state_updated",
            execution_id=EID,
            state="succeeded",
            result_uri=RESULT_URI,
            size=1,
        )
        reply = dict(
            kind="execution_result",
            execution_id=EID,
            state="succeeded",
            results=None,
            result_uri=RESULT_URI,
            size=1,
            attached=True,
        )
        for message in (pushed, reply) if pushed_first else (reply, pushed):
            s.incoming.put(message)

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session, store=STORE)
    try:
        assert outcome(cursor).store_result.result_uri == RESULT_URI
        assert wait_until(session.incoming.empty)
        time.sleep(PROBE * 4)
        assert deliveries(cursor) == 1
        assert tracked(conn, EID) is None
        assert len(session.retrieves()) == 1
        assert not conn._Connection__closed
    finally:
        conn.close()


def test_pushed_success_racing_the_attach_reply_of_a_non_store_query():
    # The pushed state_updated makes the driver request the results normally;
    # the attach reply then carries them, and the normal reply is a duplicate.
    rows = cbor2.dumps(
        {
            "kind": "execution_result",
            "execution_id": EID,
            "state": "succeeded",
            "results": {"result_bytes": b'[{"x": 1}]', "format": "json"},
            "attached": True,
        }
    )

    def on_retrieve(s, m):
        if m.get("attach"):
            s.put(kind="state_updated", execution_id=EID, state="succeeded")
        s.incoming.put(rows)

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session, results_format=ResultsFormat.JSON)
    try:
        assert outcome(cursor).results == [{"x": 1}]
        assert wait_until(lambda: len(session.retrieves()) == 2)
        assert wait_until(session.incoming.empty)
        time.sleep(PROBE * 4)
        assert deliveries(cursor) == 1
        assert tracked(conn, EID) is None
    finally:
        conn.close()


def test_acknowledged_attach_survives_a_probe_not_found():
    # Evicted from the cache of a session that supports attaching: it still
    # pushes the terminal event here, so keep waiting, as for an execution.
    def on_retrieve(s, m):
        if m.get("attach"):
            answer(s, m, "running", attached=True)
        else:
            s.put(kind="error", execution_id=EID, message=NOT_FOUND)

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session, store=STORE)
    try:
        assert wait_until(lambda: len(session.retrieves()) == 2)
        assert wait_until(lambda: not tracked(conn, EID).watch.probeable)
        assert deliveries(cursor) == 0
        session.put(
            kind="state_updated",
            execution_id=EID,
            state="succeeded",
            result_uri=RESULT_URI,
        )
        assert outcome(cursor).store_result.result_uri == RESULT_URI
    finally:
        conn.close()


def test_acknowledged_attach_waits_with_probes_disabled():
    session = new_session("running")
    conn, cursor = reattach(session, store=STORE, stale_query_probe_seconds=None)
    try:
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        time.sleep(PROBE * 4)
        assert deliveries(cursor) == 0
        session.put(
            kind="state_updated",
            execution_id=EID,
            state="succeeded",
            result_uri=RESULT_URI,
        )
        assert outcome(cursor).store_result.result_uri == RESULT_URI
    finally:
        conn.close()


def assert_cancelled_elsewhere(error):
    assert isinstance(error, ExecutionCancelledError)
    assert isinstance(error, OperationalError)
    assert error.session_id == "session-1"
    assert error.execution_id == EID
    assert str(error) == (
        f"Execution {EID} was cancelled on SQL session session-1 before or "
        "while this connection re-attached to it (for example, its orphan "
        "grace period ran out, or another client cancelled it). Its outcome "
        "is unknown: for a write, whether it committed."
    )


@pytest.mark.parametrize("make_session", [new_session, old_session])
def test_attach_to_an_already_cancelled_execution_raises(make_session):
    # E.g. the session's orphan grace period ran out before the reconnect.
    session = make_session("cancelled")
    conn, cursor = reattach(session, store=STORE)
    try:
        assert_cancelled_elsewhere(outcome(cursor).error)
        with pytest.raises(ExecutionCancelledError):
            cursor.get_store_result()
        assert tracked(conn, EID) is None
    finally:
        conn.close()


def test_cancelled_pushed_after_attach_raises():
    session = new_session("running")
    conn, cursor = reattach(session)
    try:
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        session.put(kind="state_updated", execution_id=EID, state="cancelled")
        assert_cancelled_elsewhere(outcome(cursor).error)
        assert session.requests("cancel") == []
    finally:
        conn.close()


def test_cancelled_probe_reply_after_attach_on_old_session_raises():
    states = iter(["running"])
    session = Session(on_retrieve=lambda s, m: answer(s, m, next(states, "cancelled")))
    conn, cursor = reattach(session, store=STORE)
    try:
        assert_cancelled_elsewhere(outcome(cursor).error)
        assert len(session.retrieves()) == 2
    finally:
        conn.close()


@pytest.mark.parametrize("acknowledged", [True, False])
def test_own_cancel_of_an_attached_execution_completes_empty(acknowledged):
    def on_retrieve(s, m):
        state = "cancelled" if s.requests("cancel") else "running"
        fields = {"attached": True} if acknowledged and m.get("attach") else {}
        answer(s, m, state, **fields)

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session, store=STORE)
    try:
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        cursor.close()
        assert [m["execution_id"] for _, m in session.requests("cancel")] == [EID]
        if acknowledged:
            # Pushed to the new owner. An older session's next probe reply
            # says cancelled instead.
            session.put(kind="state_updated", execution_id=EID, state="cancelled")
        result = outcome(cursor)
        assert result.error is None
        assert result.results.empty
        assert tracked(conn, EID) is None
    finally:
        conn.close()


def test_executed_query_cancelled_elsewhere_still_completes_empty():
    session = Session()
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        session.put(
            kind="state_updated", execution_id=cursor.execution_id, state="cancelled"
        )
        result = outcome(cursor)
        assert result.error is None
        assert result.results.empty
    finally:
        conn.close()


@pytest.mark.parametrize("failed_reply", [True, False])
def test_attach_to_a_failed_execution_reports_its_error(failed_reply):
    # sql-session answers a retrieve for a failed execution with a failed
    # execution_result, then an error event with the details.
    def on_retrieve(s, m):
        if failed_reply:
            answer(s, m, "failed", attached=True)
        s.put(kind="error", execution_id=EID, message="Table not found: t")

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session)
    try:
        error = outcome(cursor).error
        assert type(error) is OperationalError
        assert str(error) == "Table not found: t"
    finally:
        conn.close()


# Re-attaching on an older session, which ignores the attach


@pytest.mark.parametrize("store", [STORE, None])
def test_attach_on_old_session_polls_until_succeeded(store, caplog):
    caplog.set_level(logging.INFO)
    rows = arrow_results(x=[2])
    states = iter(["running", "pending", "running"])

    def on_retrieve(s, m):
        state = next(states, "succeeded")
        if state != "succeeded":
            answer(s, m, state)
        elif store is not None:
            answer(s, m, state, result_uri=RESULT_URI, size=3)
        else:
            s.incoming.put(
                cbor2.dumps(
                    {
                        "kind": "execution_result",
                        "execution_id": EID,
                        "state": state,
                        "results": rows,
                    }
                )
            )

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session, store=store)
    try:
        result = outcome(cursor, timeout=5)
        assert result.error is None
        if store is not None:
            assert cursor.get_store_result() == StoreResult(RESULT_URI, 3)
        else:
            assert cursor.fetchall()["x"].tolist() == [2]
        attach_request, *probes = session.retrieves()
        assert attach_request["attach"] is True
        assert len(probes) == 3
        assert all("attach" not in probe for probe in probes)
        # The replies aren't events of their own: probes back off N, 2N, 4N.
        times = session.retrieve_times()
        gaps = [later - earlier for earlier, later in zip(times, times[1:])]
        for gap, expected in zip(gaps, [1, 2, 4]):
            assert gap >= PROBE * expected, gaps
        assert "doesn't support attaching; polling" in caplog.text
        assert tracked(conn, EID) is None
    finally:
        conn.close()


@pytest.mark.parametrize("after_probe", [False, True])
def test_old_session_store_result_without_location_fails_clearly(after_probe):
    replies = iter(["running"] if after_probe else [])

    def on_retrieve(s, m):
        answer(s, m, next(replies, "succeeded"))

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session, store=STORE)
    try:
        error = outcome(cursor).error
        assert type(error) is OperationalError
        assert "succeeded" in str(error)
        assert "can't be recovered" in str(error)
        assert "session=session-1" in str(error)
        with pytest.raises(OperationalError):
            cursor.get_store_result()
        assert len(session.retrieves()) == (2 if after_probe else 1)
        assert tracked(conn, EID) is None
    finally:
        conn.close()


def test_old_session_probe_not_found_fails_instead_of_waiting_forever():
    # The terminal event would go to the lost connection, never here.
    def on_retrieve(s, m):
        if m.get("attach"):
            answer(s, m, "running")
        else:
            s.put(kind="error", execution_id=EID, message=NOT_FOUND)

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session, store=STORE)
    try:
        error = outcome(cursor).error
        assert isinstance(error, ExecutionNotFoundError)
        assert "no longer tracks" in str(error)
        assert error.execution_id == EID
        assert tracked(conn, EID) is None
    finally:
        conn.close()


@pytest.mark.parametrize("state", ["running", "pending"])
def test_old_session_running_attach_fails_when_probes_are_disabled(state):
    session = old_session(state)
    conn, cursor = reattach(session, store=STORE, stale_query_probe_seconds=None)
    try:
        error = outcome(cursor).error
        assert type(error) is OperationalError
        assert "probes are disabled" in str(error)
        assert tracked(conn, EID) is None
    finally:
        conn.close()


# Re-attaching to an execution the session doesn't know


def test_attach_not_found_raises_execution_not_found():
    session = Session(
        on_retrieve=lambda s, m: s.put(
            kind="error", execution_id=m["execution_id"], message=NOT_FOUND
        )
    )
    conn, cursor = reattach(session, store=STORE)
    try:
        error = outcome(cursor).error
        assert isinstance(error, ExecutionNotFoundError)
        assert isinstance(error, OperationalError)
        assert error.session_id == "session-1"
        assert error.execution_id == EID
        assert f"no execution {EID} after reconnecting" in str(error)
        with pytest.raises(ExecutionNotFoundError):
            cursor.get_store_result()
        assert tracked(conn, EID) is None
        time.sleep(PROBE * 4)
        assert len(session.retrieves()) == 1
    finally:
        conn.close()


def test_not_found_for_a_repeated_attach_is_not_mistaken_for_eviction():
    # The first attach goes unanswered; the probe that repeats it gets
    # not-found. It's an attach reply: fail, don't keep waiting.
    def on_retrieve(s, m):
        if len(s.retrieves()) > 1:
            s.put(kind="error", execution_id=EID, message=NOT_FOUND)

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session)
    try:
        error = outcome(cursor).error
        assert isinstance(error, ExecutionNotFoundError)
        assert "after reconnecting" in str(error)
        assert [r.get("attach") for r in session.retrieves()] == [True, True]
    finally:
        conn.close()


@pytest.mark.parametrize("acknowledged", [True, False])
def test_not_found_for_the_second_of_two_attach_replies(acknowledged):
    # The attach and the probe repeating it are both outstanding. The first
    # reply says running; the session then evicts the execution, so the
    # second says not-found. Neither flag marks the second as a reply.
    def on_retrieve(s, m):
        if len(s.retrieves()) > 1:
            fields = {"attached": True} if acknowledged else {}
            answer(s, m, "running", **fields)
            s.put(kind="error", execution_id=EID, message=NOT_FOUND)

    session = Session(on_retrieve=on_retrieve)
    conn, cursor = reattach(session, store=STORE)
    try:
        if acknowledged:
            # Its terminal event is pushed here: keep waiting for it.
            assert wait_until(lambda: not tracked(conn, EID).watch.probeable)
            assert deliveries(cursor) == 0
            session.put(
                kind="state_updated",
                execution_id=EID,
                state="succeeded",
                result_uri=RESULT_URI,
            )
            assert outcome(cursor).store_result.result_uri == RESULT_URI
        else:
            error = outcome(cursor).error
            assert isinstance(error, ExecutionNotFoundError)
            assert "no longer tracks" in str(error)
    finally:
        conn.close()


# Watchdog: an unanswered attach


def test_unanswered_attach_gets_one_probe_after_4n_that_repeats_it():
    session = Session()  # Doesn't answer, at first.
    conn, cursor = reattach(session, store=STORE)
    try:
        assert wait_until(lambda: len(session.retrieves()) >= 2)
        attached_at, probed_at = session.retrieve_times()
        assert probed_at - attached_at >= PROBE * 4
        assert session.retrieves()[1]["attach"] is True
        # Never a second one while unanswered: the reply may be a full result.
        time.sleep(PROBE * 16)
        assert len(session.retrieves()) == 2
        # Once answered, probes resume with backoff, as plain retrieves.
        session.put(
            kind="execution_result",
            execution_id=EID,
            state="running",
            results=None,
            attached=True,
        )
        assert wait_until(lambda: len(session.retrieves()) >= 3)
        assert "attach" not in session.retrieves()[2]
        assert deliveries(cursor) == 0
    finally:
        conn.close()


# The cursor's part


@pytest.mark.parametrize("then", ["close", "execute"])
def test_the_cursor_still_owns_the_reattached_execution(then):
    session = new_session("running")
    conn, cursor = reattach(session, store=STORE)
    try:
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        if then == "close":
            cursor.close()
        else:
            cursor.execute("SELECT 2")
        assert [m["execution_id"] for _, m in session.requests("cancel")] == [EID]
    finally:
        conn.close()


def test_transport_lost_again_without_reconnecting_raises_connection_lost():
    session = new_session("running")
    conn, cursor = reattach(session, store=STORE, reconnect_timeout_seconds=0.2)
    try:
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        session.incoming.put(ConnectionClosedError(None, None))
        error = outcome(cursor).error
        assert isinstance(error, ConnectionLostError)
        assert error.execution_id == EID
    finally:
        conn.close()
