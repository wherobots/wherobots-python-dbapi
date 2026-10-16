"""Recovering a query whose connection was lost: reconnect, then re-attach."""

import json
import logging
import queue
import threading
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
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from wherobots.db.connection import Connection
from wherobots.db.cursor import Cursor
from wherobots.db.driver import connect, connect_direct
from wherobots.db.errors import InterfaceError
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

EID = "exec-started-elsewhere"
"""An execution started by another (lost) connection."""

WS_CONNECT = "wherobots.db.driver.websockets.sync.client.connect"


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
        cursor.attach(EID)
        assert cursor.execution_id == EID
    finally:
        conn.close()


# Attaching on a session that supports it


def test_attach_request_carries_the_connection_settings():
    session = new_session()
    conn = open_connection(
        session,
        results_format=ResultsFormat.JSON,
        data_compression=DataCompression.BROTLI,
        geometry_representation=GeometryRepresentation.WKB,
    )
    try:
        conn.cursor().attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID)
        assert outcome(cursor).error is None
        assert cursor.fetchall()["x"].tolist() == [1, 2]
        assert cursor.rowcount == 2
    finally:
        conn.close()


def test_acknowledged_empty_store_result_completes_empty():
    session = new_session("succeeded")
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session, results_format=ResultsFormat.JSON)
    try:
        cursor = conn.cursor()
        cursor.attach(EID)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session, stale_query_probe_seconds=None)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
        assert_cancelled_elsewhere(outcome(cursor).error)
        with pytest.raises(ExecutionCancelledError):
            cursor.get_store_result()
        assert tracked(conn, EID) is None
    finally:
        conn.close()


def test_cancelled_pushed_after_attach_raises():
    session = new_session("running")
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID)
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        session.put(kind="state_updated", execution_id=EID, state="cancelled")
        assert_cancelled_elsewhere(outcome(cursor).error)
        assert session.requests("cancel") == []
    finally:
        conn.close()


def test_cancelled_probe_reply_after_attach_on_old_session_raises():
    states = iter(["running"])
    session = Session(on_retrieve=lambda s, m: answer(s, m, next(states, "cancelled")))
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID)
        error = outcome(cursor).error
        assert type(error) is OperationalError
        assert str(error) == "Table not found: t"
    finally:
        conn.close()


# Attaching on an older session, which ignores the attach


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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=store)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session, stale_query_probe_seconds=None)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
        error = outcome(cursor).error
        assert type(error) is OperationalError
        assert "probes are disabled" in str(error)
        assert tracked(conn, EID) is None
    finally:
        conn.close()


# Attaching to an execution the session doesn't know


def test_attach_not_found_raises_execution_not_found():
    session = Session(
        on_retrieve=lambda s, m: s.put(
            kind="error", execution_id=m["execution_id"], message=NOT_FOUND
        )
    )
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
        error = outcome(cursor).error
        assert isinstance(error, ExecutionNotFoundError)
        assert isinstance(error, OperationalError)
        assert error.session_id == "session-1"
        assert error.execution_id == EID
        assert f"no execution {EID} to attach to" in str(error)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID)
        error = outcome(cursor).error
        assert isinstance(error, ExecutionNotFoundError)
        assert "to attach to" in str(error)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
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


# Cursor.attach semantics


def test_attach_cancels_the_cursors_own_in_flight_execution_first():
    session = new_session("running")
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        previous = cursor.execution_id
        cursor.attach(EID, store=STORE)
        (_, cancel), *_ = session.requests("cancel")
        assert cancel["execution_id"] == previous
        # The superseded execution's late result can't leak into the attach.
        session.put(kind="state_updated", execution_id=previous, state="cancelled")
        session.put(
            kind="state_updated",
            execution_id=EID,
            state="succeeded",
            result_uri=RESULT_URI,
        )
        assert outcome(cursor).store_result.result_uri == RESULT_URI
        assert deliveries(cursor) == 1
    finally:
        conn.close()


@pytest.mark.parametrize("then", ["close", "execute"])
def test_the_cursor_owns_the_attached_execution(then):
    session = new_session("running")
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        if then == "close":
            cursor.close()
        else:
            cursor.execute("SELECT 2")
        assert [m["execution_id"] for _, m in session.requests("cancel")] == [EID]
    finally:
        conn.close()


def test_attach_to_the_cursors_own_in_flight_execution_is_rejected():
    session = Session()
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        own = cursor.execution_id
        with pytest.raises(ProgrammingError, match="already waiting"):
            cursor.attach(own)
        # Not cancelled, and still waiting on its result.
        assert session.requests("cancel") == []
        assert cursor.execution_id == own
        session.put(kind="state_updated", execution_id=own, state="cancelled")
        assert outcome(cursor).error is None
    finally:
        conn.close()


def test_attach_to_an_execution_tracked_by_another_cursor_is_rejected():
    session = Session()
    conn = open_connection(session)
    try:
        other = conn.cursor()
        other.execute("SELECT 1")
        cursor = conn.cursor()
        with pytest.raises(ProgrammingError, match="already tracking"):
            cursor.attach(other.execution_id)
        assert cursor.execution_id is None
        assert session.retrieves() == []
        # The other cursor still gets its result.
        session.put(
            kind="state_updated", execution_id=other.execution_id, state="cancelled"
        )
        assert outcome(other).error is None
    finally:
        conn.close()


@pytest.mark.parametrize("execution_id", ["", None, 42])
def test_attach_requires_an_execution_id(execution_id):
    conn = open_connection(Session())
    try:
        with pytest.raises(ProgrammingError):
            conn.cursor().attach(execution_id)
    finally:
        conn.close()


def test_cursor_without_an_attach_function_cannot_attach():
    cursor = Cursor(MagicMock(), MagicMock())
    with pytest.raises(NotSupportedError):
        cursor.attach(EID)


def test_attach_on_a_closed_connection_raises_connection_lost():
    session = Session()
    conn = open_connection(session)
    conn.close()
    cursor = conn.cursor()
    with pytest.raises(ConnectionLostError) as exc:
        cursor.attach(EID)
    assert exc.value.execution_id == EID
    assert cursor.execution_id is None


def test_transport_loss_while_attached_raises_connection_lost():
    session = new_session("running")
    conn = open_connection(session)
    try:
        cursor = conn.cursor()
        cursor.attach(EID, store=STORE)
        assert wait_until(lambda: tracked(conn, EID).state == ExecutionState.RUNNING)
        session.incoming.put(ConnectionClosedError(None, None))
        error = outcome(cursor).error
        assert isinstance(error, ConnectionLostError)
        assert error.execution_id == EID
    finally:
        conn.close()


# Connection.reconnect


def test_reconnect_opens_the_same_session_websocket_with_the_same_settings():
    sessions = [Session(), new_session("running"), Session()]
    headers = {"X-API-Key": "key", "X-Trace": "abc"}
    with patch(WS_CONNECT, side_effect=sessions) as ws_connect:
        conn = connect_direct(
            "wss://compute.example.com/sql/org/session-id",
            headers=headers,
            read_timeout=READ_TIMEOUT,
            results_format=ResultsFormat.JSON,
            data_compression=DataCompression.BROTLI,
            geometry_representation=GeometryRepresentation.WKB,
            session_id="session-1",
            stale_query_probe_seconds=SLOW_PROBE,
        )
        # Not inherited, and a later change to the caller's dict isn't sent.
        conn.set_progress_handler(lambda info: None)
        headers["X-Trace"] = "changed"
        again = conn.reconnect()
        third = again.reconnect()
    try:
        assert again is not conn
        calls = ws_connect.call_args_list
        assert len(calls) == 3
        # The first connect still gets the caller's headers verbatim.
        assert calls[0].kwargs["additional_headers"] is headers
        for call in calls:
            assert call.kwargs["uri"] == (
                "wss://compute.example.com/sql/org/session-id/1.0.0"
            )
        for call in calls[1:]:
            assert call.kwargs["additional_headers"] == {
                "X-API-Key": "key",
                "X-Trace": "abc",
            }
        ignored = {"ssl", "additional_headers"}
        first_kwargs = {k: v for k, v in calls[0].kwargs.items() if k not in ignored}
        for call in calls[1:]:
            assert {
                k: v for k, v in call.kwargs.items() if k not in ignored
            } == first_kwargs
        assert again._Connection__progress_handler is None
        assert again._Connection__probe_after == SLOW_PROBE
        assert again._Connection__session_id == "session-1"
        # The connection's settings shape the attach request.
        cursor = again.cursor()
        cursor.attach(EID, store=STORE)
        assert sessions[1].retrieves()[0] == {
            "kind": "retrieve_results",
            "execution_id": EID,
            "format": "json",
            "compression": "brotli",
            "geometry": "wkb",
            "attach": True,
        }
    finally:
        for connection in (conn, again, third):
            connection.close()


@patch("wherobots.db.driver.requests.get")
@patch("wherobots.db.driver.requests.post")
def test_reconnect_after_connect_goes_straight_to_the_websocket(mock_post, mock_get):
    post_resp = MagicMock()
    post_resp.url = "https://api.example.com/sql/session/test-id"
    mock_post.return_value = post_resp
    get_resp = MagicMock()
    get_resp.json.return_value = {
        "status": "READY",
        "appMeta": {"url": "https://compute.example.com/sql/org/session-id"},
    }
    mock_get.return_value = get_resp
    with patch(WS_CONNECT, side_effect=[Session(), Session()]) as ws_connect:
        conn = connect(api_key="key", stale_query_probe_seconds=PROBE)
        again = conn.reconnect()
    try:
        # No new session lookup: a session that's gone must fail fast.
        assert mock_post.call_count == 1
        assert mock_get.call_count == 1
        first, second = ws_connect.call_args_list
        assert second.kwargs["uri"] == first.kwargs["uri"]
        assert first.kwargs["uri"] == (
            "wss://compute.example.com/sql/org/session-id/1.0.0"
        )
        assert second.kwargs["additional_headers"] == first.kwargs["additional_headers"]
        assert second.kwargs["additional_headers"]["X-API-Key"] == "key"
        assert again._Connection__session_id == "test-id"
    finally:
        conn.close()
        again.close()


def test_reconnect_honours_the_cancel_event():
    cancel = threading.Event()
    with patch(WS_CONNECT, side_effect=[Session()]) as ws_connect:
        conn = connect_direct("wss://compute/sql", cancel_event=cancel)
        cancel.set()
        with pytest.raises(InterfaceError, match="cancelled by caller"):
            conn.reconnect()
    conn.close()
    assert ws_connect.call_count == 1


def test_reconnect_without_a_factory_is_not_supported():
    conn = Connection(Session(), read_timeout=READ_TIMEOUT)
    try:
        with pytest.raises(NotSupportedError):
            conn.reconnect()
    finally:
        conn.close()


def test_lost_query_is_recovered_by_reconnecting_and_attaching():
    lost, recovered = Session(), new_session("running")
    with patch(WS_CONNECT, side_effect=[lost, recovered]):
        conn = connect_direct(
            "wss://compute/sql",
            read_timeout=READ_TIMEOUT,
            session_id="session-1",
            stale_query_probe_seconds=PROBE,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM t", store=STORE)
        lost.incoming.put(ConnectionClosedError(None, None))
        error = outcome(cursor).error
        assert isinstance(error, ConnectionLostError)
        conn.close()
        again = conn.reconnect()
    try:
        assert error.execution_id == cursor.execution_id
        recovered_cursor = again.cursor()
        recovered_cursor.attach(error.execution_id, store=STORE)
        recovered.put(
            kind="state_updated",
            execution_id=error.execution_id,
            state="succeeded",
            result_uri=RESULT_URI,
            size=7,
        )
        assert outcome(recovered_cursor).error is None
        assert recovered_cursor.get_store_result() == StoreResult(RESULT_URI, 7)
    finally:
        again.close()
