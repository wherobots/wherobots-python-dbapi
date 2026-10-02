"""The stale-query watchdog re-asks the session when a terminal event is lost."""

import json
import logging
import queue
import threading
import time
from unittest.mock import MagicMock, patch

import cbor2
import pytest
from websockets.exceptions import ConnectionClosedOK

from wherobots.db.connection import Connection
from wherobots.db.driver import connect_direct
from wherobots.db.errors import OperationalError
from wherobots.db.models import Store
from wherobots.db.types import ExecutionState, ResultsFormat, StorageFormat

PROBE = 0.05
"""Probe interval used throughout: small, so the tests stay fast."""

SLOW_PROBE = 0.2
"""For assertions that something does *not* happen within N: a larger N, so a
briefly stalled test thread on a loaded runner can't produce a false probe."""

NOT_FOUND = "Execution not found"

READ_TIMEOUT = 0.01

STORE = Store.for_download(format=StorageFormat.PARQUET)

RESULT_URI = "https://presigned.example.com/results.parquet"


class Session:
    """A fake SQL session transport.

    Records every request with its send time, and lets a test answer probes
    (``retrieve_results`` requests) through ``on_probe``. ``before_send`` runs
    before a request is recorded, so a test can stall a send.
    """

    def __init__(self, on_probe=None):
        self.incoming = queue.Queue()
        self.sent = []
        self.on_probe = on_probe
        self.before_send = None
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
        if self.before_send is not None:
            self.before_send(message)
        self.sent.append((time.monotonic(), message))
        if message["kind"] == "retrieve_results" and self.on_probe is not None:
            self.on_probe(self, message)

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

    def probe_times(self, execution_id=None):
        return [t for t, _ in self.requests("retrieve_results", execution_id)]


def wait_until(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def connect(session, **kwargs):
    kwargs.setdefault("stale_query_probe_seconds", PROBE)
    return Connection(session, read_timeout=READ_TIMEOUT, **kwargs)


def execute(conn, session, sql="SELECT 1", store=None):
    """Start a query that the session reports as running, then goes quiet."""
    cursor = conn.cursor()
    cursor.execute(sql, store=store)
    execution_id = session.requests("execute_sql")[-1][1]["execution_id"]
    session.put(kind="state_updated", execution_id=execution_id, state="running")
    return cursor, execution_id


def results_of(cursor):
    return cursor._Cursor__queue


def tracked(conn, execution_id):
    return conn._Connection__queries.get(execution_id)


def reply(session, execution_id, state="succeeded", **fields):
    session.put(
        kind="execution_result",
        execution_id=execution_id,
        state=state,
        results=None,
        **fields,
    )


def test_disabled_watchdog_never_probes():
    session = Session()
    conn = connect(session, stale_query_probe_seconds=None)
    try:
        cursor, execution_id = execute(conn, session, store=STORE)
        time.sleep(PROBE * 8)
        assert session.requests("retrieve_results") == []
        assert tracked(conn, execution_id) is not None
    finally:
        conn.close()


def test_steady_traffic_for_one_query_does_not_starve_anothers_watchdog():
    # read_timeout is well above the progress cadence, so recv() never times
    # out while B is streaming.
    session = Session()
    conn = Connection(session, read_timeout=0.5, stale_query_probe_seconds=SLOW_PROBE)
    stop = threading.Event()
    try:
        _, quiet = execute(conn, session)
        _, busy = execute(conn, session)

        def stream_progress():
            while not stop.is_set():
                session.put(kind="execution_progress", execution_id=busy)
                time.sleep(0.005)

        streamer = threading.Thread(target=stream_progress, daemon=True)
        streamer.start()
        assert wait_until(lambda: session.probe_times(quiet), timeout=2)
        # Progress events are activity: the busy query is never probed.
        assert session.probe_times(busy) == []
    finally:
        stop.set()
        conn.close()


def test_successive_probes_back_off_to_a_cap():
    session = Session(
        on_probe=lambda s, m: reply(s, m["execution_id"], state="running")
    )
    conn = connect(session)
    try:
        _, execution_id = execute(conn, session)
        registered = session.requests("execute_sql")[0][0]
        assert wait_until(lambda: len(session.probe_times()) >= 5, timeout=10)
        times = session.probe_times()[:5]
        assert times[0] - registered >= PROBE
        gaps = [later - earlier for earlier, later in zip(times, times[1:])]
        # N, then 2N, 4N, 8N, capped at 8N; never a tight loop. The upper
        # bound (next doubling) still tells each step, and the cap, apart.
        for gap, expected in zip(gaps, [2, 4, 8, 8]):
            assert PROBE * expected <= gap < PROBE * expected * 2, gaps
    finally:
        conn.close()


def test_probe_recovers_non_store_query_and_requests_configured_format():
    rows = b'[{"x": 1}, {"x": 2}]'

    def on_probe(s, m):
        s.incoming.put(
            cbor2.dumps(
                {
                    "kind": "execution_result",
                    "execution_id": m["execution_id"],
                    "state": "succeeded",
                    "results": {"result_bytes": rows, "format": "json"},
                }
            )
        )

    session = Session(on_probe=on_probe)
    conn = connect(session, results_format=ResultsFormat.JSON)
    try:
        cursor, execution_id = execute(conn, session)
        result = results_of(cursor).get(timeout=3)
        assert result.error is None
        assert result.results == [{"x": 1}, {"x": 2}]
        (_, probe), *_ = session.requests("retrieve_results")
        assert probe == {
            "kind": "retrieve_results",
            "execution_id": execution_id,
            "format": "json",
        }
    finally:
        conn.close()


def test_lost_retrieve_results_reply_is_re_requested():
    asked = []

    def on_probe(s, m):
        # Lose the reply to the driver's own request; answer the probe.
        asked.append(m)
        if len(asked) > 1:
            s.incoming.put(
                cbor2.dumps(
                    {
                        "kind": "execution_result",
                        "execution_id": m["execution_id"],
                        "state": "succeeded",
                        "results": {"result_bytes": b"[]", "format": "json"},
                    }
                )
            )

    session = Session(on_probe=on_probe)
    conn = connect(session)
    try:
        cursor, execution_id = execute(conn, session)
        session.put(kind="state_updated", execution_id=execution_id, state="succeeded")
        result = results_of(cursor).get(timeout=3)
        assert result.error is None
        assert result.results == []
        assert len(asked) == 2
    finally:
        conn.close()


@pytest.mark.parametrize("state", ["running", "pending"])
def test_non_terminal_probe_reply_keeps_waiting_and_probing(state):
    session = Session(on_probe=lambda s, m: reply(s, m["execution_id"], state=state))
    conn = connect(session)
    try:
        cursor, execution_id = execute(conn, session)
        assert wait_until(lambda: len(session.probe_times()) >= 2)
        query = tracked(conn, execution_id)
        assert query is not None
        assert query.state == ExecutionState[state.upper()]
        assert results_of(cursor).empty()
        # The probe replies aren't events of their own: backoff keeps growing.
        first, second = session.probe_times()[:2]
        assert second - first >= PROBE * 2
    finally:
        conn.close()


def test_genuine_events_postpone_probes_and_reset_backoff():
    session = Session(
        on_probe=lambda s, m: reply(s, m["execution_id"], state="running")
    )
    conn = connect(session, stale_query_probe_seconds=SLOW_PROBE)
    try:
        _, execution_id = execute(conn, session)
        assert wait_until(lambda: len(session.probe_times()) >= 2)
        query = tracked(conn, execution_id)
        assert wait_until(lambda: query.watch.probes_sent >= 2)
        # Progress is activity: no probes while it flows, and backoff resets.
        deadline = time.monotonic() + SLOW_PROBE * 4
        probes_before = len(session.probe_times())
        while time.monotonic() < deadline:
            session.put(kind="execution_progress", execution_id=execution_id)
            time.sleep(SLOW_PROBE / 5)
        assert wait_until(lambda: query.watch.probes_sent == 0, timeout=1)
        assert len(session.probe_times()) == probes_before
    finally:
        conn.close()


def test_probe_not_found_keeps_waiting_for_an_evicted_query(caplog):
    # The session's small LRU cache can evict a query that is still running.
    # Its terminal state_updated still arrives (from the future's callback),
    # but a probe gets "Execution not found". That must not fail the query.
    session = Session(
        on_probe=lambda s, m: s.put(
            kind="error", execution_id=m["execution_id"], message=NOT_FOUND
        )
    )
    conn = connect(session)
    try:
        cursor, execution_id = execute(conn, session, store=STORE)
        assert wait_until(lambda: "no longer tracks execution" in caplog.text)
        time.sleep(PROBE * 8)
        assert results_of(cursor).empty()
        assert tracked(conn, execution_id) is not None
        # Not probeable any more: no further probes.
        assert len(session.probe_times()) == 1
        session.put(
            kind="state_updated",
            execution_id=execution_id,
            state="succeeded",
            result_uri=RESULT_URI,
            size=3,
        )
        result = results_of(cursor).get(timeout=3)
        assert result.error is None
        assert result.store_result.result_uri == RESULT_URI
    finally:
        conn.close()


def test_progress_while_probe_outstanding_does_not_unmask_not_found(caplog):
    # A progress event between the probe and its reply must not make the
    # not-found reply look like an answer to a normal request.
    def on_probe(s, m):
        s.put(kind="execution_progress", execution_id=m["execution_id"])
        s.put(kind="error", execution_id=m["execution_id"], message=NOT_FOUND)

    session = Session(on_probe=on_probe)
    conn = connect(session)
    try:
        cursor, execution_id = execute(conn, session)
        assert wait_until(lambda: "no longer tracks execution" in caplog.text)
        assert results_of(cursor).empty()
        assert tracked(conn, execution_id) is not None
    finally:
        conn.close()


def test_not_found_for_the_normal_results_request_still_fails():
    # After the genuine state_updated: succeeded, there is no other terminal
    # event to wait for: a not-found reply fails the query, as without the
    # watchdog.
    session = Session(
        on_probe=lambda s, m: s.put(
            kind="error", execution_id=m["execution_id"], message=NOT_FOUND
        )
    )
    conn = connect(session)
    try:
        cursor, execution_id = execute(conn, session)
        session.put(kind="state_updated", execution_id=execution_id, state="succeeded")
        error = results_of(cursor).get(timeout=3).error
        assert isinstance(error, OperationalError)
        assert NOT_FOUND in str(error)
        assert tracked(conn, execution_id) is None
    finally:
        conn.close()


def test_not_found_after_results_requested_probe_still_fails():
    # The normal retrieve's reply is lost, and the one probe that follows it
    # gets not-found: the terminal event has already arrived, so fail.
    asked = []

    def on_probe(s, m):
        asked.append(m)
        if len(asked) > 1:
            s.put(kind="error", execution_id=m["execution_id"], message=NOT_FOUND)

    session = Session(on_probe=on_probe)
    conn = connect(session)
    try:
        cursor, execution_id = execute(conn, session)
        session.put(kind="state_updated", execution_id=execution_id, state="succeeded")
        error = results_of(cursor).get(timeout=3).error
        assert isinstance(error, OperationalError)
        assert NOT_FOUND in str(error)
        assert len(asked) == 2
    finally:
        conn.close()


def test_results_requested_gets_one_probe_no_earlier_than_4n():
    session = Session()
    conn = connect(session)
    try:
        _, execution_id = execute(conn, session)
        session.put(kind="state_updated", execution_id=execution_id, state="succeeded")
        assert wait_until(lambda: len(session.probe_times()) >= 2)
        requested, probed = session.probe_times()
        assert probed - requested >= PROBE * 4
        # Never a second probe: duplicate retrieves re-send the full result.
        time.sleep(PROBE * 16)
        assert len(session.probe_times()) == 2
    finally:
        conn.close()


def test_stalled_user_send_does_not_hold_up_other_results():
    session = Session()
    conn = connect(session)
    sending, release = threading.Event(), threading.Event()

    def stall_new_queries(message):
        if message["kind"] == "execute_sql" and message["statement"] == "SLOW":
            sending.set()
            release.wait(timeout=5)

    try:
        _, quiet = execute(conn, session)
        cursor, ready = execute(conn, session, sql="SELECT 2")
        session.before_send = stall_new_queries
        sender = threading.Thread(target=conn.cursor().execute, args=("SLOW",))
        sender.start()
        assert sending.wait(timeout=1)
        # Let a probe for the quiet query come due while the send is stalled.
        time.sleep(PROBE * 3)
        started = time.monotonic()
        session.put(
            kind="state_updated",
            execution_id=ready,
            state="cancelled",
        )
        result = results_of(cursor).get(timeout=3)
        delay = time.monotonic() - started
        release.set()
        sender.join(timeout=3)
        assert result.error is None
        assert delay < 1.0, delay
        # The skipped probe goes out once the send lock is free.
        assert wait_until(lambda: session.probe_times(quiet))
    finally:
        release.set()
        conn.close()


def test_recovery_is_logged_only_for_a_successful_probe_reply(caplog):
    caplog.set_level(logging.INFO)
    session = Session(
        on_probe=lambda s, m: s.incoming.put(
            cbor2.dumps(
                {
                    "kind": "execution_result",
                    "execution_id": m["execution_id"],
                    "state": "succeeded",
                    "results": {"result_bytes": b"not json", "format": "json"},
                }
            )
        )
    )
    conn = connect(session)
    try:
        cursor, execution_id = execute(conn, session)
        assert isinstance(results_of(cursor).get(timeout=3).error, OperationalError)
        assert "recovered" not in caplog.text
    finally:
        conn.close()


def test_watchdog_runs_when_read_timeout_is_none():
    # recv() with no timeout would block forever on a quiet connection, which
    # is exactly the lost-terminal-event shape.
    session = Session(
        on_probe=lambda s, m: reply(s, m["execution_id"], state="running")
    )
    conn = Connection(session, read_timeout=None, stale_query_probe_seconds=PROBE)
    try:
        _, execution_id = execute(conn, session)
        assert wait_until(lambda: session.probe_times(execution_id))
    finally:
        conn.close()


@pytest.mark.parametrize(
    "value",
    [0, -1, -0.5, float("nan"), float("inf"), float("-inf"), True, "30", object()],
)
def test_invalid_probe_interval_disables_watchdog(value, caplog):
    session = Session()
    conn = connect(session, stale_query_probe_seconds=value)
    try:
        assert "stale-query probes disabled" in caplog.text
        assert conn._Connection__probe_after is None
        _, execution_id = execute(conn, session)
        time.sleep(PROBE * 3)
        assert session.requests("retrieve_results") == []
        assert tracked(conn, execution_id) is not None
    finally:
        conn.close()


def test_connect_direct_forwards_probe_interval():
    session = Session()
    target = "wherobots.db.driver.websockets.sync.client.connect"
    with patch(target, return_value=session):
        conn = connect_direct("wss://compute/sql", stale_query_probe_seconds=PROBE)
    try:
        _, execution_id = execute(conn, session)
        assert wait_until(lambda: session.probe_times(execution_id))
    finally:
        conn.close()
