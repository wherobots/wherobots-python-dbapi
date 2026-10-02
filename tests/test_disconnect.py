"""Transport loss must complete pending queries without waiting for a sender."""
import errno
import json
import queue
import socket
import ssl
import subprocess
import shutil
import threading
import time
from unittest.mock import MagicMock, patch

import pandas
import cbor2
import pytest
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.protocol import State
from websockets.sync.client import ClientConnection
from websockets.client import ClientProtocol
from websockets.uri import parse_uri

from wherobots.db._transport import abort_connection
from wherobots.db.connection import Connection, Query
from wherobots.db.driver import connect_direct
from wherobots.db.errors import OperationalError, ProgrammingError
from wherobots.db.types import ExecutionState


class Transport:
    def __init__(self):
        self.protocol = MagicMock(state=State.OPEN)
        self.incoming = queue.Queue()
        self.sent = []
        self.aborted = threading.Event()
        self.closed = threading.Event()
        self.close_calls = 0
        # Mirrors connect()'s DEFAULT_CLOSE_TIMEOUT_SECONDS; bounds the handshake.
        self.close_timeout = 1.0
        self.socket = MagicMock()
        self.socket.shutdown.side_effect = self.shutdown

    def recv(self, timeout):
        try:
            value = self.incoming.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError from None
        if isinstance(value, Exception):
            if not isinstance(value, TimeoutError):
                self.protocol.state = State.CLOSED
            raise value
        if isinstance(value, bytes):
            return value
        return json.dumps(value)

    def send(self, value):
        if self.aborted.is_set():
            raise ConnectionClosedError(None, None)
        self.sent.append(json.loads(value))

    def shutdown(self, how):
        assert how == socket.SHUT_RDWR
        self.aborted.set()
        self.incoming.put(ConnectionClosedOK(None, None))

    def close(self):
        # Graceful handshake: the library's close() completes and recv sees EOF.
        self.close_calls += 1
        self.closed.set()
        self.aborted.set()
        self.incoming.put(ConnectionClosedOK(None, None))


def deliver(ws, execution_id):
    ws.incoming.put(
        {
            "kind": "execution_result",
            "execution_id": execution_id,
            "state": "succeeded",
            "results": None,
        }
    )


@pytest.mark.parametrize(
    "error",
    [
        ConnectionClosedError(None, None),
        ConnectionClosedOK(None, None),
        OSError("transport lost"),
    ],
)
def test_disconnect_unblocks_all_cursors_and_rejects_new_queries(error):
    ws = Transport()
    conn = Connection(ws, session_id="session-1")
    cursors = [conn.cursor() for _ in range(3)]
    for cursor in cursors:
        cursor.execute("MERGE INTO secret VALUES ('private')")
    ws.incoming.put(error)
    conn._Connection__thread.join(timeout=3)
    assert not conn._Connection__thread.is_alive()
    for cursor, request in zip(cursors, ws.sent):
        with pytest.raises(OperationalError) as exc:
            cursor.fetchall()
        assert "session-1" in str(exc.value)
        assert request["execution_id"] in str(exc.value)
        assert "Commit outcome is unknown" in str(exc.value)
        assert "private" not in str(exc.value)
        with pytest.raises(OperationalError):
            cursor.fetchall()
        assert cursor._Cursor__queue.empty()
    assert not conn._Connection__queries
    with pytest.raises(OperationalError):
        conn.cursor().execute("INSERT INTO t VALUES (1)")
    assert len(ws.sent) == 3
    assert ws.aborted.is_set()


def test_idle_timeouts_then_result_leave_connection_usable():
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    ws.incoming.put(TimeoutError())
    ws.incoming.put(TimeoutError())
    deliver(ws, ws.sent[0]["execution_id"])
    assert cursor._Cursor__queue.get(timeout=3).error is None
    assert conn._Connection__thread.is_alive()
    assert not conn._Connection__closed
    conn.cursor().execute("SELECT 2")
    assert len(ws.sent) == 2
    conn.close()
    assert not conn._Connection__thread.is_alive()


def test_delivered_result_wins_close_and_is_not_overwritten():
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    deliver(ws, ws.sent[0]["execution_id"])
    ws.incoming.put(ConnectionClosedOK(None, None))
    conn._Connection__thread.join(timeout=3)
    assert cursor._Cursor__queue.get(timeout=1).error is None
    assert cursor._Cursor__queue.empty()


def test_close_fails_pending_and_joins_reader():
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    conn.close()
    with pytest.raises(OperationalError):
        cursor.fetchall()
    assert not conn._Connection__thread.is_alive()
    conn.close()
    assert ws.close_calls == 1
    ws.socket.shutdown.assert_not_called()


def test_idle_close_performs_close_handshake():
    ws = Transport()
    conn = Connection(ws)
    conn.close()
    assert ws.close_calls == 1
    ws.socket.shutdown.assert_not_called()
    assert not conn._Connection__thread.is_alive()
    assert not conn._Connection__send_lock.locked()


def test_close_with_stalled_send_falls_back_to_abort():
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    sending, timed_out = threading.Event(), threading.Event()
    original_send = ws.send

    def blocked_send(value):
        sending.set()
        # Only actual transport shutdown releases the writer. Record a timeout
        # rather than asserting here: __send re-raises, but only the test body
        # can report it.
        if not ws.aborted.wait(timeout=3):
            timed_out.set()
        original_send(value)

    ws.send = blocked_send
    sender = threading.Thread(target=cursor.execute, args=("SELECT 1",))
    sender.start()
    try:
        assert sending.wait(timeout=1)
        conn.close()
        ws.socket.shutdown.assert_called_once()
        assert ws.close_calls == 0
        assert isinstance(cursor._Cursor__queue.get(timeout=2).error, OperationalError)
    finally:
        conn.close()
        sender.join(timeout=3)
    assert not sender.is_alive()
    assert not timed_out.is_set()


@pytest.mark.parametrize(
    "error", [ConnectionClosedError(None, None), OSError("transport lost")]
)
def test_reader_failure_aborts_without_close_handshake(error):
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    ws.incoming.put(error)
    conn._Connection__thread.join(timeout=3)
    assert not conn._Connection__thread.is_alive()
    ws.socket.shutdown.assert_called_once()
    assert ws.close_calls == 0
    with pytest.raises(OperationalError):
        cursor.fetchall()


@pytest.mark.parametrize(
    "error", [ConnectionClosedError(None, None), OSError("send failed")]
)
def test_send_failure_does_not_leave_pending_query(error):
    ws = Transport()
    conn = Connection(ws)
    ws.send = MagicMock(side_effect=error)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(OperationalError):
        cursor.fetchall()
    assert not conn._Connection__queries
    conn.close()


def test_buffered_result_is_drained_even_when_transport_is_already_closed():
    ws = Transport()
    with patch("wherobots.db.connection.threading.Thread.start"):
        conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    deliver(ws, ws.sent[0]["execution_id"])
    ws.incoming.put(ConnectionClosedOK(None, None))
    ws.protocol.state = State.CLOSED
    conn._Connection__main_loop()
    assert cursor._Cursor__queue.get(timeout=1).error is None


@pytest.mark.parametrize(
    "decode_error",
    [
        ValueError("private payload"),
        OSError("private payload"),
        TimeoutError("private payload"),
        ConnectionClosedError(None, None),
    ],
)
def test_result_decode_error_completes_only_affected_query(decode_error, caplog):
    ws = Transport()
    conn = Connection(ws, session_id="session-1")
    bad = conn.cursor()
    good = conn.cursor()
    bad.execute("SELECT bad")
    good.execute("SELECT good")
    with patch.object(conn, "_handle_results", side_effect=decode_error):
        ws.incoming.put(
            {
                "kind": "execution_result",
                "execution_id": ws.sent[0]["execution_id"],
                "state": "succeeded",
                "results": {"ignored": True},
            }
        )
        deliver(ws, ws.sent[1]["execution_id"])
        assert good._Cursor__queue.get(timeout=3).error is None
        outcome = bad._Cursor__queue.get(timeout=3)
    assert isinstance(outcome.error, OperationalError)
    assert "Could not decode" in str(outcome.error)
    assert "session-1" in str(outcome.error)
    assert ws.sent[0]["execution_id"] in str(outcome.error)
    assert "private payload" not in str(outcome.error) + caplog.text
    assert "connection lost" not in str(outcome.error)
    assert not conn._Connection__queries
    bad._Cursor__queue.put(outcome)
    for fetch in (bad.fetchall, bad.fetchall, bad.get_store_result):
        with pytest.raises(OperationalError) as exc:
            fetch()
        assert exc.value is outcome.error
    assert not conn._Connection__closed
    good.execute("SELECT next")
    deliver(ws, ws.sent[-1]["execution_id"])
    assert good._Cursor__queue.get(timeout=3).error is None
    conn.close()
    assert bad._Cursor__queue.empty()


@pytest.mark.parametrize(
    "results",
    [
        {"format": "json", "result_bytes": b"private malformed JSON"},
        {"format": "arrow", "result_bytes": b"private malformed Arrow"},
        {"format": "unsupported", "result_bytes": b"private"},
        {"format": "arrow"},
        ["private malformed object"],
        "",
        False,
    ],
)
def test_malformed_result_payload_delivers_one_error(results, caplog):
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    message = cbor2.dumps(
        {
            "kind": "execution_result",
            "execution_id": ws.sent[0]["execution_id"],
            "state": "succeeded",
            "results": results,
        }
    )
    ws.incoming.put(message)
    outcome = cursor._Cursor__queue.get(timeout=3)
    assert isinstance(outcome.error, OperationalError)
    assert "Could not decode" in str(outcome.error)
    assert "private" not in str(outcome.error) + caplog.text
    assert not conn._Connection__queries
    # Duplicate server messages and later shutdown cannot deliver again.
    ws.incoming.put(message)
    assert not conn._Connection__closed
    conn.close()
    assert cursor._Cursor__queue.empty()


@pytest.mark.parametrize("state", [None, "unknown", 123, {}])
def test_invalid_state_completes_only_identified_query(state):
    ws = Transport()
    conn = Connection(ws)
    bad, good = conn.cursor(), conn.cursor()
    bad.execute("SELECT bad")
    good.execute("SELECT good")
    ws.incoming.put(
        {
            "kind": "execution_result",
            "execution_id": ws.sent[0]["execution_id"],
            "state": state,
        }
    )
    outcome = bad._Cursor__queue.get(timeout=3)
    assert isinstance(outcome.error, OperationalError)
    assert "Could not interpret" in str(outcome.error)
    assert ws.sent[0]["execution_id"] not in conn._Connection__queries
    deliver(ws, ws.sent[1]["execution_id"])
    assert good._Cursor__queue.get(timeout=3).error is None
    assert not conn._Connection__closed
    conn.close()


def test_local_retrieve_send_error_completes_only_affected_query():
    ws = Transport()
    conn = Connection(ws)
    bad, good = conn.cursor(), conn.cursor()
    bad.execute("SELECT bad")
    good.execute("SELECT good")
    with patch.object(ws, "send", side_effect=ValueError("private API error")):
        ws.incoming.put(
            {
                "kind": "state_updated",
                "execution_id": ws.sent[0]["execution_id"],
                "state": "succeeded",
            }
        )
        outcome = bad._Cursor__queue.get(timeout=3)
    assert isinstance(outcome.error, OperationalError)
    assert "Could not request" in str(outcome.error)
    assert "private" not in str(outcome.error)
    assert ws.sent[0]["execution_id"] not in conn._Connection__queries
    deliver(ws, ws.sent[1]["execution_id"])
    assert good._Cursor__queue.get(timeout=3).error is None
    assert not conn._Connection__closed
    conn.close()
    assert bad._Cursor__queue.empty()


def test_serialization_error_does_not_register_or_fail_other_queries():
    ws = Transport()
    conn = Connection(ws)
    good = conn.cursor()
    good.execute("SELECT 1")
    store = MagicMock()
    store.to_dict.return_value = {"invalid": object()}
    with pytest.raises(TypeError):
        conn.cursor().execute("SELECT 2", store=store)
    assert len(ws.sent) == len(conn._Connection__queries) == 1
    deliver(ws, ws.sent[0]["execution_id"])
    assert good._Cursor__queue.get(timeout=3).error is None
    assert not conn._Connection__closed
    conn.close()


def test_rejected_reexecution_does_not_leave_a_stale_execution_id():
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    deliver(ws, ws.sent[0]["execution_id"])
    # Consume the terminal outcome without depending on empty-result slicing.
    assert cursor._Cursor__get_results() is None
    store = MagicMock()
    store.to_dict.return_value = {"invalid": object()}
    with pytest.raises(TypeError):
        cursor.execute("SELECT 2", store=store)
    with pytest.raises(ProgrammingError, match="No query"):
        cursor.fetchall()
    conn.close()


def test_nontransport_send_error_is_propagated_and_query_is_untracked():
    ws = Transport()
    conn = Connection(ws)
    good = conn.cursor()
    good.execute("SELECT 1")
    with patch.object(ws, "send", side_effect=ValueError("API misuse")):
        with pytest.raises(ValueError, match="API misuse"):
            conn.cursor().execute("SELECT 2")
    assert len(conn._Connection__queries) == 1
    assert not conn._Connection__closed
    conn.close()


@pytest.mark.parametrize("shutdown", ["close", "reader"])
def test_stalled_send_does_not_block_result_delivery_or_shutdown(shutdown):
    ws = Transport()
    conn = Connection(ws)
    a = conn.cursor()
    b = conn.cursor()
    a.execute("SELECT 1")
    sending = threading.Event()
    original_send = ws.send

    def blocked_send(value):
        sending.set()
        # Only actual transport shutdown releases the writer, not the test.
        assert ws.aborted.wait(timeout=3)
        original_send(value)

    ws.send = blocked_send
    sender = threading.Thread(target=b.execute, args=("INSERT INTO t VALUES (1)",))
    sender.start()
    try:
        assert sending.wait(timeout=1)
        deliver(ws, ws.sent[0]["execution_id"])
        assert a._Cursor__queue.get(timeout=1).error is None
        if shutdown == "close":
            conn.close()
        else:
            ws.incoming.put(ConnectionClosedError(None, None))
        assert isinstance(b._Cursor__queue.get(timeout=2).error, OperationalError)
        sender.join(timeout=2)
        assert not sender.is_alive()
        assert len(ws.sent) == 1
        assert b._Cursor__queue.empty()
    finally:
        conn.close()
        sender.join(timeout=3)


def test_close_from_reader_callback_does_not_join_itself():
    ws = Transport()
    conn = Connection(ws)
    finished = threading.Event()

    def progress(_):
        conn.close()
        finished.set()

    conn.set_progress_handler(progress)
    ws.incoming.put({"kind": "execution_progress", "execution_id": "progress"})
    assert finished.wait(timeout=2)
    conn._Connection__thread.join(timeout=2)
    assert not conn._Connection__thread.is_alive()


@pytest.mark.parametrize("decode_fails", [False, True])
def test_close_racing_result_decode_delivers_only_one_terminal_outcome(decode_fails):
    decoding = threading.Event()
    release = threading.Event()
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")

    def decode(*args):
        decoding.set()
        assert release.wait(timeout=5)
        if decode_fails:
            raise ValueError("private payload")
        return pandas.DataFrame({"x": [1]})

    with patch.object(conn, "_handle_results", side_effect=decode):
        ws.incoming.put(
            {
                "kind": "execution_result",
                "execution_id": ws.sent[0]["execution_id"],
                "state": "succeeded",
                "results": {"ignored": True},
            }
        )
        assert decoding.wait(timeout=2)
        started = time.monotonic()
        conn.close()
        assert time.monotonic() - started < 2
        assert conn._Connection__thread.is_alive()
        with pytest.raises(OperationalError):
            cursor.fetchall()
        release.set()
        conn._Connection__thread.join(timeout=2)
    assert cursor._Cursor__queue.empty()


def test_concurrent_close_delivers_once():
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    closers = [threading.Thread(target=conn.close) for _ in range(4)]
    for closer in closers:
        closer.start()
    for closer in closers:
        closer.join(timeout=2)
        assert not closer.is_alive()
    assert isinstance(cursor._Cursor__queue.get(timeout=1).error, OperationalError)
    assert cursor._Cursor__queue.empty()
    assert ws.close_calls == 1
    ws.socket.shutdown.assert_not_called()


_MISSING = object()


class _NeverCompares(float):
    """A float subclass whose comparisons all report False."""

    def __lt__(self, other):
        return False

    __le__ = __gt__ = __ge__ = __lt__


def test_concurrent_close_waits_for_a_slow_handshake_to_deliver():
    # Closer A owns shutdown and is mid-handshake for longer than 1s, within
    # the transport's close_timeout. Closer B must not return before A has
    # failed the pending query.
    ws = Transport()
    ws.close_timeout = 1.5
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    handshaking = threading.Event()
    original_close = ws.close

    def slow_close():
        handshaking.set()
        time.sleep(1.3)
        original_close()

    ws.close = slow_close
    closer = threading.Thread(target=conn.close)
    closer.start()
    try:
        assert handshaking.wait(timeout=1)
        conn.close()
        delivered = not cursor._Cursor__queue.empty()
    finally:
        closer.join(timeout=3)
    assert delivered
    assert not closer.is_alive()
    assert isinstance(cursor._Cursor__queue.get(timeout=1).error, OperationalError)
    assert cursor._Cursor__queue.empty()
    assert ws.close_calls == 1


def test_slow_handshake_does_not_consume_reader_join():
    # The handshake uses most of close_timeout. A long read_timeout keeps the
    # reader parked in recv(), so only the end-of-stream delivered 0.3s after
    # the handshake wakes it; close() must still be waiting then.
    ws = Transport()
    conn = Connection(ws, read_timeout=10)

    def slow_close():
        time.sleep(0.9)
        ws.close_calls += 1
        ws.aborted.set()
        eof = ConnectionClosedOK(None, None)
        threading.Timer(0.3, ws.incoming.put, args=(eof,)).start()

    ws.close = slow_close
    conn.close()
    assert not conn._Connection__thread.is_alive()
    assert ws.close_calls == 1
    ws.socket.shutdown.assert_not_called()


def test_close_timeout_cleared_after_construction_aborts():
    # websockets reads close_timeout live, so the bound is resolved at close().
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    ws.close_timeout = None
    begun = time.monotonic()
    conn.close()
    assert time.monotonic() - begun < 1
    assert ws.close_calls == 0
    ws.socket.shutdown.assert_called_once()
    with pytest.raises(OperationalError):
        cursor.fetchall()


class _RaisingCloseTimeout(Transport):
    @property
    def close_timeout(self):
        raise RuntimeError("close_timeout unavailable")

    @close_timeout.setter
    def close_timeout(self, value):
        pass


def test_close_timeout_read_error_still_aborts_and_fails_pending():
    # The bound is read after the __closed latch is set; an error there must
    # not strand pending queries.
    ws = _RaisingCloseTimeout()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    conn.close()
    assert ws.close_calls == 0
    ws.socket.shutdown.assert_called_once()
    with pytest.raises(OperationalError):
        cursor.fetchall()
    assert conn._Connection__shutdown_done.is_set()
    assert not conn._Connection__thread.is_alive()


@pytest.mark.parametrize(
    "close_timeout",
    [
        None,
        "1",
        True,
        0,
        -1.0,
        float("inf"),
        float("nan"),
        pytest.param(10**400, id="huge-int"),
        pytest.param(1e300, id="huge-float"),
        pytest.param(_NeverCompares(1e300), id="float-subclass-lying-compare"),
        MagicMock(),
        _MISSING,
    ],
)
def test_unbounded_close_timeout_aborts_instead_of_handshaking(close_timeout):
    ws = Transport()
    if close_timeout is _MISSING:
        del ws.close_timeout
    else:
        ws.close_timeout = close_timeout
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    begun = time.monotonic()
    conn.close()
    assert time.monotonic() - begun < 1
    assert ws.close_calls == 0
    ws.socket.shutdown.assert_called_once()
    assert not conn._Connection__thread.is_alive()
    with pytest.raises(OperationalError):
        cursor.fetchall()


def test_abort_closes_socket_even_if_shutdown_errors():
    ws = MagicMock()
    ws.socket.shutdown.side_effect = OSError(errno.EIO, "shutdown failure")
    with pytest.raises(OSError):
        abort_connection(ws)
    ws.socket.close.assert_called_once()


def test_abort_failure_still_fails_pending_and_completes_shutdown():
    # A non-OSError from the adapter (e.g. a renamed attribute in a future
    # websockets release) must not skip delivery: __closed is a one-way latch.
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    ws.close = MagicMock(side_effect=AttributeError("no attribute 'close'"))
    ws.socket.shutdown.side_effect = AttributeError("no attribute 'socket'")
    conn.close()
    with pytest.raises(OperationalError):
        cursor.fetchall()
    assert not conn._Connection__queries
    assert conn._Connection__shutdown_done.is_set()
    assert conn._Connection__shutdown_owner is None
    with pytest.raises(OperationalError):
        conn.cursor().execute("SELECT 2")
    conn.close()


def _delayed(hook, started, release, timed_out):
    """Wrap a teardown hook so the test controls when it completes.

    Production code swallows exceptions from the transport, so an ``assert``
    inside the hook would be invisible; record a timeout instead and let the
    test body assert on it.
    """

    def delayed(*args):
        started.set()
        if not release.wait(timeout=3):
            timed_out.set()
        hook(*args)

    return delayed


def test_failure_is_not_delivered_before_graceful_close_completes():
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    started, release, timed_out = (threading.Event() for _ in range(3))
    ws.close = _delayed(ws.close, started, release, timed_out)
    closer = threading.Thread(target=conn.close)
    closer.start()
    try:
        assert started.wait(timeout=1)
        assert cursor._Cursor__queue.empty()
        # Senders fail fast on the latch; they don't wait for the handshake.
        begun = time.monotonic()
        with pytest.raises(OperationalError):
            conn.cursor().execute("SELECT 2")
        assert time.monotonic() - begun < 1
        assert cursor._Cursor__queue.empty()
        release.set()
        closer.join(timeout=2)
        assert not closer.is_alive()
        assert not timed_out.is_set()
        assert ws.close_calls == 1
        ws.socket.shutdown.assert_not_called()
        assert isinstance(cursor._Cursor__queue.get(timeout=1).error, OperationalError)
    finally:
        release.set()
        closer.join(timeout=3)


def test_failure_is_not_delivered_before_transport_is_aborted():
    ws = Transport()
    conn = Connection(ws)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    started, release, timed_out = (threading.Event() for _ in range(3))
    ws.socket.shutdown.side_effect = _delayed(ws.shutdown, started, release, timed_out)
    # Reader-side failure: the abort runs on the reader thread.
    ws.incoming.put(ConnectionClosedError(None, None))
    try:
        assert started.wait(timeout=1)
        assert cursor._Cursor__queue.empty()
        with pytest.raises(OperationalError):
            conn.cursor().execute("SELECT 2")
        assert cursor._Cursor__queue.empty()
        release.set()
        conn._Connection__thread.join(timeout=2)
        assert not conn._Connection__thread.is_alive()
        assert not timed_out.is_set()
        ws.socket.shutdown.assert_called_once()
        assert ws.close_calls == 0
        assert isinstance(cursor._Cursor__queue.get(timeout=1).error, OperationalError)
    finally:
        release.set()
        conn.close()


@pytest.mark.parametrize("tls", [False, True])
def test_real_websocket_stalled_send_is_interrupted_by_close(tls, tmp_path):
    # Real library protocol mutex + socket.sendall; the peer never reads.
    local, peer = socket.socketpair()
    local.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    if tls:
        openssl = shutil.which("openssl")
        if openssl is None:
            local.close()
            peer.close()
            pytest.skip("TLS fixture requires openssl")
        key, cert = tmp_path / "key.pem", tmp_path / "cert.pem"
        subprocess.run(
            [
                openssl,
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(cert),
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
            ],
            check=True,
            capture_output=True,
        )
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert, key)
        client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_context.check_hostname = False
        client_context.verify_mode = (
            ssl.CERT_NONE
        )  # Local generated test certificate only.
        peers = queue.Queue()
        server_socket = peer
        handshake = threading.Thread(
            target=lambda: peers.put(
                server_context.wrap_socket(server_socket, server_side=True)
            )
        )
        handshake.start()
        local = client_context.wrap_socket(local, server_hostname="localhost")
        peer = peers.get(timeout=3)
        handshake.join(timeout=3)
    protocol = ClientProtocol(parse_uri("ws://localhost"), state=State.OPEN)
    ws = ClientConnection(local, protocol)
    conn = Connection(ws)
    entered = threading.Event()
    send_data = ws.send_data

    def observe_send():
        entered.set()
        send_data()

    outcomes = queue.Queue()
    query = Query(
        "SELECT 1", "blocked", ExecutionState.EXECUTION_REQUESTED, outcomes.put
    )
    with patch.object(ws, "send_data", side_effect=observe_send):
        sender = threading.Thread(
            target=conn._Connection__send,
            args=(
                {
                    "kind": "execute_sql",
                    "execution_id": "blocked",
                    "statement": "x" * (8 * 1024 * 1024),
                },
                query,
            ),
        )
        sender.start()
        try:
            assert entered.wait(timeout=3)
            assert sender.is_alive()
            conn.close()
            assert isinstance(outcomes.get(timeout=2).error, OperationalError)
            sender.join(timeout=3)
            assert not sender.is_alive()
            assert not conn._Connection__thread.is_alive()
            ws.recv_events_thread.join(timeout=2)
            assert not ws.recv_events_thread.is_alive()
        finally:
            peer.close()
            conn.close()
            sender.join(timeout=3)


def test_direct_connection_uses_explicit_session_id():
    ws = Transport()
    with patch("wherobots.db.driver.websockets.sync.client.connect", return_value=ws):
        conn = connect_direct("wss://compute/sql", session_id="session-1")
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    conn.close()
    with pytest.raises(OperationalError, match="session=session-1"):
        cursor.fetchall()
