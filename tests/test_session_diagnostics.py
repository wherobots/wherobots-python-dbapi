"""Optional diagnostics are bounded and never delay a cursor's terminal result."""
import json
import threading
from unittest.mock import MagicMock, patch

import pytest
from websockets.exceptions import ConnectionClosedError

from wherobots.db import _diagnostics
from wherobots.db._diagnostics import SessionDiagnostics
from wherobots.db.connection import Connection
from wherobots.db.driver import connect_direct
from wherobots.db.errors import OperationalError
from test_disconnect import Transport


def response(payload, status=200):
    result = MagicMock(status_code=status)
    result.__enter__.return_value = result
    result.iter_content.return_value = [json.dumps(payload).encode()]
    return result


@pytest.fixture
def workers(monkeypatch):
    # Keep real concurrency; remove only jitter and record workers for cleanup.
    threads = []
    thread_type = threading.Thread

    def thread(*args, **kwargs):
        result = thread_type(*args, **kwargs)
        threads.append(result)
        return result

    monkeypatch.setattr(_diagnostics.threading, "Thread", thread)
    monkeypatch.setattr(_diagnostics.time, "sleep", lambda _: None)
    yield threads
    for worker in threads:
        worker.join(timeout=3)
        assert not worker.is_alive()


@pytest.mark.parametrize(
    "payload,expected",
    [
        (
            {"status": "FAILED", "firstFailure": {"message": "Evicted"}},
            ("FAILED", "Evicted"),
        ),
        ({"status": "READY"}, ("READY", None)),
        ({"firstFailure": {"message": "Evicted"}}, (None, "Evicted")),
        ({}, None),
        (None, None),
        ([], None),
        ({"status": 1, "firstFailure": {"message": 5}}, None),
        ({"firstFailure": "invalid"}, None),
    ],
)
def test_status_and_failure_parsing(payload, expected):
    with patch.object(
        _diagnostics.requests, "get", return_value=response(payload)
    ) as get:
        assert (
            SessionDiagnostics._fetch(
                "https://api/session/1", {"Authorization": "Bearer test"}
            )
            == expected
        )
    assert get.call_args.kwargs == {
        "headers": {"Authorization": "Bearer test"},
        "timeout": 1.0,
        "allow_redirects": False,
        "stream": True,
    }


def test_message_and_status_lengths_are_bounded():
    payload = {"status": "s" * 200, "firstFailure": {"message": "m" * 5000}}
    with patch.object(_diagnostics.requests, "get", return_value=response(payload)):
        status, message = SessionDiagnostics._fetch("https://api/session/1", {})
    assert status == "s" * 128
    assert message == "m" * 4096


def test_streaming_response_size_is_bounded():
    result = response({})
    chunks_consumed = []

    def chunks(chunk_size):
        for i in range(1000):
            chunks_consumed.append(i)
            yield b"x" * chunk_size

    result.iter_content.side_effect = chunks
    with patch.object(_diagnostics.requests, "get", return_value=result):
        assert SessionDiagnostics._fetch("https://api/session/1", {}) is None
    assert len(chunks_consumed) == 9
    result.__exit__.assert_called_once()


@pytest.mark.parametrize("status", [302, 404, 503])
def test_http_errors_are_logged_without_parsing(status, caplog):
    result = response({}, status)
    with caplog.at_level("DEBUG"), patch.object(
        _diagnostics.requests, "get", return_value=result
    ):
        assert SessionDiagnostics._fetch("https://api/session/1", {}) is None
    result.iter_content.assert_not_called()
    assert f"HTTP status: {status}" in caplog.text


def test_inflight_and_recent_requests_are_shared(workers, caplog):
    service = SessionDiagnostics()
    entered = threading.Event()
    release = threading.Event()

    def fetch(*args):
        entered.set()
        assert release.wait(timeout=3)
        return "FAILED", "Evicted"

    with caplog.at_level("WARNING"), patch.object(
        service, "_fetch", side_effect=fetch
    ) as get:
        try:
            service.request("https://api/session/1", {"Authorization": "secret"}, "1")
            assert entered.wait(timeout=1)
            for _ in range(50):
                service.request(
                    "https://api/session/1", {"Authorization": "secret"}, "1"
                )
            assert len(workers) == 1
            release.set()
            workers[0].join(timeout=2)
            service.request("https://api/session/1", {"Authorization": "secret"}, "1")
            get.assert_called_once()
        finally:
            release.set()
    assert "status='FAILED'" in caplog.text
    assert "firstFailure='Evicted'" in caplog.text
    assert "secret" not in caplog.text


def test_authentication_and_endpoint_contexts_are_not_shared(workers):
    service = SessionDiagnostics()
    with patch.object(service, "_fetch", return_value=None) as get:
        for url, token in [
            ("https://one/session/1", "a"),
            ("https://one/session/1", "b"),
            ("https://two/session/1", "a"),
        ]:
            service.request(url, {"Authorization": token}, "1")
            workers[-1].join(timeout=1)
    assert get.call_count == 3
    assert all(
        isinstance(key[1], bytes) and len(key[1]) == 32 for key in service._entries
    )


def test_worker_budget_bounds_stalled_lookups(workers):
    service = SessionDiagnostics()
    release = threading.Event()

    def fetch(*args):
        assert release.wait(timeout=3)
        return None

    with patch.object(service, "_fetch", side_effect=fetch):
        try:
            for i in range(100):
                service.request(f"https://api/session/{i}", {}, str(i))
            assert len(workers) == service._active == 4
            assert len(service._entries) == 4
        finally:
            release.set()
            for worker in workers:
                worker.join(timeout=2)
    assert service._active == 0


def test_cache_expires_and_has_a_fixed_size(workers, monkeypatch):
    service = SessionDiagnostics()
    monkeypatch.setattr(_diagnostics, "_MAX_ENTRIES", 2)
    with patch.object(service, "_fetch", return_value=None) as get:
        for i in range(3):
            service.request(f"https://api/session/{i}", {}, str(i))
            workers[-1].join(timeout=1)
        assert len(service._entries) == 2
        assert get.call_count == 3
        for key in service._entries:
            service._entries[key] = 0.0
        service.request("https://api/session/2", {}, "2")
        workers[-1].join(timeout=1)
        assert get.call_count == 4


def test_worker_exception_is_debug_logged_without_credentials(workers, caplog):
    service = SessionDiagnostics()
    with caplog.at_level("DEBUG"), patch.object(
        service, "_fetch", side_effect=ValueError("secret token")
    ):
        service.request("https://api/session/1", {}, "1")
        workers[0].join(timeout=1)
    assert "lookup failed (ValueError)" in caplog.text
    assert "secret token" not in caplog.text
    assert service._active == 0


def test_thread_start_failure_is_debug_logged_and_releases_slot(caplog):
    service = SessionDiagnostics()
    with caplog.at_level("DEBUG"), patch.object(
        threading.Thread, "start", side_effect=RuntimeError
    ):
        service.request("https://api/session/1", {}, "1")
    assert "Could not start session diagnostic thread" in caplog.text
    assert service._active == 0


@pytest.mark.parametrize("failure", ["send", "receive"])
def test_stalled_lookup_does_not_delay_failure_delivery(failure, workers):
    service = SessionDiagnostics()
    entered = threading.Event()
    release = threading.Event()

    def fetch(*args):
        entered.set()
        assert release.wait(timeout=3)
        return None

    ws = Transport()
    conn = Connection(
        ws, on_connection_lost=lambda: service.request("https://api/session/1", {}, "1")
    )
    with patch.object(service, "_fetch", side_effect=fetch):
        try:
            if failure == "send":
                ws.send = MagicMock(side_effect=ConnectionClosedError(None, None))
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            if failure == "receive":
                ws.incoming.put(ConnectionClosedError(None, None))
            assert isinstance(
                cursor._Cursor__queue.get(timeout=1).error, OperationalError
            )
            assert entered.wait(timeout=1)
            assert not release.is_set()  # Error preceded lookup completion.
        finally:
            release.set()
            conn.close()


def test_explicit_close_does_not_request_diagnostics():
    callback = MagicMock()
    conn = Connection(Transport(), on_connection_lost=callback)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    conn.close()
    callback.assert_not_called()


@pytest.mark.parametrize("suffix", ["", "/", "/?ignored=1"])
def test_direct_status_url_derivation_and_header_snapshot(suffix):
    headers = {"Authorization": "original"}
    url = "https://api/session/session-1" + suffix
    with patch(
        "wherobots.db.driver.websockets.sync.client.connect", return_value=Transport()
    ), patch.object(_diagnostics.session_diagnostics, "request") as request:
        conn = connect_direct(
            "wss://compute/sql", session_status_url=url, headers=headers
        )
        headers["Authorization"] = "changed"
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn._Connection__ws.incoming.put(ConnectionClosedError(None, None))
        conn._Connection__thread.join(timeout=2)
        request.assert_called_once_with(url, {"Authorization": "original"}, "session-1")
        with pytest.raises(OperationalError, match="session=session-1"):
            cursor.fetchall()
