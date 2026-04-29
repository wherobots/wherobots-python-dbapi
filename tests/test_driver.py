"""Tests for the connect() and connect_direct() driver functions."""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from wherobots.db.driver import (
    DEFAULT_HTTP_TIMEOUT,
    _check_cancelled,
    connect,
    connect_direct,
)
from wherobots.db.errors import InterfaceError


class TestCheckCancelled:
    def test_none_event_is_noop(self):
        _check_cancelled(None)

    def test_unset_event_is_noop(self):
        event = threading.Event()
        _check_cancelled(event)

    def test_set_event_raises(self):
        event = threading.Event()
        event.set()
        with pytest.raises(InterfaceError, match="cancelled by caller"):
            _check_cancelled(event)


class TestConnectCancelEvent:
    @patch("wherobots.db.driver.requests.post")
    def test_cancel_before_post(self, mock_post):
        """cancel_event set before connect() should raise immediately without making HTTP calls."""
        cancel = threading.Event()
        cancel.set()

        with pytest.raises(InterfaceError, match="cancelled by caller"):
            connect(api_key="test-key", cancel_event=cancel)

        mock_post.assert_not_called()

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_cancel_during_polling(self, mock_post, mock_get):
        """cancel_event set during session polling should abort the retry loop."""
        # POST succeeds with redirect
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.url = "https://api.example.com/sql/session/test-id"
        post_resp.raise_for_status = MagicMock()
        mock_post.return_value = post_resp

        # GET returns INITIALIZING (triggers TryAgain)
        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.raise_for_status = MagicMock()
        get_resp.json.return_value = {"status": "INITIALIZING"}
        mock_get.return_value = get_resp

        cancel = threading.Event()

        # Set cancel after a short delay (during polling)
        def set_cancel():
            time.sleep(0.1)
            cancel.set()

        t = threading.Thread(target=set_cancel)
        t.start()

        with pytest.raises(InterfaceError, match="cancelled by caller"):
            connect(api_key="test-key", cancel_event=cancel, wait_timeout=10)

        t.join()

    @patch("wherobots.db.driver.requests.post")
    def test_http_timeout_on_post(self, mock_post):
        """requests.post should be called with a timeout."""
        post_resp = MagicMock()
        post_resp.status_code = 401
        post_resp.raise_for_status.side_effect = requests.HTTPError(response=post_resp)
        post_resp.json.side_effect = requests.JSONDecodeError("", "", 0)
        mock_post.return_value = post_resp

        with pytest.raises(InterfaceError, match="Failed to create SQL session"):
            connect(api_key="test-key")

        _, kwargs = mock_post.call_args
        assert kwargs["timeout"] == DEFAULT_HTTP_TIMEOUT

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_http_timeout_on_get(self, mock_post, mock_get):
        """requests.get in the polling loop should be called with a timeout."""
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.url = "https://api.example.com/sql/session/test-id"
        post_resp.raise_for_status = MagicMock()
        mock_post.return_value = post_resp

        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.raise_for_status = MagicMock()
        get_resp.json.return_value = {
            "status": "READY",
            "appMeta": {"url": "https://compute.example.com/sql/org/session-id"},
        }
        mock_get.return_value = get_resp

        # Patch connect_direct to avoid actual WebSocket connection
        with patch("wherobots.db.driver.connect_direct") as mock_cd:
            mock_cd.return_value = MagicMock()
            connect(api_key="test-key")

        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == DEFAULT_HTTP_TIMEOUT

    @patch("wherobots.db.driver.requests.post")
    def test_connect_without_cancel_event(self, mock_post):
        """connect() without cancel_event should work as before (backward compat)."""
        post_resp = MagicMock()
        post_resp.status_code = 401
        post_resp.raise_for_status.side_effect = requests.HTTPError(response=post_resp)
        post_resp.json.side_effect = requests.JSONDecodeError("", "", 0)
        mock_post.return_value = post_resp

        with pytest.raises(InterfaceError):
            connect(api_key="test-key")


class TestConnectDirectCancelEvent:
    @patch("wherobots.db.driver.websockets.sync.client.connect")
    def test_cancel_before_ws_connect(self, mock_ws):
        cancel = threading.Event()
        cancel.set()

        with pytest.raises(InterfaceError, match="cancelled by caller"):
            connect_direct(
                uri="wss://compute.example.com/sql/org/session-id",
                cancel_event=cancel,
            )

        mock_ws.assert_not_called()
