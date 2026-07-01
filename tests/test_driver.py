"""Tests for the connect() and connect_direct() driver functions."""

import threading
import time
from importlib import metadata
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
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime

# The version DBAPI stamps into its X-Wherobots-Client hop, resolved the same
# way as gen_user_agent_header().
DBAPI_VERSION = metadata.version("wherobots-python-dbapi")


def _run_connect(mock_post, mock_get, **connect_kwargs):
    """Drive a successful connect() and return the kwargs passed to requests.post."""
    kwargs, _ = _run_connect_full(mock_post, mock_get, **connect_kwargs)
    return kwargs


def _run_connect_full(mock_post, mock_get, **connect_kwargs):
    """Drive a successful connect().

    Returns a tuple of (kwargs passed to requests.post, kwargs passed to the
    patched connect_direct) so tests can assert on both the HTTP POST and the
    WebSocket upgrade path.
    """
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

    with patch("wherobots.db.driver.connect_direct") as mock_cd:
        mock_cd.return_value = MagicMock()
        connect(api_key="test-key", **connect_kwargs)
        _, cd_kwargs = mock_cd.call_args

    _, post_kwargs = mock_post.call_args
    return post_kwargs, cd_kwargs


class TestConnectRegionRuntime:
    """region/runtime accept enum|str and are omitted when not provided."""

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_omitted_region_runtime_not_sent(self, mock_post, mock_get):
        """Omitting region/runtime sends no value so the API applies the org default."""
        kwargs = _run_connect(mock_post, mock_get)
        # `requests` drops query params that are None, so region is not sent.
        assert kwargs["params"]["region"] is None
        assert kwargs["json"]["runtimeId"] is None

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_enum_region_runtime_serialized(self, mock_post, mock_get):
        """Enum values serialize to their string form."""
        kwargs = _run_connect(
            mock_post, mock_get, region=Region.AWS_US_WEST_2, runtime=Runtime.TINY
        )
        assert kwargs["params"]["region"] == "aws-us-west-2"
        assert kwargs["json"]["runtimeId"] == "tiny"

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_string_region_runtime_passthrough(self, mock_post, mock_get):
        """Raw strings (e.g. BYOC regions) are passed through untouched."""
        kwargs = _run_connect(
            mock_post, mock_get, region="byoc-acme-us-east-1", runtime="x-large"
        )
        assert kwargs["params"]["region"] == "byoc-acme-us-east-1"
        assert kwargs["json"]["runtimeId"] == "x-large"


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


class TestWherobotsClientHeader:
    """connect() emits/appends the shared X-Wherobots-Client hop.

    The header is an ordered, append-only, comma-separated list of hops; each
    component appends its own `client=<token>;ver=<v>` hop on the right. DBAPI
    appends `client=dbapi;ver=<version>`.
    """

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_appends_dbapi_hop_to_inbound_chain(self, mock_post, mock_get):
        """An inbound X-Wherobots-Client chain gets the dbapi hop appended on the right."""
        inbound = "client=claude_web, client=mcp;ver=0.9"
        post_kwargs = _run_connect(
            mock_post,
            mock_get,
            extra_headers={"X-Wherobots-Client": inbound},
        )
        assert post_kwargs["headers"]["X-Wherobots-Client"] == (
            f"{inbound}, client=dbapi;ver={DBAPI_VERSION}"
        )

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_sets_dbapi_hop_when_no_inbound_chain(self, mock_post, mock_get):
        """With no inbound chain the header is exactly the dbapi hop."""
        post_kwargs = _run_connect(mock_post, mock_get)
        assert (
            post_kwargs["headers"]["X-Wherobots-Client"]
            == f"client=dbapi;ver={DBAPI_VERSION}"
        )

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_appends_case_insensitively(self, mock_post, mock_get):
        """A differently-cased inbound header is matched and collapsed to one canonical header."""
        inbound = "client=claude_web"
        post_kwargs = _run_connect(
            mock_post,
            mock_get,
            extra_headers={"x-wherobots-client": inbound},
        )
        headers = post_kwargs["headers"]
        # No differently-cased duplicate remains (would send two HTTP headers).
        client_keys = [k for k in headers if k.lower() == "x-wherobots-client"]
        assert client_keys == ["X-Wherobots-Client"]
        assert headers["X-Wherobots-Client"] == (
            f"{inbound}, client=dbapi;ver={DBAPI_VERSION}"
        )

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_forwards_arbitrary_extra_headers(self, mock_post, mock_get):
        """Arbitrary extra_headers keys are merged into the request headers."""
        post_kwargs = _run_connect(
            mock_post,
            mock_get,
            extra_headers={"X-Trace-Id": "abc-123", "X-Custom": "yes"},
        )
        assert post_kwargs["headers"]["X-Trace-Id"] == "abc-123"
        assert post_kwargs["headers"]["X-Custom"] == "yes"

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_extra_headers_do_not_override_auth(self, mock_post, mock_get):
        """The advisory header must never clobber the auth header."""
        post_kwargs = _run_connect(
            mock_post,
            mock_get,
            extra_headers={"X-Wherobots-Client": "client=claude_web"},
        )
        # api_key auth is set by _run_connect; extra_headers must not remove it.
        assert post_kwargs["headers"]["X-API-Key"] == "test-key"

    @patch("wherobots.db.driver.requests.get")
    @patch("wherobots.db.driver.requests.post")
    def test_ws_upgrade_carries_appended_header(self, mock_post, mock_get):
        """The appended X-Wherobots-Client also flows to the WebSocket upgrade path."""
        inbound = "client=claude_web, client=mcp;ver=0.9"
        _post_kwargs, cd_kwargs = _run_connect_full(
            mock_post,
            mock_get,
            extra_headers={"X-Wherobots-Client": inbound},
        )
        assert cd_kwargs["headers"]["X-Wherobots-Client"] == (
            f"{inbound}, client=dbapi;ver={DBAPI_VERSION}"
        )

    @patch("wherobots.db.driver.websockets.sync.client.connect")
    def test_connect_direct_forwards_headers_to_ws(self, mock_ws):
        """connect_direct forwards headers verbatim to the websocket upgrade."""
        headers = {"X-Wherobots-Client": f"client=dbapi;ver={DBAPI_VERSION}"}
        mock_ws.return_value = MagicMock()
        with patch("wherobots.db.driver.Connection") as mock_conn:
            mock_conn.return_value = MagicMock()
            connect_direct(
                uri="wss://compute.example.com/sql/org/session-id",
                headers=headers,
            )
        _, ws_kwargs = mock_ws.call_args
        assert ws_kwargs["additional_headers"]["X-Wherobots-Client"] == (
            f"client=dbapi;ver={DBAPI_VERSION}"
        )
