"""Wherobots DB driver.

A PEP-0249 compatible driver for interfacing with Wherobots DB.
"""

import ssl
from importlib import metadata
from importlib.metadata import PackageNotFoundError
import logging
from packaging.version import Version
import platform
import requests
import tenacity
import threading
from typing import Final, Union, Dict
import urllib.parse
import websockets.exceptions
import websockets.sync.client
import certifi

from .connection import Connection
from .constants import (
    DEFAULT_ENDPOINT,
    DEFAULT_READ_TIMEOUT_SECONDS,
    DEFAULT_SESSION_TYPE,
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    MAX_MESSAGE_SIZE,
    PARAM_STYLE,
    PROTOCOL_VERSION,
)
from .errors import (
    InterfaceError,
    OperationalError,
)
from .region import Region
from .runtime import Runtime
from .session_type import SessionType
from .types import (
    AppStatus,
    DataCompression,
    GeometryRepresentation,
    ResultsFormat,
)

apilevel = "2.0"
threadsafety = 1
paramstyle: Final[str] = PARAM_STYLE

# HTTP status codes that indicate transient server-side issues and should be retried.
# This follows the industry-standard set used by urllib3.util.Retry's status_forcelist.
TRANSIENT_HTTP_STATUS_CODES = {429, 502, 503, 504}

# Default timeout for individual HTTP requests (connect + read), in seconds.
DEFAULT_HTTP_TIMEOUT = 30


def gen_user_agent_header():
    try:
        package_version = metadata.version("wherobots-python-dbapi")
    except PackageNotFoundError:
        package_version = "unknown"
    python_version = platform.python_version()
    system = platform.system().lower()
    return {
        "User-Agent": f"wherobots-python-dbapi/{package_version} os/{system} python/{python_version}"
    }


def connect(
    host: str = DEFAULT_ENDPOINT,
    token: Union[str, None] = None,
    api_key: Union[str, None] = None,
    runtime: Union[str, Runtime, None] = None,
    region: Union[str, Region, None] = None,
    version: Union[str, None] = None,
    wait_timeout: float = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    read_timeout: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session_type: Union[SessionType, None] = None,
    force_new: bool = False,
    shutdown_after_inactive_seconds: Union[int, None] = None,
    results_format: Union[ResultsFormat, None] = None,
    data_compression: Union[DataCompression, None] = None,
    geometry_representation: Union[GeometryRepresentation, None] = None,
    cancel_event: Union[threading.Event, None] = None,
) -> Connection:
    """Create a connection to a Wherobots SQL session.

    :param runtime: The compute runtime to use. Accepts a ``Runtime`` enum value
        or a raw string; strings are passed to the API as-is. Override the
        default runtime set for your organization — only set this if you need a
        specific runtime instead of the one your administrator has configured.
        When omitted, your organization's default runtime is used.
    :param region: The compute region to run in. Accepts a ``Region`` enum value
        or a raw string (e.g. a BYOC region such as ``byoc-acme-us-east-1``);
        strings are passed to the API as-is. Override the default region set for
        your organization — only set this if you intend to use a specific region
        instead of the one your administrator has configured. When omitted, your
        organization's default region is used.
    """
    if not token and not api_key:
        raise ValueError("At least one of `token` or `api_key` is required")
    if token and api_key:
        raise ValueError("`token` and `api_key` can't be both provided")

    headers = gen_user_agent_header()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif api_key:
        headers["X-API-Key"] = api_key

    host = host or DEFAULT_ENDPOINT
    session_type = session_type or DEFAULT_SESSION_TYPE

    # Normalize enum values to their string form and pass raw strings through
    # untouched. When omitted (None) the field is dropped from the request so
    # the API applies the organization's configured default.
    runtime_id = runtime.value if isinstance(runtime, Runtime) else runtime
    region_name = region.value if isinstance(region, Region) else region

    logging.info(
        "Requesting %s%s runtime %sin %s from %s ...",
        "new " if force_new else "",
        runtime_id or "org-default",
        f"running {version} " if version else "",
        region_name or "org-default",
        host,
    )

    # Default to HTTPS if the hostname doesn't explicitly specify a scheme.
    if not host.startswith("http:"):
        host = f"https://{host}"

    _check_cancelled(cancel_event)

    try:
        resp = requests.post(
            url=f"{host}/sql/session",
            # `requests` omits query params whose value is None, so an omitted
            # region is simply not sent and the API applies the org default.
            params={"region": region_name, "force_new": force_new},
            json={
                "runtimeId": runtime_id,
                "shutdownAfterInactiveSeconds": shutdown_after_inactive_seconds,
                "version": version,
                "sessionType": session_type.value,
            },
            headers=headers,
            timeout=DEFAULT_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.HTTPError as e:
        details = str(e)
        try:
            info = e.response.json()
            errors = info.get("errors", [])
            if errors and isinstance(errors, list):
                details = f"{errors[0]['message']}: {errors[0]['details']}"
        except requests.JSONDecodeError:
            pass
        raise InterfaceError(f"Failed to create SQL session: {details}") from e

    # At this point we've been redirected to /sql/session/{session_id}, which we'll need to keep polling until the
    # session is in READY state.
    session_id_url = resp.url

    @tenacity.retry(
        stop=tenacity.stop_after_delay(wait_timeout),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=5),
        retry=(
            tenacity.retry_if_exception(
                lambda e: (
                    isinstance(e, requests.HTTPError)
                    and e.response.status_code in TRANSIENT_HTTP_STATUS_CODES
                )
            )
            | tenacity.retry_if_exception_type(tenacity.TryAgain)
        ),
        before_sleep=lambda _: _check_cancelled(cancel_event),
        reraise=True,
    )
    def get_session_uri() -> str:
        _check_cancelled(cancel_event)
        r = requests.get(session_id_url, headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
        r.raise_for_status()
        payload = r.json()
        status = AppStatus(payload.get("status"))
        logging.info(" ... %s", status)
        if status.is_starting():
            raise tenacity.TryAgain("SQL Session is not ready yet")
        elif status == AppStatus.READY:
            return payload["appMeta"]["url"]
        else:
            logging.error("SQL session creation failed: %s; should not retry.", status)
            raise OperationalError(f"Failed to create SQL session: {status}")

    try:
        logging.info("Getting SQL session status from %s ...", session_id_url)
        session_uri = get_session_uri()
        logging.debug("SQL session URI from app status: %s", session_uri)
    except InterfaceError:
        raise
    except Exception as e:
        raise InterfaceError("Could not acquire SQL session!", e)

    return connect_direct(
        uri=http_to_ws(session_uri),
        headers=headers,
        read_timeout=read_timeout,
        results_format=results_format,
        data_compression=data_compression,
        geometry_representation=geometry_representation,
        cancel_event=cancel_event,
    )


def _check_cancelled(cancel_event: Union[threading.Event, None]) -> None:
    """Raise InterfaceError if the cancel event is set."""
    if cancel_event is not None and cancel_event.is_set():
        raise InterfaceError("Connection cancelled by caller")


def http_to_ws(uri: str) -> str:
    """Converts an HTTP URI to a WebSocket URI."""
    parsed = urllib.parse.urlparse(uri)
    for from_scheme, to_scheme in [("http", "ws"), ("https", "wss")]:
        if parsed.scheme == from_scheme:
            parsed = parsed._replace(scheme=to_scheme)
    return str(urllib.parse.urlunparse(parsed))


def connect_direct(
    uri: str,
    protocol: Version = PROTOCOL_VERSION,
    headers: Union[Dict[str, str], None] = None,
    read_timeout: float = DEFAULT_READ_TIMEOUT_SECONDS,
    results_format: Union[ResultsFormat, None] = None,
    data_compression: Union[DataCompression, None] = None,
    geometry_representation: Union[GeometryRepresentation, None] = None,
    cancel_event: Union[threading.Event, None] = None,
) -> Connection:
    uri_with_protocol = f"{uri}/{protocol}"
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(certifi.where())

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=5),
        retry=tenacity.retry_if_exception_type(
            (
                ConnectionRefusedError,
                ConnectionResetError,
                TimeoutError,
                websockets.exceptions.InvalidHandshake,
            )
        ),
        before_sleep=lambda _: _check_cancelled(cancel_event),
        reraise=True,
    )
    def ws_connect() -> websockets.sync.client.ClientConnection:
        _check_cancelled(cancel_event)
        logging.info("Connecting to SQL session at %s ...", uri_with_protocol)
        return websockets.sync.client.connect(
            uri=uri_with_protocol,
            additional_headers=headers,
            max_size=MAX_MESSAGE_SIZE,
            open_timeout=DEFAULT_HTTP_TIMEOUT,
            ssl=ssl_context,
        )

    try:
        ws = ws_connect()
    except InterfaceError:
        raise
    except Exception as e:
        raise InterfaceError("Failed to connect to SQL session!") from e

    return Connection(
        ws,
        read_timeout=read_timeout,
        results_format=results_format,
        data_compression=data_compression,
        geometry_representation=geometry_representation,
    )
