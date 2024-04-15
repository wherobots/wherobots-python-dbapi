"""Wherobots DB driver.

A PEP-0249 compatible driver for interfacing with Wherobots DB.
"""

import logging
import urllib.parse
import requests
import tenacity
import websockets.sync.client

from .constants import (
    DEFAULT_ENDPOINT,
    DEFAULT_REGION,
    DEFAULT_RUNTIME,
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    MAX_MESSAGE_SIZE,
)
from .errors import (
    InterfaceError,
    OperationalError,
)
from .region import Region
from .runtime import Runtime
from .connection import Connection

apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"


def connect(
    host: str = DEFAULT_ENDPOINT,
    token: str = None,
    api_key: str = None,
    runtime: Runtime = None,
    region: Region = None,
    wait_timeout_seconds: int = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
) -> Connection:
    if not token and not api_key:
        raise ValueError("At least one of `token` or `api_key` is required")
    if token and api_key:
        raise ValueError("`token` and `api_key` can't be both provided")

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif api_key:
        headers["X-API-Key"] = api_key

    runtime = runtime or DEFAULT_RUNTIME
    region = region or DEFAULT_REGION

    logging.info(
        "Requesting %s/%s runtime in %s from %s ...",
        runtime.name,
        runtime.value,
        region.value,
        host,
    )

    # Default to HTTPS if the hostname doesn't explicitly specify a scheme.
    if not host.startswith("http:"):
        host = f"https://{host}"

    try:
        resp = requests.post(
            url=f"{host}/sql/session",
            params={"region": region.value},
            json={"runtimeId": runtime.value},
            headers=headers,
        )
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise InterfaceError("Failed to create SQL session!", e)

    # At this point we've been redirected to /sql/session/{session_id}, which we'll need to keep polling until the
    # session is in READY state.
    session_id_url = resp.url

    @tenacity.retry(
        stop=tenacity.stop_after_delay(wait_timeout_seconds),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=5),
        retry=tenacity.retry_if_not_exception_type(
            (requests.HTTPError, OperationalError)
        ),
    )
    def get_session_uri() -> str:
        r = requests.get(session_id_url, headers=headers)
        r.raise_for_status()
        payload = r.json()
        status = payload.get("status")
        logging.info(" ... %s", status)
        if status in ("REQUESTED", "DEPLOYING", "DEPLOYED", "INITIALIZING"):
            raise tenacity.TryAgain("SQL Session is not ready yet")
        elif status == "READY":
            return payload["appMeta"]["url"]
        else:
            logging.error("SQL session creation failed: %s; should not retry.", status)
            raise OperationalError(f"Failed to create SQL session: {status}")

    try:
        logging.info("Getting SQL session status from %s ...", session_id_url)
        session_uri = get_session_uri()
    except Exception as e:
        raise InterfaceError("Could not acquire SQL session!", e)

    return connect_direct(http_to_ws(session_uri), headers)


def http_to_ws(uri: str) -> str:
    """Converts an HTTP URI to a WebSocket URI."""
    parsed = urllib.parse.urlparse(uri)
    for from_scheme, to_scheme in [("http", "ws"), ("https", "wss")]:
        if parsed.scheme == from_scheme:
            parsed = parsed._replace(scheme=to_scheme)
    return str(urllib.parse.urlunparse(parsed))


def connect_direct(uri: str, headers: dict[str, str] = None) -> Connection:
    logging.info("Connecting to SQL session at %s ...", uri)
    try:
        ws = websockets.sync.client.connect(
            uri=uri, additional_headers=headers, max_size=MAX_MESSAGE_SIZE
        )
        session = Connection(ws)
        return session
    except Exception as e:
        raise InterfaceError("Failed to connect to SQL session!") from e
