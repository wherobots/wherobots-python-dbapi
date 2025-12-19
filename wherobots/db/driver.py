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
from typing import Final, Union, Dict
import urllib.parse
import websockets.sync.client
import certifi

from .connection import Connection
from .constants import (
    DEFAULT_ENDPOINT,
    DEFAULT_REGION,
    DEFAULT_RUNTIME,
    DEFAULT_READ_TIMEOUT_SECONDS,
    DEFAULT_SESSION_TYPE,
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    MAX_MESSAGE_SIZE,
    PARAM_STYLE,
    PROTOCOL_VERSION,
    AppStatus,
    DataCompression,
    GeometryRepresentation,
    ResultsFormat,
    SessionType,
)
from .errors import (
    InterfaceError,
    OperationalError,
)
from .region import Region
from .runtime import Runtime

apilevel = "2.0"
threadsafety = 1
paramstyle: Final[str] = PARAM_STYLE


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
    runtime: Union[Runtime, None] = None,
    region: Union[Region, None] = None,
    version: Union[str, None] = None,
    wait_timeout: float = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    read_timeout: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session_type: Union[SessionType, None] = None,
    force_new: bool = False,
    shutdown_after_inactive_seconds: Union[int, None] = None,
    results_format: Union[ResultsFormat, None] = None,
    data_compression: Union[DataCompression, None] = None,
    geometry_representation: Union[GeometryRepresentation, None] = None,
) -> Connection:
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
    runtime = runtime or DEFAULT_RUNTIME
    region = region or DEFAULT_REGION
    session_type = session_type or DEFAULT_SESSION_TYPE

    logging.info(
        "Requesting %s%s runtime %sin %s from %s ...",
        "new " if force_new else "",
        runtime.value,
        f"running {version} " if version else "",
        region.value,
        host,
    )

    # Default to HTTPS if the hostname doesn't explicitly specify a scheme.
    if not host.startswith("http:"):
        host = f"https://{host}"

    try:
        resp = requests.post(
            url=f"{host}/sql/session",
            params={"region": region.value, "force_new": force_new},
            json={
                "runtimeId": runtime.value,
                "shutdownAfterInactiveSeconds": shutdown_after_inactive_seconds,
                "version": version,
                "sessionType": session_type.value,
            },
            headers=headers,
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
        retry=tenacity.retry_if_not_exception_type(
            (requests.HTTPError, OperationalError)
        ),
    )
    def get_session_uri() -> str:
        r = requests.get(session_id_url, headers=headers)
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
    except Exception as e:
        raise InterfaceError("Could not acquire SQL session!", e)

    return connect_direct(
        uri=http_to_ws(session_uri),
        headers=headers,
        read_timeout=read_timeout,
        results_format=results_format,
        data_compression=data_compression,
        geometry_representation=geometry_representation,
    )


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
) -> Connection:
    uri_with_protocol = f"{uri}/{protocol}"

    try:
        logging.info("Connecting to SQL session at %s ...", uri_with_protocol)
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(certifi.where())
        ws = websockets.sync.client.connect(
            uri=uri_with_protocol,
            additional_headers=headers,
            max_size=MAX_MESSAGE_SIZE,
            ssl=ssl_context,
        )
    except Exception as e:
        raise InterfaceError("Failed to connect to SQL session!") from e

    return Connection(
        ws,
        read_timeout=read_timeout,
        results_format=results_format,
        data_compression=data_compression,
        geometry_representation=geometry_representation,
    )
