"""Wherobots DB driver.

A PEP-0249 compatible driver for interfacing with Wherobots DB.
"""

import json
import logging
import urllib.parse
import uuid
from contextlib import contextmanager
from typing import Any

import requests
import tenacity
import websockets.sync.client

from .constants import (
    DEFAULT_ENDPOINT,
    DEFAULT_REGION,
    DEFAULT_RUNTIME,
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
)
from .errors import (
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from .region import Region
from .runtime import Runtime

apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"


@contextmanager
def connect(
    host: str = DEFAULT_ENDPOINT,
    token: str = None,
    api_key: str = None,
    runtime: Runtime = DEFAULT_RUNTIME,
    region: Region = DEFAULT_REGION,
    wait_timeout_seconds: int = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
):
    if not token and not api_key:
        raise ValueError("At least one of `token` or `api_key` is required")
    if token and api_key:
        raise ValueError("`token` and `api_key` can't be both provided")

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif api_key:
        headers["X-API-Key"] = api_key

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

    resp = requests.post(
        url=f"{host}/sql/session",
        params={"region": region.value},
        json={"runtimeId": runtime.value},
        headers=headers,
    )
    resp.raise_for_status()

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
    def get_session_uri():
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

    yield from connect_direct(http_to_ws(session_uri), headers)


def http_to_ws(uri: str) -> str:
    parsed = urllib.parse.urlparse(uri)
    for from_scheme, to_scheme in [("http", "ws"), ("https", "wss")]:
        if parsed.scheme == from_scheme:
            parsed = parsed._replace(scheme=to_scheme)
    return str(urllib.parse.urlunparse(parsed))


@contextmanager
def connect_direct(
    uri: str,
    headers: dict[str, str] = None,
):
    logging.info("Connecting to SQL session at %s ...", uri)
    connection = websockets.sync.client.connect(uri=uri, additional_headers=headers)
    session = Session(ws=connection)
    try:
        yield session
    finally:
        session.close()


class Session:

    def __init__(self, ws):
        self.__ws = ws

    def close(self):
        self.__ws.close()

    def commit(self):
        raise NotSupportedError

    def rollback(self):
        raise NotSupportedError

    def cursor(self):
        return Cursor(self.__ws.send, self.__ws.recv)


class Cursor:

    def __init__(self, send_func, recv_func):
        self.__send_func = send_func
        self.__recv_func = recv_func

        self.__current_execution_id: str | None = None
        self.__current_execution_state: str | None = None

        # Description and row count are set by the last executed operation.
        # Their default values are defined by PEP-0249.
        self.__description: str | None = None
        self.__rowcount: int = -1

        # Array-size is also defined by PEP-0249 and is expected to be read/writable.
        self.arraysize: int = 1

    @property
    def description(self) -> str | None:
        return self.__description

    @property
    def rowcount(self) -> int:
        return self.__rowcount

    def close(self):
        pass

    def __send_request(self, request):
        self.__send_func(json.dumps(request))

    def execute(self, operation: str, parameters: dict[str, Any] = None):
        self.__current_execution_id = str(uuid.uuid4())
        self.__current_execution_state = "requested"

        sql = operation.format(**(parameters or {}))
        logging.info("Executing SQL: %s", sql)
        exec_request = {
            "kind": "execute_sql",
            "execution_id": self.__current_execution_id,
            "statement": sql,
        }
        self.__send_request(exec_request)

    def executemany(self, operation: str, seq_of_parameters: list[dict[str, Any]]):
        raise NotImplementedError

    def fetchone(self):
        if not self.__current_execution_id:
            raise ProgrammingError("No query has been executed yet")
        raise NotImplementedError

    def fetchmany(self, size: int = None):
        size = size or self.arraysize
        # TODO: evaluate if optimizations are possible here based on results encoding.
        rows = []
        while len(rows) < size:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self):
        result = []
        while True:
            size = self.arraysize
            rows = self.fetchmany(size)
            result.extend(rows)
            if len(rows) < size:
                break
        return result

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration
