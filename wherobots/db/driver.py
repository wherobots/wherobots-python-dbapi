"""Wherobots DB driver.

A PEP-0249 compatible driver for interfacing with Wherobots DB.
"""

from contextlib import contextmanager
import logging
import requests
import tenacity
import websockets

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
    def get_session_ws_uri():
        r = requests.get(session_id_url, headers=headers)
        r.raise_for_status()
        payload = r.json()
        status = payload.get("status")
        logging.debug("Polled %s; status: %s", session_id_url, status)
        if status in ("REQUESTED", "DEPLOYING", "DEPLOYED", "INITIALIZING"):
            raise tenacity.TryAgain("SQL Session is not ready yet")
        elif status == "READY":
            return payload["appMeta"]["url"]
        else:
            logging.error("SQL session creation failed: %s; should not retry.", status)
            raise OperationalError(f"Failed to create SQL session: {status}")

    try:
        ws_uri = get_session_ws_uri()
    except Exception as e:
        raise InterfaceError("Could not acquire SQL session", e)

    logging.info("Connecting to session at %s", ws_uri)
    session = Session(ws=websockets.connect(ws_uri))
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
        return Cursor(self)


class Cursor:

    def __init__(self, session):
        self.__session = session

        # Description and row count are set by the last executed operation.
        # Their default values are defined by PEP-0249.
        self.__description = None
        self.__rowcount = -1

        self.arraysize = 1

    @property
    def connection(self):
        return self.__session

    @property
    def description(self):
        return self.__description

    @property
    def rowcount(self):
        return self.__rowcount

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        raise NotImplementedError

    def executemany(self, operation, seq_of_parameters):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self):
        raise NotImplementedError

    def fetchmany(self, size=None):
        size = size or self.arraysize
        raise NotImplementedError

    def fetchall(self):
        raise NotImplementedError

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration
