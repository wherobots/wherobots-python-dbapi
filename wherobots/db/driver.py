"""Wherobots DB driver.

A PEP-0249 compatible driver for interfacing with Wherobots DB.
"""

from contextlib import contextmanager
import logging
import requests
import websockets

from .constants import DEFAULT_ENDPOINT, DEFAULT_REGION, DEFAULT_RUNTIME
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

    resp = requests.post(
        url=f"https://{host}/sql/session",
        params={"runtime": runtime.value, "region": region.value},
        headers=headers,
    )
    resp.raise_for_status()

    ws_uri = resp.json().get("uri")
    if not ws_uri:
        raise errors.InterfaceError("Could not acquire SQL session")

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
        raise errors.NotSupportedError

    def rollback(self):
        raise errors.NotSupportedError

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
