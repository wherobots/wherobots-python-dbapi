import os

import pytest
from typing import Union

from wherobots.db import connect, Connection, InterfaceError
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime


class TestConnection:
    """Lazily resolved connection suitable for use as a fixture

    This is to avoid connecting if at all possible, and for skipping tests
    such that `pytest tests/` can run without a live connection for
    tests that don't require it (without the test writer having to make up a
    skip message).
    """
    def __init__(self, api_key, **kwargs):
        self._api_key = api_key
        self._kwargs = kwargs
        self._connect_error: Union[str, None] = None
        self._conn: Union[Connection, None] = None

    def conn_or_skip(self) -> Connection:
        if self._conn is not None:
            return self._conn

        if self._connect_error is not None:
            pytest.skip(self._connect_error)

        if not self._api_key:
            pytest.skip("WHEROBOTS_API_KEY environment variable was not wet")

        try:
            self._conn = connect(api_key=self._api_key, **self._kwargs)
            return self._conn
        except InterfaceError as e:
            self._connect_error = f"{type(e).__name__}: {str(e)}"
            return self.conn_or_skip()


@pytest.fixture()
def wbc() -> TestConnection:
    if "WHEROBOTS_API_KEY" in os.environ:
        api_key = os.environ["WHEROBOTS_API_KEY"] or None
    else:
        api_key = None

    if "WHEROBOTS_HOST" in os.environ:
        host = os.environ["WHEROBOTS_HOST"] or None
    else:
        host = None

    if api_key is None:
        return None

    return TestConnection(
        api_key=api_key, host=host, region=Region.AWS_US_WEST_2, runtime=Runtime.TINY
    )
