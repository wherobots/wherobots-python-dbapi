from wherobots.db.cursor import Cursor
from wherobots.db.errors import NotSupportedError


class Session:
    """
    A PEP-0249 compatible Session object for Wherobots DB.

    The session is backed by the WebSocket connection to the Wherobots SQL session.
    Transactions are not supported, so commit() and rollback() raise NotSupportedError.
    """

    def __init__(self, ws):
        self.__ws = ws

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.__ws.close()

    def commit(self):
        raise NotSupportedError

    def rollback(self):
        raise NotSupportedError

    def cursor(self) -> Cursor:
        return Cursor(self.__ws.send, self.__ws.recv)
