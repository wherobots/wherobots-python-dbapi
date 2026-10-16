class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class ConnectionLostError(OperationalError):
    """The connection to the SQL session was lost while a query was pending.

    The query may still be running on the SQL session. To recover its outcome,
    open a new connection to the same session with ``Connection.reconnect()``
    and re-attach to the execution with ``Cursor.attach(execution_id)``.
    Raised by ``Cursor.execute()`` itself, rather than by a fetch, it means
    the connection was already lost and the statement was never sent.

    Attributes:
        session_id: The SQL session's ID, or None if the connection didn't
            know it.
        execution_id: The ID of the pending execution.
    """

    def __init__(
        self,
        message: str,
        session_id: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.session_id = session_id
        self.execution_id = execution_id


class ExecutionCancelledError(OperationalError):
    """An attached execution was cancelled, and not by this connection.

    The SQL session cancelled it before or while this connection re-attached
    to it, for example because its orphan grace period ran out, or another
    client cancelled it. Its outcome is unknown: for a write, whether it
    committed.

    Attributes:
        session_id: The SQL session's ID, or None if the connection didn't
            know it.
        execution_id: The ID of the execution.
    """

    def __init__(
        self,
        message: str,
        session_id: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.session_id = session_id
        self.execution_id = execution_id


class ExecutionNotFoundError(OperationalError):
    """The SQL session has no record of the execution to attach to.

    The execution ran on a different SQL session, the session restarted, or
    it no longer tracks the execution. Its outcome can't be recovered.

    Attributes:
        session_id: The SQL session's ID, or None if the connection didn't
            know it.
        execution_id: The ID of the execution.
    """

    def __init__(
        self,
        message: str,
        session_id: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.session_id = session_id
        self.execution_id = execution_id
