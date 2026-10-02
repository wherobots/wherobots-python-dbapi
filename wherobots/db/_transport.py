"""Transport termination independent of the WebSocket protocol's send lock."""

import errno
import socket

from websockets.sync.client import ClientConnection


def abort_connection(ws: ClientConnection) -> None:
    """Disable further writes and wake blocked I/O before publishing failures.

    ClientConnection.close() takes the library's protocol mutex, which a
    stalled sendall() may hold. Shutdown the owned socket instead; the library's
    receive thread will observe EOF/error and finish protocol cleanup itself.
    Already transmitted bytes may still execute remotely.
    """
    try:
        ws.socket.shutdown(socket.SHUT_RDWR)
    except OSError as exc:
        if exc.errno not in (errno.ENOTCONN, errno.EBADF, errno.ECONNRESET):
            raise
    finally:
        # Even if shutdown fails, prevent any later send on this socket object.
        ws.socket.close()
