"""Transport termination independent of the WebSocket protocol's send lock."""

import errno
import select
import selectors
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


def socket_writable(ws: ClientConnection) -> bool:
    """Whether a small write to the transport's socket won't block right now.

    For a send that must not stall the reader thread: a full send buffer
    (a peer that keeps sending but stops reading) would park sendall(), which
    also holds the library's protocol mutex, so even keepalive pings stall.

    Never raises. Returns ``True`` (proceed, as without this check) on any
    failure or for a transport without a real socket, such as a test fake:
    ``False`` would silently disable the caller's sends. Uses ``poll`` (or a
    selector), not ``select.select``, which fails for fds above FD_SETSIZE.
    """
    try:
        sock = getattr(ws, "socket", None)
        if not isinstance(sock, socket.socket):
            return True
        fd = sock.fileno()
        if fd < 0:
            return True  # Closed: let the send itself report it.
        if hasattr(select, "poll"):
            poller = select.poll()
            poller.register(fd, select.POLLOUT)
            # POLLERR/POLLHUP count as writable: the send then fails fast.
            return bool(poller.poll(0))
        with selectors.DefaultSelector() as selector:
            selector.register(fd, selectors.EVENT_WRITE)
            return bool(selector.select(0))
    except Exception:
        return True
