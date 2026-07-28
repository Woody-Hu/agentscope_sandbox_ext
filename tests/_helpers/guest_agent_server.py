# -*- coding: utf-8 -*-
"""Real Unix-socket server speaking the Firecracker guest-agent wire
protocol.

This is **not** a mock.  It loads the exact same handler functions
that ship inside the microVM (the ``GUEST_AGENT_SOURCE`` string from
:mod:`agentscope_sandbox_ext._firecracker._guest_agent`) and serves
them over a Unix-domain socket instead of AF_VSOCK.  AF_VSOCK is only
available inside a real Firecracker VM, so tests run the handler logic
on a transport that CI *can* create.

Every byte that crosses the wire — the 4-byte length header, the
JSON body, the base64-encoded stdout/stderr — is identical to what a
real microVM would send.  The ``exec`` handler really does
``subprocess.run``, the ``read_file``/``write_file`` handlers really
do ``open()``.  This gives the :class:`GuestAgentClient` a true
end-to-end workout without a microVM.
"""

from __future__ import annotations

import socket
import threading
import types
from pathlib import Path

from agentscope_sandbox_ext._firecracker._guest_agent import (
    GUEST_AGENT_SOURCE,
)


def load_guest_agent_handlers() -> types.ModuleType:
    """Exec the ``GUEST_AGENT_SOURCE`` string into a fresh module.

    Returns a module object exposing ``_handle_exec``,
    ``_handle_read_file``, ``_handle_write_file``, ``_handle_ping``,
    ``_recv_frame``, ``_send_frame`` and ``_serve_connection`` — the
    real production handler code.
    """
    mod = types.ModuleType("guest_agent_under_test")
    mod.__file__ = "<GUEST_AGENT_SOURCE>"
    exec(compile(GUEST_AGENT_SOURCE, str(mod.__file__), "exec"), mod.__dict__)
    return mod


class GuestAgentUnixServer:
    """A real, threaded Unix-socket server running the guest-agent
    protocol handlers.

    The server accepts one connection at a time (matching the
    production agent's single-threaded behaviour) and dispatches each
    frame to the loaded handler functions.

    Usage::

        srv = GuestAgentUnixServer()
        srv.start()
        try:
            client = GuestAgentClient(connect=lambda: _unix_connect(srv.path))
            result = await client.exec_shell(["echo", "hi"])
        finally:
            srv.stop()
    """

    def __init__(self, *, max_conns: int = 16) -> None:
        self._handlers = load_guest_agent_handlers()
        # Create a Unix socket pair the server listens on.
        self._listen_sock = socket.socket(
            socket.AF_UNIX,
            socket.SOCK_STREAM,
        )
        self._path = (
            Path(__file__).resolve().parent
            / f"_ga_sock_{id(self)}.sock"
        )
        # Remove any stale socket file.
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass
        self._listen_sock.bind(str(self._path))
        self._listen_sock.listen(max_conns)
        self._listen_sock.settimeout(0.5)
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    @property
    def path(self) -> str:
        """Path of the listening Unix socket."""
        return str(self._path)

    def start(self) -> None:
        """Start the accept loop in a background thread."""
        self._thread = threading.Thread(
            target=self._accept_loop,
            daemon=True,
            name="guest-agent-unix-server",
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the accept loop to stop and close the socket."""
        self._stop.set()
        try:
            self._listen_sock.close()
        except OSError:
            pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass

    def _accept_loop(self) -> None:
        """Accept connections and dispatch to the real handlers."""
        while not self._stop.is_set():
            try:
                conn, _ = self._listen_sock.accept()
            except (socket.timeout, OSError):
                if self._stop.is_set():
                    return
                continue
            # Serve one connection synchronously, matching the
            # production agent's single-connection behaviour.
            try:
                self._handlers._serve_connection(conn)
            except Exception:
                # A handler bug or client disconnect — the connection
                # is already closed by ``_serve_connection``'s finally
                # block; just move on to the next client.
                pass

    def __enter__(self) -> "GuestAgentUnixServer":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
