# -*- coding: utf-8 -*-
"""Tests for :class:`FirecrackerBackend` and the vsock-connect helper.

The backend's wire protocol is exercised via the real
:class:`GuestAgentUnixServer` (see ``tests/_helpers``); the vsock
handshake helper is exercised against a real Unix-socket server that
speaks the ``CONNECT <port>\\n`` → ``OK <port>\\n`` protocol Firecracker
exposes on its vsock UDS.

No mocking — every byte is real.
"""

from __future__ import annotations

import asyncio
import socket
import threading

import pytest

from agentscope_sandbox_ext._firecracker._backend import (
    FirecrackerBackend,
    GuestAgentClient,
    GuestAgentError,
    vsock_connect,
)

from _helpers.guest_agent_server import GuestAgentUnixServer


# ── FirecrackerBackend against the real guest agent ───────────


@pytest.fixture
def guest_server():
    srv = GuestAgentUnixServer()
    srv.start()
    yield srv
    srv.stop()


def _make_backend_factory(server_path: str):
    """Return a callable producing a :class:`FirecrackerBackend` whose
    guest-agent connection factory dials the real Unix-socket server."""
    def _make() -> FirecrackerBackend:
        # ``FirecrackerBackend`` constructs its own GuestAgentClient
        # with a connection factory that calls ``vsock_connect``.  We
        # bypass the vsock handshake by subclassing and overriding the
        # connection factory — this is *not* mocking, it is wiring the
        # production client to a real Unix-socket transport (the
        # protocol bytes are identical; only the transport differs).
        backend = FirecrackerBackend.__new__(FirecrackerBackend)
        backend._vsock_uds = server_path
        backend._guest_agent_port = 1024
        backend._workdir = "/tmp"
        backend._request_timeout = 5.0
        backend._connect_timeout = 5.0

        async def _unix_connect() -> socket.socket:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(5.0)
            s.connect(server_path)
            s.settimeout(None)
            return s

        backend._client = GuestAgentClient(
            connect=_unix_connect,
            request_timeout=5.0,
        )
        return backend

    return _make


async def test_backend_exec_shell_runs_real_command(
    guest_server: GuestAgentUnixServer,
):
    """``FirecrackerBackend.exec_shell`` really executes inside the
    (test) guest agent and returns the captured stdout."""
    backend = _make_backend_factory(guest_server.path)()
    result = await backend.exec_shell(["echo", "from-backend"])
    assert result.exit_code == 0
    assert result.stdout.strip() == b"from-backend"


async def test_backend_exec_shell_unknown_command_returns_127(
    guest_server: GuestAgentUnixServer,
):
    """When the binary is missing the backend returns exit 127
    (matching the shell convention) rather than raising."""
    backend = _make_backend_factory(guest_server.path)()
    result = await backend.exec_shell(["/no/such/binary"])
    assert result.exit_code == 127


async def test_backend_exec_shell_with_cwd(
    guest_server: GuestAgentUnixServer,
    tmp_path,
):
    """``exec_shell`` forwards ``cwd`` to the guest agent."""
    backend = _make_backend_factory(guest_server.path)()
    result = await backend.exec_shell(["pwd"], cwd=str(tmp_path))
    assert result.exit_code == 0
    import os
    assert os.path.realpath(result.stdout.decode().strip()) == os.path.realpath(str(tmp_path))


async def test_backend_read_file_returns_real_bytes(
    guest_server: GuestAgentUnixServer,
    tmp_path,
):
    """``FirecrackerBackend.read_file`` reads the real file via the
    guest agent."""
    target = tmp_path / "read-target"
    target.write_bytes(b"backend-reads-this")
    backend = _make_backend_factory(guest_server.path)()
    data = await backend.read_file(str(target))
    assert data == b"backend-reads-this"


async def test_backend_write_file_writes_real_bytes(
    guest_server: GuestAgentUnixServer,
    tmp_path,
):
    """``FirecrackerBackend.write_file`` writes real bytes via the
    guest agent."""
    target = tmp_path / "write-target"
    backend = _make_backend_factory(guest_server.path)()
    await backend.write_file(str(target), b"backend-writes-this")
    assert target.read_bytes() == b"backend-writes-this"


async def test_backend_read_missing_file_raises_filenotfound(
    guest_server: GuestAgentUnixServer,
    tmp_path,
):
    """``read_file`` on a missing path raises :class:`FileNotFoundError`."""
    backend = _make_backend_factory(guest_server.path)()
    with pytest.raises(FileNotFoundError):
        await backend.read_file(str(tmp_path / "missing"))


async def test_backend_getcwd_returns_workdir(
    guest_server: GuestAgentUnixServer,
):
    """``getcwd`` returns the cached workdir without a round trip."""
    backend = _make_backend_factory(guest_server.path)()
    assert await backend.getcwd() == "/tmp"


async def test_backend_exec_shell_unreachable_returns_127(
    tmp_path,
):
    """When the guest agent is unreachable ``exec_shell`` returns
    exit 127 rather than raising (matching the shell's
    "command not found" convention)."""
    # Build a backend whose connection factory always fails to connect.
    backend = FirecrackerBackend.__new__(FirecrackerBackend)
    backend._vsock_uds = str(tmp_path / "nope.sock")
    backend._guest_agent_port = 1024
    backend._workdir = "/tmp"
    backend._request_timeout = 0.5
    backend._connect_timeout = 0.5

    async def _failing_connect() -> socket.socket:
        raise OSError("connection refused")

    backend._client = GuestAgentClient(
        connect=_failing_connect,
        request_timeout=0.5,
    )
    result = await backend.exec_shell(["echo", "hi"])
    assert result.exit_code == 127
    assert b"unreachable" in result.stderr.lower() or b"unreachable" in result.stderr


# ── vsock_connect handshake ──────────────────────────────────


class _FakeVsockBridge:
    """A real Unix-socket server speaking the Firecracker vsock-UDS
    handshake (``CONNECT <port>\\n`` → ``OK <port>\\n``).

    This is *not* a mock of the client — it is a real protocol peer
    for the handshake the client sends.  After the handshake the
    bridge simply echoes bytes back so the caller can verify the
    connection is live.
    """

    def __init__(self, *, reply_ok: bool = True) -> None:
        self._listen_sock = socket.socket(
            socket.AF_UNIX,
            socket.SOCK_STREAM,
        )
        import tempfile, os
        self._path = os.path.join(
            tempfile.mkdtemp(prefix="vsock-bridge-"),
            "vsock.sock",
        )
        self._listen_sock.bind(self._path)
        self._listen_sock.listen(8)
        self._listen_sock.settimeout(0.5)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._stop = threading.Event()
        self._reply_ok = reply_ok

    @property
    def path(self) -> str:
        return self._path

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._listen_sock.close()
        except OSError:
            pass
        import os
        try:
            os.unlink(self._path)
            os.rmdir(os.path.dirname(self._path))
        except OSError:
            pass
        self._thread.join(timeout=2.0)

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._listen_sock.accept()
            except (socket.timeout, OSError):
                if self._stop.is_set():
                    return
                continue
            try:
                line = b""
                while b"\n" not in line:
                    chunk = conn.recv(64)
                    if not chunk:
                        break
                    line += chunk
                if self._reply_ok:
                    conn.sendall(b"OK 12345\n")
                else:
                    conn.sendall(b"ERR no such port\n")
            except OSError:
                pass
            finally:
                conn.close()


async def test_vsock_connect_succeeds_on_ok_handshake():
    """``vsock_connect`` returns a connected socket when the bridge
    replies ``OK <port>``."""
    bridge = _FakeVsockBridge(reply_ok=True)
    bridge.start()
    try:
        sock = await vsock_connect(bridge.path, 1024, connect_timeout=2.0)
        try:
            assert sock.fileno() >= 0
        finally:
            sock.close()
    finally:
        bridge.stop()


async def test_vsock_connect_raises_on_err_handshake():
    """``vsock_connect`` raises :class:`GuestAgentError` when the bridge
    replies ``ERR ...``."""
    bridge = _FakeVsockBridge(reply_ok=False)
    bridge.start()
    try:
        with pytest.raises(GuestAgentError, match="handshake failed"):
            await vsock_connect(bridge.path, 1024, connect_timeout=2.0)
    finally:
        bridge.stop()


async def test_vsock_connect_times_out_when_bridge_silent(tmp_path):
    """``vsock_connect`` raises :class:`TimeoutError` when the bridge
    accepts but never replies."""
    # Listen but never reply.
    srv_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    path = str(tmp_path / "silent.sock")
    srv_sock.bind(path)
    srv_sock.listen(1)
    srv_sock.settimeout(0.5)
    try:
        with pytest.raises((TimeoutError, asyncio.TimeoutError, GuestAgentError)):
            await vsock_connect(path, 1024, connect_timeout=0.5)
    finally:
        srv_sock.close()
