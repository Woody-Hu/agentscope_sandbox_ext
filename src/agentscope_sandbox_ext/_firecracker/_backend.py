# -*- coding: utf-8 -*-
"""Firecracker microVM :class:`BackendBase` implementation.

The backend talks to a small in-VM guest agent over virtio-vsock
(bridged through Firecracker's host-side Unix-domain socket).  The
agent serves a length-prefixed JSON protocol implementing the three
:class:`BackendBase` primitives — ``exec_shell``, ``read_file``,
``write_file`` — so the agentscope builtin tools (Bash, Read, Write,
Edit, Grep, Glob) operate inside a Firecracker microVM transparently.

The wire protocol is split out into :class:`GuestAgentClient` so it
can be exercised against a real protocol peer (the bundled
:mod:`._guest_agent` source run as a subprocess on the host) without
any mocking — see ``tests/test_firecracker_backend.py``.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import socket
import struct
from typing import Any

from agentscope.tool._builtin._backend import BackendBase, ExecResult

from .._firecracker._constants import (
    DEFAULT_GUEST_AGENT_PORT,
    DEFAULT_SHUTDOWN_TIMEOUT,
)


# ── wire protocol ────────────────────────────────────────────────


class GuestAgentError(RuntimeError):
    """Raised when the in-VM guest agent returns an error response."""


class GuestAgentClient:
    """Length-prefixed JSON protocol client for the in-VM guest agent.

    The client is given a *connection factory* — a callable that
    returns a fresh connected socket-like object — rather than a fixed
    socket so each protocol round can open a fresh connection (the
    bundled agent is single-connection) and so the same client code
    can be tested against any socket-transport (Unix-domain, TCP, the
    Firecracker vsock bridge, ...).

    Args:
        connect (`Callable[[], Awaitable[socket.socket]]`):
            Async callable returning a connected socket.  Production
            wires this to :meth:`_vsock_connect` below; tests can wire
            it to a Unix-domain socket pair served by a real protocol
            peer (the bundled guest agent source run on the host).
        request_timeout (`float`, defaults to ``30.0``):
            Per-request timeout (covers connect + request round trip).
    """

    def __init__(
        self,
        connect,
        *,
        request_timeout: float = 30.0,
    ) -> None:
        """Bind factory and timeout."""
        self._connect = connect
        self._request_timeout = request_timeout

    # ── public API ───────────────────────────────────────────────

    async def ping(self) -> bool:
        """Health-check probe; returns ``True`` iff the agent replied."""
        try:
            resp = await self._request({"op": "ping"})
        except (OSError, asyncio.TimeoutError, GuestAgentError):
            return False
        return bool(resp.get("ok"))

    async def exec_shell(
        self,
        argv: list[str],
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        """Run ``argv`` inside the VM via the guest agent."""
        req: dict[str, Any] = {"op": "exec", "argv": list(argv)}
        if cwd is not None:
            req["cwd"] = cwd
        if timeout is not None:
            req["timeout"] = timeout
        resp = await self._request(req)
        if not resp.get("ok"):
            raise GuestAgentError(str(resp.get("error", "unknown")))
        return ExecResult(
            exit_code=int(resp.get("exit_code", -1)),
            stdout=_b64decode(resp.get("stdout", "")),
            stderr=_b64decode(resp.get("stderr", "")),
        )

    async def read_file(self, path: str) -> bytes:
        """Read a file's raw bytes from inside the VM."""
        resp = await self._request({"op": "read_file", "path": path})
        if not resp.get("ok"):
            err = str(resp.get("error", "unknown"))
            if "FileNotFound" in err:
                raise FileNotFoundError(f"not found in VM: {path}")
            raise GuestAgentError(err)
        return _b64decode(resp.get("data", ""))

    async def write_file(self, path: str, data: bytes) -> None:
        """Write raw bytes to a file inside the VM."""
        resp = await self._request(
            {
                "op": "write_file",
                "path": path,
                "data": base64.b64encode(data).decode("ascii"),
            },
        )
        if not resp.get("ok"):
            raise GuestAgentError(str(resp.get("error", "unknown")))

    # ── internals ────────────────────────────────────────────────

    async def _request(self, req: dict[str, Any]) -> dict[str, Any]:
        """Send one request frame and read one response frame."""
        sock: socket.socket | None = None
        try:
            sock = await asyncio.wait_for(
                self._connect(),
                timeout=self._request_timeout,
            )
            sock.settimeout(None)
            # httpx-style framing: 4-byte big-endian length + JSON body.
            body = json.dumps(req).encode("utf-8")
            await asyncio.get_event_loop().run_in_executor(
                None,
                sock.sendall,
                struct.pack(">I", len(body)) + body,
            )
            return await asyncio.wait_for(
                self._recv_frame(sock),
                timeout=self._request_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise asyncio.TimeoutError(
                f"guest agent timed out after {self._request_timeout}s "
                f"for op={req.get('op')!r}",
            ) from exc
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    async def _recv_frame(self, sock: socket.socket) -> dict[str, Any]:
        """Read one length-prefixed JSON frame from ``sock``."""
        loop = asyncio.get_event_loop()

        def _read() -> dict[str, Any]:
            header = _recv_exactly_sync(sock, 4)
            (length,) = struct.unpack(">I", header)
            if length <= 0 or length > 64 * 1024 * 1024:
                raise ValueError(f"frame length out of range: {length}")
            body = _recv_exactly_sync(sock, length)
            return json.loads(body.decode("utf-8"))

        return await loop.run_in_executor(None, _read)


def _recv_exactly_sync(sock: socket.socket, n: int) -> bytes:
    """Read exactly ``n`` bytes from a sync socket or raise EOFError."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed mid-frame")
        buf.extend(chunk)
    return bytes(buf)


def _b64decode(s: str | bytes) -> bytes:
    """Tolerant base64 decode that returns ``b""`` for empty input."""
    if not s:
        return b""
    if isinstance(s, bytes):
        return base64.b64decode(s)
    return base64.b64decode(s.encode("ascii"))


# ── vsock connection helper ──────────────────────────────────────


async def vsock_connect(
    vsock_uds: str,
    guest_port: int,
    *,
    connect_timeout: float = 5.0,
) -> socket.socket:
    """Open a vsock connection to the guest agent via Firecracker's UDS.

    Firecracker exposes a Unix-domain socket at ``vsock_uds`` that
    bridges host connections to guest AF_VSOCK ports.  The handshake
    is line-based: send ``CONNECT <port>\\n``, read ``OK <host_port>\\n``.

    Args:
        vsock_uds (`str`):
            Path to Firecracker's vsock UDS (the ``uds_path`` passed
            to ``PUT /vsock``).
        guest_port (`int`):
            Port inside the guest the agent is listening on.
        connect_timeout (`float`, defaults to ``5.0``):
            Timeout for the connect + handshake.

    Returns:
        `socket.socket`:
            A connected Unix-domain socket bridged to the guest's
            vsock port.

    Raises:
        TimeoutError: If the connect or handshake does not complete
            in time.
        OSError: On any underlying socket error.
    """
    loop = asyncio.get_event_loop()

    def _do_connect() -> socket.socket:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(connect_timeout)
        sock.connect(vsock_uds)
        sock.sendall(f"CONNECT {guest_port}\n".encode("ascii"))
        # Read up to the first newline — the reply is ``OK <port>\n``
        # or ``ERR <reason>\n``.
        buf = bytearray()
        while b"\n" not in buf:
            chunk = sock.recv(64)
            if not chunk:
                raise EOFError("vsock bridge closed during handshake")
            buf.extend(chunk)
        line, _, _ = bytes(buf).partition(b"\n")
        line_s = line.decode("ascii", errors="replace").strip()
        if not line_s.startswith("OK"):
            sock.close()
            raise GuestAgentError(f"vsock handshake failed: {line_s}")
        # Reset to blocking mode — caller sets its own timeout.
        sock.settimeout(None)
        return sock

    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _do_connect),
            timeout=connect_timeout,
        )
    except asyncio.TimeoutError as exc:
        raise TimeoutError(
            f"vsock connect to {vsock_uds}:{guest_port} timed out",
        ) from exc


# ── the backend ─────────────────────────────────────────────────


class FirecrackerBackend(BackendBase):
    """:class:`BackendBase` backed by a Firecracker microVM.

    Only the three abstract primitives (``exec_shell``, ``read_file``,
    ``write_file``) are implemented here; the derived filesystem
    helpers (``file_exists``, ``is_dir``, ``list_dir``, ``stat_mtime``,
    ``delete_path``) are inherited from :class:`BackendBase` and work
    out of the box for any POSIX-like guest.

    Args:
        vsock_uds (`str`):
            Host-side path of Firecracker's vsock UDS (the
            ``uds_path`` passed to ``PUT /vsock``).
        guest_agent_port (`int`, defaults to :data:`DEFAULT_GUEST_AGENT_PORT`):
            Port inside the guest the agent is listening on.
        workdir (`str`):
            Default working directory for ``exec_shell`` calls inside
            the VM.
        request_timeout (`float`, defaults to ``30.0``):
            Per-primitive timeout.
        connect_timeout (`float`, defaults to ``5.0``):
            Per-call vsock connect timeout.
    """

    def __init__(
        self,
        vsock_uds: str,
        *,
        guest_agent_port: int = DEFAULT_GUEST_AGENT_PORT,
        workdir: str = "/",
        request_timeout: float = 30.0,
        connect_timeout: float = 5.0,
    ) -> None:
        """Bind vsock UDS path, port, workdir and timeouts."""
        self._vsock_uds = vsock_uds
        self._guest_agent_port = guest_agent_port
        self._workdir = workdir
        self._request_timeout = request_timeout
        self._connect_timeout = connect_timeout
        self._client = GuestAgentClient(
            connect=self._make_connection,
            request_timeout=request_timeout,
        )

    # ── exec ─────────────────────────────────────────────────────

    async def getcwd(self) -> str:
        """Return the VM's default working directory.

        Overrides the base class default (which would shell out to
        ``pwd``) with the cached ``workdir`` supplied at construction,
        avoiding a per-call vsock round trip.
        """
        return self._workdir

    async def exec_shell(
        self,
        command: list[str],
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        """Run ``command`` inside the VM via the guest agent.

        *command* is an argv list passed directly to the guest agent's
        ``subprocess.run`` — no intervening shell.  Callers needing
        shell features wrap with ``["sh", "-c", line]``.

        Args:
            command (`list[str]`):
                Executable path/name followed by its arguments.
            cwd (`str | None`, optional):
                Working directory inside the VM.  When ``None`` the
                backend's default ``workdir`` is used.
            timeout (`float | None`, optional):
                Maximum number of seconds to wait.  When ``None`` the
                call waits indefinitely.  On timeout the result carries
                an ``exit_code`` of ``-1``.

        Returns:
            `ExecResult`:
                The captured exit code, stdout, and stderr.  When the
                guest agent itself is unreachable the result carries
                an ``exit_code`` of ``127`` (mirroring a shell's
                "command not found") so callers see a normal non-zero
                result rather than an exception.
        """
        effective_cwd = cwd or self._workdir
        try:
            return await self._client.exec_shell(
                list(command),
                cwd=effective_cwd,
                timeout=timeout,
            )
        except (OSError, asyncio.TimeoutError, GuestAgentError) as exc:
            return ExecResult(
                exit_code=127,
                stdout=b"",
                stderr=(
                    f"Firecracker guest agent unreachable: {exc!r}"
                ).encode("utf-8"),
            )

    # ── file I/O ─────────────────────────────────────────────────

    async def read_file(self, path: str) -> bytes:
        """Read a file from inside the VM via the guest agent.

        Args:
            path (`str`):
                Path to the file inside the VM.

        Returns:
            `bytes`:
                The raw file contents.

        Raises:
            `FileNotFoundError`:
                If the path does not exist inside the VM.
            `OSError`:
                On any other read failure.
        """
        return await self._client.read_file(path)

    async def write_file(self, path: str, data: bytes) -> None:
        """Write *data* to a file inside the VM via the guest agent.

        Args:
            path (`str`):
                Destination path inside the VM.
            data (`bytes`):
                The raw bytes to write.
        """
        await self._client.write_file(path, data)

    # ── internals ────────────────────────────────────────────────

    async def _make_connection(self) -> socket.socket:
        """Open a fresh vsock-bridged UDS connection to the guest agent."""
        return await vsock_connect(
            self._vsock_uds,
            self._guest_agent_port,
            connect_timeout=self._connect_timeout,
        )

    @property
    def request_timeout(self) -> float:
        """Per-primitive timeout in seconds (read-only)."""
        return self._request_timeout

    @property
    def connect_timeout(self) -> float:
        """Per-call vsock connect timeout in seconds (read-only)."""
        return self._connect_timeout

    @property
    def shutdown_timeout(self) -> float:
        """Default shutdown timeout for a Firecracker teardown (read-only).

        Convenience alias for :data:`DEFAULT_SHUTDOWN_TIMEOUT` so
        callers wiring teardown logic don't need to import the
        constants module directly.
        """
        return DEFAULT_SHUTDOWN_TIMEOUT
