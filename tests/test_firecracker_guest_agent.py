# -*- coding: utf-8 -*-
"""End-to-end tests for the Firecracker guest-agent wire protocol.

These tests run the **real** guest-agent handler code (the
``GUEST_AGENT_SOURCE`` string that ships into the microVM) on a
Unix-domain socket and drive it with the real
:class:`GuestAgentClient`.  No method is stubbed or replaced —
``exec`` really spawns subprocesses, ``read_file`` really opens files,
``write_file`` really writes them, and the wire framing is the
production framing.
"""

from __future__ import annotations

import asyncio
import base64
import socket
import sys

import pytest

from agentscope_sandbox_ext._firecracker._backend import (
    GuestAgentClient,
    GuestAgentError,
    _b64decode,
    _recv_exactly_sync,
)

from _helpers.guest_agent_server import GuestAgentUnixServer


# ── connection factory wired to the Unix-socket test server ──────


def _make_unix_connect(path: str):
    """Return an async callable that opens a real Unix-socket connection."""
    loop_holder: dict = {}

    async def _connect() -> socket.socket:
        loop = asyncio.get_event_loop()

        def _do() -> socket.socket:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(5.0)
            s.connect(path)
            s.settimeout(None)
            return s

        return await loop.run_in_executor(None, _do)

    return _connect


@pytest.fixture
def guest_server():
    """Start a real guest-agent Unix-socket server for the test."""
    srv = GuestAgentUnixServer()
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture
def client_factory(guest_server: GuestAgentUnixServer):
    """Factory building a :class:`GuestAgentClient` against the server."""
    def _make(*, request_timeout: float = 5.0) -> GuestAgentClient:
        return GuestAgentClient(
            connect=_make_unix_connect(guest_server.path),
            request_timeout=request_timeout,
        )
    return _make


# ── ping ────────────────────────────────────────────────────────


async def test_ping_returns_true_when_agent_is_up(client_factory):
    """``ping`` returns ``True`` when the real agent responds."""
    client = client_factory()
    assert await client.ping() is True


async def test_ping_returns_false_when_agent_is_down(tmp_path):
    """``ping`` returns ``False`` (never raises) when connect fails.

    Points the client at a socket path nobody is listening on — a
    real ``ConnectionRefusedError`` that the client must translate to
    ``False`` rather than propagate.
    """
    bad_path = str(tmp_path / "does-not-exist.sock")
    client = GuestAgentClient(
        connect=_make_unix_connect(bad_path),
        request_timeout=1.0,
    )
    assert await client.ping() is False


# ── exec ────────────────────────────────────────────────────────


async def test_exec_echo_returns_stdout(client_factory):
    """``exec_shell(["echo", "hi"])`` really spawns ``echo`` and
    captures its stdout."""
    client = client_factory()
    result = await client.exec_shell(["echo", "hello-firecracker"])
    assert result.exit_code == 0
    assert result.stdout.strip() == b"hello-firecracker"
    assert result.stderr == b""


async def test_exec_exit_code_propagates(client_factory):
    """A non-zero exit code is faithfully returned."""
    client = client_factory()
    result = await client.exec_shell(
        ["sh", "-c", "exit 42"],
    )
    assert result.exit_code == 42


async def test_exec_captures_stderr(client_factory):
    """stderr is captured separately from stdout."""
    client = client_factory()
    result = await client.exec_shell(
        ["sh", "-c", "echo to-out; echo to-err 1>&2"],
    )
    assert result.exit_code == 0
    assert result.stdout.strip() == b"to-out"
    assert result.stderr.strip() == b"to-err"


async def test_exec_cwd_is_respected(client_factory, tmp_path):
    """``cwd`` is forwarded to ``subprocess.run`` and the process
    really changes directory."""
    client = client_factory()
    result = await client.exec_shell(
        ["pwd"],
        cwd=str(tmp_path),
    )
    assert result.exit_code == 0
    # Resolve symlinks — /tmp on macOS is /private/tmp.
    resolved_expected = str(tmp_path.resolve())
    resolved_actual = result.stdout.decode().strip()
    # The guest agent runs on the same host (test mode), so the path
    # should match.  On some systems ``pwd`` may report a symlink-
    # resolved path; normalise both sides.
    import os
    assert os.path.realpath(resolved_actual) == os.path.realpath(
        resolved_expected,
    )


async def test_exec_unknown_command_returns_127(client_factory):
    """A missing binary surfaces as exit 127 (the shell convention).

    The guest agent's ``_handle_exec`` catches ``FileNotFoundError``
    and returns ``exit_code=127`` — this mirrors the production
    behaviour the host backend relies on.
    """
    client = client_factory()
    result = await client.exec_shell(
        ["/nonexistent/binary/that/does/not/exist"],
    )
    assert result.exit_code == 127


async def test_exec_timeout_is_enforced(client_factory):
    """When ``timeout`` is supplied the subprocess is really killed
    and the result carries ``exit_code=-1``."""
    client = client_factory()
    result = await client.exec_shell(
        ["sleep", "30"],
        timeout=0.5,
    )
    assert result.exit_code == -1
    # The handler appends ``b"\ntimed out"`` to stderr.
    assert b"timed out" in result.stderr


# ── file I/O ───────────────────────────────────────────────────


async def test_write_then_read_roundtrip(client_factory, tmp_path):
    """``write_file`` really writes bytes; ``read_file`` really reads
    them back."""
    client = client_factory()
    payload = b"line one\nline two\n\x00binary\xFF" * 10
    target = tmp_path / "roundtrip.bin"
    await client.write_file(str(target), payload)
    assert target.read_bytes() == payload
    read_back = await client.read_file(str(target))
    assert read_back == payload


async def test_read_file_missing_raises_filenotfound(client_factory, tmp_path):
    """A missing file surfaces as :class:`FileNotFoundError`."""
    client = client_factory()
    missing = tmp_path / "no-such-file"
    with pytest.raises(FileNotFoundError):
        await client.read_file(str(missing))


async def test_write_file_creates_parent_dirs(client_factory, tmp_path):
    """``write_file`` creates parent directories as needed."""
    client = client_factory()
    target = tmp_path / "deep" / "nested" / "dir" / "out.txt"
    await client.write_file(str(target), b"deep")
    assert target.read_bytes() == b"deep"


async def test_read_empty_file_returns_empty_bytes(client_factory, tmp_path):
    """An empty file reads back as ``b""``."""
    client = client_factory()
    empty = tmp_path / "empty"
    empty.write_bytes(b"")
    result = await client.read_file(str(empty))
    assert result == b""


# ── wire-protocol helpers ──────────────────────────────────────


async def test_b64decode_handles_empty_string():
    """``_b64decode("")`` returns ``b""`` rather than raising."""
    assert _b64decode("") == b""
    assert _b64decode(b"") == b""


async def test_b64decode_roundtrips_binary():
    """``_b64decode`` inverts ``base64.b64encode`` for binary data."""
    for payload in (b"", b"\x00", b"\xff\xfe", b"hello world"):
        encoded = base64.b64encode(payload).decode("ascii")
        assert _b64decode(encoded) == payload


def test_recv_exactly_sync_reads_exact_count():
    """``_recv_exactly_sync`` blocks until ``n`` bytes arrive."""
    # Use a real connected socket pair.
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        payload = bytes(range(256))
        a.sendall(payload)
        # Read in two halves to make sure the loop in
        # ``_recv_exactly_sync`` really accumulates across ``recv``.
        first = _recv_exactly_sync(b, 100)
        second = _recv_exactly_sync(b, 156)
        assert first + second == payload
    finally:
        a.close()
        b.close()


def test_recv_exactly_sync_raises_on_eof():
    """A closed connection mid-frame raises :class:`EOFError`."""
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        a.sendall(b"partial")
        a.close()
        with pytest.raises(EOFError):
            _recv_exactly_sync(b, 100)
    finally:
        b.close()


# ── malformed requests ─────────────────────────────────────────


async def test_unknown_op_returns_error(client_factory):
    """An unknown ``op`` makes the agent return ``ok: false``."""
    client = client_factory()
    resp = await client._request({"op": "totally-bogus"})
    assert resp.get("ok") is False
    assert "unknown op" in str(resp.get("error", "")).lower()


async def test_exec_missing_argv_returns_error(client_factory):
    """A malformed ``exec`` request (no ``argv``) is rejected."""
    client = client_factory()
    with pytest.raises(GuestAgentError):
        await client.exec_shell([])
