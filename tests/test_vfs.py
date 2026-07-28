# -*- coding: utf-8 -*-
"""Real (no-mock) tests for the VFS backend abstraction and the
``agentfs`` reference implementation.

These tests exercise the full translation path — real host I/O,
real ``asyncio.subprocess`` exec — against a per-test tempdir.  No
Docker daemon, no Firecracker, no mock objects anywhere in the stack.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile

import pytest

from agentscope_sandbox_ext import (
    AgentFSBackend,
    AgentFSWorkspace,
    VFSBackendBase,
    VFSWorkspaceBase,
)


# ── fixtures ────────────────────────────────────────────────────


@pytest.fixture
def workdir():
    d = tempfile.mkdtemp(prefix="as_vfs_test_")
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def backend(workdir):
    return AgentFSBackend(workdir=workdir)


# ── type / inheritance contract ─────────────────────────────────


def test_agentfs_backend_is_vfs_backend_base_subclass():
    assert issubclass(AgentFSBackend, VFSBackendBase)


def test_agentfs_workspace_is_vfs_workspace_base_subclass():
    assert issubclass(AgentFSWorkspace, VFSWorkspaceBase)


def test_agentfs_workspace_sandbox_kind():
    assert AgentFSWorkspace.sandbox_kind == "agentfs"


def test_vfs_backend_base_is_abstract():
    with pytest.raises(TypeError):
        VFSBackendBase()  # type: ignore[abstract]


# ── verify_runtime_available (classmethod, always succeeds) ────


async def test_verify_runtime_available_always_succeeds():
    # Should not raise — VFS backends need no host runtime.
    await AgentFSWorkspace.verify_runtime_available()


# ── read/write translation ──────────────────────────────────────


async def test_write_then_read_file_roundtrip(backend):
    await backend.write_file("hello.txt", b"hello vfs")
    data = await backend.read_file("hello.txt")
    assert data == b"hello vfs"


async def test_write_file_creates_parent_dirs(backend):
    await backend.write_file("nested/dir/file.txt", b"deep")
    assert await backend.read_file("nested/dir/file.txt") == b"deep"


async def test_read_missing_file_raises_filenotfound(backend):
    with pytest.raises(FileNotFoundError):
        await backend.read_file("does-not-exist.txt")


async def test_write_overwrites_existing_file(backend):
    await backend.write_file("f.txt", b"v1")
    await backend.write_file("f.txt", b"v2")
    assert await backend.read_file("f.txt") == b"v2"


async def test_path_resolution_refuses_to_escape_workdir(backend, workdir):
    # Absolute paths should be treated as workspace-relative, not host.
    await backend.write_file("/etc_pass_shadow", b"x")
    assert await backend.read_file("/etc_pass_shadow") == b"x"
    # But a traversal that would escape should raise.
    with pytest.raises(PermissionError):
        await backend.read_file("../../../etc/passwd")


# ── exec translation ────────────────────────────────────────────


async def test_exec_echo_returns_stdout(backend):
    result = await backend.exec_shell(["sh", "-c", "echo hello"])
    assert result.exit_code == 0
    assert result.stdout.strip() == b"hello"


async def test_exec_writes_to_workspace_files(backend):
    result = await backend.exec_shell(["sh", "-c", "echo data > out.txt"])
    assert result.exit_code == 0
    assert await backend.read_file("out.txt") == b"data\n"


async def test_exec_nonzero_exit_code_propagated(backend):
    result = await backend.exec_shell(["sh", "-c", "exit 7"])
    assert result.exit_code == 7


async def test_exec_missing_command_returns_127(backend):
    result = await backend.exec_shell(["no-such-binary-xyz"])
    assert result.exit_code == 127
    assert b"agentfs" in result.stderr.lower() or b"not found" in result.stderr.lower()


async def test_exec_empty_command_returns_2(backend):
    result = await backend.exec_shell([])
    assert result.exit_code == 2


async def test_exec_timeout_returns_negative_one(backend):
    # Use a sleep longer than the timeout we pass.
    result = await backend.exec_shell(
        ["sh", "-c", "sleep 5"],
        timeout=0.2,
    )
    assert result.exit_code == -1
    assert b"timed out" in result.stderr.lower()


async def test_exec_cwd_escape_rejected(backend):
    result = await backend.exec_shell(
        ["sh", "-c", "pwd"],
        cwd="../../../",
    )
    assert result.exit_code == 126
    assert b"escapes workdir" in result.stderr


# ── getcwd / metrics ────────────────────────────────────────────


async def test_getcwd_returns_workdir(backend, workdir):
    cwd = await backend.getcwd()
    assert cwd == workdir


async def test_metrics_includes_vfs_fields(backend, workdir):
    # metrics is on the workspace, not the backend; build a workspace.
    ws = AgentFSWorkspace(host_workdir=workdir)
    await ws._provision_backend()
    try:
        m = await ws.metrics()
        assert m["sandbox_kind"] == "agentfs"
        assert m["vfs_backend_type"] == "AgentFSBackend"
        assert m["host_workdir"] == workdir
    finally:
        await ws._teardown_backend()


# ── workspace lifecycle (real provision → teardown) ─────────────


async def test_workspace_get_backend_after_provision(workdir):
    ws = AgentFSWorkspace(host_workdir=workdir)
    # Before provision, get_backend should raise.
    with pytest.raises(RuntimeError):
        ws.get_backend()
    await ws._provision_backend()
    try:
        b = ws.get_backend()
        assert isinstance(b, AgentFSBackend)
        # End-to-end: write via the backend, read via host fs.
        await b.write_file("lifecycle.txt", b"ok")
        with open(os.path.join(workdir, "lifecycle.txt"), "rb") as f:
            assert f.read() == b"ok"
    finally:
        await ws._teardown_backend()
        # After teardown, get_backend should raise again.
        with pytest.raises(RuntimeError):
            ws.get_backend()


async def test_workspace_get_instructions(workdir):
    ws = AgentFSWorkspace(host_workdir=workdir)
    instructions = await ws.get_instructions()
    assert "agentfs" in instructions
    assert workdir in instructions


# ── derived BackendBase helpers (inherited, exercised for real) ──


async def test_inherited_file_exists(backend):
    await backend.write_file("exists.txt", b"x")
    assert await backend.file_exists("exists.txt") is True
    assert await backend.file_exists("nope.txt") is False


async def test_inherited_is_dir(backend):
    await backend.write_file("dir/a.txt", b"x")
    assert await backend.is_dir("dir") is True
    assert await backend.is_dir("dir/a.txt") is False


async def test_inherited_list_dir(backend):
    await backend.write_file("d/a.txt", b"x")
    await backend.write_file("d/b.txt", b"y")
    entries = await backend.list_dir("d")
    names = sorted(entries)
    assert names == ["a.txt", "b.txt"]


async def test_inherited_stat_mtime(backend):
    await backend.write_file("m.txt", b"x")
    mtime = await backend.stat_mtime("m.txt")
    assert mtime is not None
    assert mtime > 0


async def test_inherited_delete_path(backend):
    await backend.write_file("del.txt", b"x")
    assert await backend.file_exists("del.txt") is True
    await backend.delete_path("del.txt")
    assert await backend.file_exists("del.txt") is False
