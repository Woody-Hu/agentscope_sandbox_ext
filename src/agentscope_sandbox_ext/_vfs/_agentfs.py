# -*- coding: utf-8 -*-
"""``agentfs`` — a reference VFS backend implementation.

``agentfs`` is a pure-Python VFS that satisfies
:class:`BackendBase` without ever spawning a container or microVM:

* File I/O (``read_file`` / ``write_file``) is translated into direct
  reads/writes against a per-workspace host directory.
* ``exec_shell`` is translated into a host-side ``asyncio.subprocess``
  run, constrained to the workspace's host directory as ``cwd``.

The result is a workspace that is functionally equivalent to a
container backend for the purposes of the agentscope builtin tools
(Bash, Read, Write, Edit, Grep, Glob), but with **microsecond**
provisioning — there is no image build, no container start, no guest
agent bootstrap.  This makes ``agentfs`` the natural "theoretical
upper bound" baseline in benchmarks that compare cold-boot latency
across real container / microVM backends.

It is also a useful development / CI backend: any test that runs
against ``agentfs`` exercises the full sandboxed-workspace lifecycle
(provision → initialize → exec → teardown) with real I/O and real
subprocesses, but needs no Docker daemon.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from agentscope._logging import logger
from agentscope.tool._builtin._backend import ExecResult

from ._base import VFSBackendBase, VFSWorkspaceBase


# ── backend ─────────────────────────────────────────────────────


class AgentFSBackend(VFSBackendBase):
    """VFS backend that translates primitives to host I/O + subprocess.

    Args:
        workdir (`str`):
            Host directory backing the VFS.  ``read_file`` /
            ``write_file`` operate directly on paths under this dir;
            ``exec_shell`` runs with this dir as ``cwd``.
        exec_timeout (`float`, defaults to ``60.0``):
            Default wall-clock cap for a translated subprocess when the
            caller does not pass one.
    """

    def __init__(
        self,
        *,
        workdir: str,
        exec_timeout: float = 60.0,
    ) -> None:
        super().__init__(workdir=workdir)
        self._exec_timeout = exec_timeout
        os.makedirs(workdir, exist_ok=True)

    # ── translation hooks ───────────────────────────────────────

    async def _exec_translated(
        self,
        command: list[str],
        *,
        cwd: str,
        timeout: float | None,
    ) -> ExecResult:
        """Run *command* as a host subprocess rooted at *cwd*."""
        if not command:
            return ExecResult(
                exit_code=2,
                stdout=b"",
                stderr=b"agentfs: empty command",
            )
        effective_timeout = (
            timeout if timeout is not None else self._exec_timeout
        )
        # Resolve cwd relative to workdir so callers passing relative
        # paths don't escape the workspace root.
        if not os.path.isabs(cwd):
            cwd = os.path.join(self._workdir, cwd)
        cwd = os.path.realpath(cwd)
        # Belt-and-braces: keep cwd inside workdir.
        workdir_real = os.path.realpath(self._workdir)
        if not (cwd == workdir_real or cwd.startswith(workdir_real + os.sep)):
            return ExecResult(
                exit_code=126,
                stdout=b"",
                stderr=(
                    f"agentfs: cwd {cwd!r} escapes workdir "
                    f"{workdir_real!r}"
                ).encode("utf-8"),
            )
        os.makedirs(cwd, exist_ok=True)
        logger.debug(
            "agentfs exec: %s (cwd=%s, timeout=%s)",
            command,
            cwd,
            effective_timeout,
        )
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            return ExecResult(
                exit_code=127,
                stdout=b"",
                stderr=(
                    f"agentfs: command not found: {command[0]!r}"
                ).encode("utf-8"),
            )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=effective_timeout,
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
            return ExecResult(
                exit_code=-1,
                stdout=b"",
                stderr=(
                    f"agentfs: timed out after {effective_timeout}s"
                ).encode("utf-8"),
            )
        return ExecResult(
            exit_code=proc.returncode if proc.returncode is not None else -1,
            stdout=stdout or b"",
            stderr=stderr or b"",
        )

    async def _read_translated(self, path: str) -> bytes:
        """Read a file relative to the workspace root."""
        full = self._resolve(path)
        try:
            with open(full, "rb") as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"agentfs: not found: {path}")
        except IsADirectoryError as exc:
            raise IsADirectoryError(f"agentfs: is a directory: {path}") from exc

    async def _write_translated(self, path: str, data: bytes) -> None:
        """Write *data* to *path* relative to the workspace root."""
        full = self._resolve(path)
        os.makedirs(os.path.dirname(full) or ".", exist_ok=True)
        # Atomic-ish write: write to a temp sibling then rename.
        tmp = full + ".agentfs.tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, full)

    # ── helpers ──────────────────────────────────────────────────

    def _resolve(self, path: str) -> str:
        """Resolve *path* under workdir, refusing to escape it."""
        if os.path.isabs(path):
            # Treat absolute paths as workspace-relative — matches the
            # container semantics where "/" is the container root.
            rel = path.lstrip(os.sep)
        else:
            rel = path
        full = os.path.realpath(os.path.join(self._workdir, rel))
        workdir_real = os.path.realpath(self._workdir)
        if not (
            full == workdir_real or full.startswith(workdir_real + os.sep)
        ):
            raise PermissionError(
                f"agentfs: path {path!r} escapes workdir {workdir_real!r}",
            )
        return full

    async def close(self) -> None:
        """Nothing to release for agentfs — host dir stays on disk."""


# ── workspace ───────────────────────────────────────────────────


class AgentFSWorkspace(VFSWorkspaceBase):
    """Workspace backed by :class:`AgentFSBackend`.

    Provisioning is a single ``os.makedirs`` plus a Python object
    construction — there is no image build, no container start, no
    guest agent.  This is the cheapest backend in the package and the
    intended "theoretical upper bound" baseline for benchmarks.
    """

    sandbox_kind = "agentfs"

    async def _make_backend(self, workdir: str) -> AgentFSBackend:
        return AgentFSBackend(workdir=workdir)


__all__ = ["AgentFSBackend", "AgentFSWorkspace"]
