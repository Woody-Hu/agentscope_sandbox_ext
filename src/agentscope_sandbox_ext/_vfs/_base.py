# -*- coding: utf-8 -*-
"""Virtual File System (VFS) backend abstraction.

A VFS backend is a *translation layer* that satisfies the
:class:`agentscope.tool._builtin._backend.BackendBase` contract —
``exec_shell`` / ``read_file`` / ``write_file`` — without ever spawning
a real container or microVM.  Instead, every primitive is translated
into operations against an in-process or host-backed virtual workspace.

This module defines :class:`VFSBackendBase`, the abstract translation
interface, plus :class:`VFSWorkspaceBase`, the
:class:`SandboxedWorkspaceExtBase` specialization that wires a VFS
backend into the agentscope sandboxed-workspace lifecycle without
requiring Docker / Firecracker / Kata / gVisor.

The split mirrors the existing :mod:`._firecracker` package:

* :class:`VFSBackendBase` ↔ :class:`FirecrackerBackend`
* :class:`VFSWorkspaceBase` ↔ :class:`FirecrackerWorkspace`

A concrete implementation (:mod:`._agentfs`) ships in the same package
and is used both as a reference and as the "theoretical upper bound"
baseline in ``tests/benchmarks``.
"""

from __future__ import annotations

import abc
import asyncio
import os
import tempfile
from typing import Any

from agentscope._logging import logger
from agentscope.mcp import MCPClient
from agentscope.tool._builtin._backend import BackendBase, ExecResult

from .._base import SandboxedWorkspaceExtBase


# ── backend abstraction ──────────────────────────────────────────


class VFSBackendBase(BackendBase):
    """Abstract VFS translation backend.

    A VFS backend translates the three :class:`BackendBase` primitives
    into operations against some virtual workspace (an in-memory tree,
    a host directory, a FUSE mount, a 9p server, ...).  The default
    :meth:`getcwd` reads a cached ``workdir`` so subclasses do not pay a
    per-call translation round trip.

    Subclasses MUST implement :meth:`_exec_translated`,
    :meth:`_read_translated` and :meth:`_write_translated`; everything
    else (including the derived filesystem helpers ``file_exists``,
    ``is_dir``, ``list_dir``, ...) is inherited from
    :class:`BackendBase` and works out of the box.
    """

    def __init__(self, *, workdir: str = "/workspace") -> None:
        self._workdir = workdir

    # ── BackendBase primitive contract ──────────────────────────

    async def getcwd(self) -> str:
        return self._workdir

    async def exec_shell(
        self,
        command: list[str],
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        return await self._exec_translated(
            list(command),
            cwd=cwd or self._workdir,
            timeout=timeout,
        )

    async def read_file(self, path: str) -> bytes:
        return await self._read_translated(path)

    async def write_file(self, path: str, data: bytes) -> None:
        await self._write_translated(path, data)

    # ── translation hooks (subclass contract) ────────────────────

    @abc.abstractmethod
    async def _exec_translated(
        self,
        command: list[str],
        *,
        cwd: str,
        timeout: float | None,
    ) -> ExecResult:
        """Translate an ``exec_shell`` call into VFS operations.

        Implementations are free to interpret ``command`` however they
        like (in-process interpreter, host-side ``subprocess``, a remote
        9p gateway, ...).  The only constraint is that the result must
        carry an ``exit_code`` / ``stdout`` / ``stderr`` triple.
        """

    @abc.abstractmethod
    async def _read_translated(self, path: str) -> bytes:
        """Translate a ``read_file`` call into VFS operations."""

    @abc.abstractmethod
    async def _write_translated(self, path: str, data: bytes) -> None:
        """Translate a ``write_file`` call into VFS operations."""

    # ── lifecycle hook ───────────────────────────────────────────

    async def close(self) -> None:
        """Release any VFS-held resources (override as needed)."""


# ── workspace abstraction ────────────────────────────────────────


class VFSWorkspaceBase(SandboxedWorkspaceExtBase):
    """Sandboxed workspace whose backend is a :class:`VFSBackendBase`.

    Unlike the container / microVM backends, a VFS workspace:

    * Has **no** Docker / Firecracker / Kata runtime to verify —
      :meth:`verify_runtime_available` always succeeds.
    * Does **not** build or pull an image; :meth:`_provision_backend`
      simply instantiates the VFS backend (a pure-Python object), so
      provisioning is microsecond-cheap.  This makes VFS backends the
      natural "theoretical upper bound" baseline in benchmarks that
      compare cold-boot latency across real backends.
    * Has **no** guest agent to bootstrap; :meth:`_bootstrap_commands`
      returns an empty list.

    Concrete subclasses set :attr:`sandbox_kind` and override
    :meth:`_make_backend` to return a concrete
    :class:`VFSBackendBase` subclass instance.
    """

    #: VFS workspaces use a host tempdir by default; subclasses
    #: typically override this with a per-workspace path.
    sandbox_kind: str = "vfs"

    def __init__(
        self,
        *,
        workspace_id: str | None = None,
        host_workdir: str | None = None,
        default_mcps: list[MCPClient] | None = None,
        skill_paths: list[str] | None = None,
    ) -> None:
        # ``SandboxedWorkspaceBase`` does not expose ``__init__``
        # kwargs for host_workdir on the sandboxed path — we stash it
        # here so :meth:`_provision_backend` can hand it to the VFS
        # backend factory.
        SandboxedWorkspaceExtBase.__init__(
            self,
            workspace_id=workspace_id,
            default_mcps=default_mcps,
            skill_paths=skill_paths,
        )
        self._host_workdir = host_workdir or tempfile.mkdtemp(
            prefix=f"as_vfs_{self.workspace_id}_",
        )
        self._vfs_backend: VFSBackendBase | None = None

    # ── SandboxedWorkspaceExtBase overrides ─────────────────────

    @classmethod
    async def verify_runtime_available(cls) -> None:
        """VFS backends need no host runtime — always succeeds."""
        return None

    def get_backend(self) -> VFSBackendBase:
        """Return the bound VFS backend (set by ``_provision_backend``)."""
        if self._vfs_backend is None:
            raise RuntimeError(
                "VFS backend not provisioned — call _provision_backend "
                "(or initialize()) first",
            )
        return self._vfs_backend

    # ── SandboxedWorkspaceBase template-method hooks ────────────

    def _bootstrap_commands(self) -> list[str]:
        """No guest agent / image bootstrap for VFS workspaces."""
        return []

    async def _provision_backend(self) -> None:
        """Instantiate the VFS backend (microsecond-cheap)."""
        if self._vfs_backend is None:
            logger.debug(
                "VFSWorkspace: provisioning backend in %s",
                self._host_workdir,
            )
            self._vfs_backend = await self._make_backend(self._host_workdir)

    async def _teardown_backend(self) -> None:
        """Close the VFS backend; leaves host_workdir on disk."""
        if self._vfs_backend is not None:
            await self._vfs_backend.close()
            self._vfs_backend = None

    async def get_instructions(self) -> str:
        return (
            f"You are running inside a VFS workspace "
            f"({self.sandbox_kind}). Working directory: {self._host_workdir}"
        )

    # ── subclass contract ────────────────────────────────────────

    @abc.abstractmethod
    async def _make_backend(self, workdir: str) -> VFSBackendBase:
        """Construct the concrete VFS backend bound to *workdir*."""

    # ── metrics ─────────────────────────────────────────────────

    async def metrics(self) -> dict[str, Any]:
        base = await super().metrics()
        base.update(
            {
                "vfs_backend_type": type(self._vfs_backend).__name__
                if self._vfs_backend is not None
                else None,
                "host_workdir": self._host_workdir,
            },
        )
        return base


__all__ = ["VFSBackendBase", "VFSWorkspaceBase"]
