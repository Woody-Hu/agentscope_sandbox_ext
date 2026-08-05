# -*- coding: utf-8 -*-
"""Adapter that wraps a :class:`SandboxedWorkspaceExtBase` backend into the
:class:`SandboxRuntime` protocol.

This is the bridge between the *existing* backend layer (Firecracker /
gVisor / Kata / Sysbox / VFS) and the *new* actor/worker layer.  By
going through this adapter, the worker pool, scheduler and checkpoint
manager never import a concrete backend — they only see
:class:`SandboxRuntime`.  A future ``RemoteSandboxRuntime`` (driving an
external node agent over gRPC) implements the same protocol, which is
what makes the runtime *interoperable* with an external control plane.
"""

from __future__ import annotations

import socket
import uuid
from typing import TYPE_CHECKING

from .._actor._types import SandboxClass
from ._types import SandboxRuntime

if TYPE_CHECKING:  # pragma: no cover - import only for type checkers
    from .._base import SandboxedWorkspaceExtBase


def _default_node() -> str:
    """Best-effort local node name for pause-locality bookkeeping."""
    try:
        return socket.gethostname() or "local"
    except Exception:  # pragma: no cover - defensive
        return "local"


class WorkspaceSandboxRuntime:
    """Adapt a :class:`SandboxedWorkspaceExtBase` into :class:`SandboxRuntime`.

    ``provision`` maps to the backend's ``initialize`` (which provisions
    the sandbox + bootstraps the gateway / skills); ``snapshot`` /
    ``restore`` map 1:1; ``close`` maps 1:1.  The ``sandbox_class`` is
    derived from the backend's ``sandbox_kind`` discriminator so the
    class constraint and the existing discriminator stay in sync.

    Args:
        workspace: A concrete :class:`SandboxedWorkspaceExtBase` instance
            (not yet initialised — ``provision`` will call ``initialize``).
        node: Node name for pause-locality.  Defaults to the hostname.
        worker_id: Optional explicit id; a uuid4 is generated otherwise.
    """

    def __init__(
        self,
        workspace: "SandboxedWorkspaceExtBase",
        *,
        node: str | None = None,
        worker_id: str | None = None,
    ) -> None:
        self._workspace = workspace
        self._worker_id = worker_id or f"w-{uuid.uuid4().hex[:12]}"
        self._node = node or _default_node()
        # Derive the class from the backend's discriminator.  ``vfs``
        # maps to the ``vfs`` class; every other backend maps to its
        # own kind.  This keeps the class constraint truthful.
        kind = getattr(workspace, "sandbox_kind", "vfs")
        self._sandbox_class = SandboxClass.of(kind)

    # ── SandboxRuntime protocol ──────────────────────────────────

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def sandbox_class(self) -> SandboxClass:
        return self._sandbox_class

    @property
    def node(self) -> str:
        return self._node

    @property
    def is_alive(self) -> bool:
        return bool(getattr(self._workspace, "is_alive", False))

    @property
    def workspace(self) -> "SandboxedWorkspaceExtBase":
        """The wrapped backend (for callers that need backend-specific ops)."""
        return self._workspace

    async def provision(self) -> None:
        """Initialise the underlying workspace (provision + bootstrap)."""
        await self._workspace.initialize()

    async def snapshot(self, tag: str) -> str:
        """Write a snapshot under *tag*; return its backend-specific ref."""
        return await self._workspace.snapshot(tag)

    async def restore(self, tag: str) -> None:
        """Restore the workspace to the snapshot identified by *tag*."""
        await self._workspace.restore(tag)

    async def stage(self, tag: str, source_path: str) -> None:
        """Stage an external snapshot tree into the workspace's snapshot slot.

        Currently implemented for VFS-backed workspaces (which expose a
        ``_snapshots_root`` directory and a tag-keyed ``snapshot``/``restore``
        convention).  For other backends this raises
        :class:`NotImplementedError`; gaining native snapshot support means
        overriding this on the concrete runtime.
        """
        import os
        import shutil

        snapshots_root = getattr(self._workspace, "_snapshots_root", None)
        if not snapshots_root:
            raise NotImplementedError(
                f"{type(self._workspace).__name__} has no snapshots_root; "
                "stage() is only supported on VFS-style backends",
            )
        os.makedirs(snapshots_root, exist_ok=True)
        dest = os.path.join(snapshots_root, tag)
        if os.path.exists(dest):
            shutil.rmtree(dest)
        shutil.copytree(
            source_path,
            dest,
            symlinks=True,
            ignore_dangling_symlinks=True,
        )

    async def close(self) -> None:
        await self._workspace.close()


__all__ = ["WorkspaceSandboxRuntime"]
