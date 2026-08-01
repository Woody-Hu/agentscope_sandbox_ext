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
import shutil
import tempfile
from typing import Any

from agentscope._logging import logger
from agentscope.mcp import MCPClient
from agentscope.tool._builtin._backend import BackendBase, ExecResult
from agentscope.workspace._docker._make_dockerfile import GATEWAY_HOME

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

    #: Agent-visible root directory inside the virtual workspace.
    #: Mirrors the container convention (``/workspace``) so the
    #: inherited workspace-layout machinery (``data/``, ``skills/``,
    #: ``sessions/``, ``.mcp``) works unchanged.
    workdir: str = "/workspace"

    #: Directory holding the gateway venv / script / log inside the
    #: virtual workspace.  Reused from the Docker backend so the
    #: inherited gateway-bootstrap template methods find the same
    #: paths.
    _gateway_home: str = GATEWAY_HOME

    def __init__(
        self,
        *,
        workspace_id: str | None = None,
        host_workdir: str | None = None,
        snapshots_root: str | None = None,
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
        # Snapshots live *outside* the workdir so :meth:`restore`
        # (which wipes the workdir) does not delete them.  Defaulting
        # to a sibling dir keeps everything co-located for cleanup
        # while preserving snapshot durability across restores.
        self._snapshots_root = snapshots_root or (
            self._host_workdir.rstrip(os.sep) + ".snapshots"
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

    async def initialize(self) -> None:
        """Provision the VFS backend and set up the workspace layout.

        Overrides :meth:`SandboxedWorkspaceBase.initialize` to skip the
        MCP gateway setup — a VFS workspace has no in-sandbox gateway
        process, so the gateway bootstrap / health-poll path does not
        apply.  Everything else (provision backend, ensure workspace
        layout, seed skills) runs unchanged.

        Idempotent — a no-op when already alive.
        """
        if self.is_alive:
            return
        await self._provision_backend()
        assert (
            self._backend is not None
        ), "_provision_backend must set self._backend before returning"
        await self._ensure_workspace_layout()
        await self._setup_skills()
        self.is_alive = True

    async def _provision_backend(self) -> None:
        """Instantiate the VFS backend (microsecond-cheap).

        ``SandboxedWorkspaceBase`` requires that subclasses set
        ``self._backend`` to a :class:`BackendBase` instance before
        returning from ``_provision_backend``; we bind it to the VFS
        backend so the inherited template-method lifecycle
        (``initialize`` → ``get_backend`` → ... → ``close``) works
        unchanged.
        """
        if self._vfs_backend is None:
            logger.debug(
                "VFSWorkspace: provisioning backend in %s",
                self._host_workdir,
            )
            self._vfs_backend = await self._make_backend(self._host_workdir)
        self._backend = self._vfs_backend

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

    # ── snapshot / restore (VFS translation) ────────────────────
    #
    # VFS workspaces translate snapshot/restore into host-side tree
    # copies against ``_host_workdir``.  The default implementation
    # shipped here is a *deep copy* (``shutil.copytree``) rather than
    # a hardlink tree (``cp -al``): ``exec_shell`` runs arbitrary
    # subprocesses that may mutate files in place (``sed -i``,
    # ``echo >> file``, ``dd conv=notrunc``), and a hardlink tree
    # would leak those mutations back into the snapshot.  containerd
    # overlayfs gets away with hardlinks because the kernel enforces
    # the lower dir's read-only-ness; a VFS workspace has no such
    # enforcement, so we pay the deep-copy cost for correctness.
    #
    # Subclasses whose translation layer can guarantee copy-on-write
    # semantics (e.g. a future ``overlayfs`` VFS backend that mounts a
    # read-only lower + writable upper) override :meth:`_snapshot_to`
    # / :meth:`_restore_from` to use the cheaper primitive.
    #
    # See ``docs/SNAPSHOT.md`` for the open-source survey (E2B,
    # gVisor, Firecracker, containerd, k8s agent-sandbox) and the
    # logical closure for why this is worth shipping.

    async def snapshot(self, tag: str) -> str:
        """Write a deep-copy snapshot of the VFS tree under *tag*.

        Overrides :meth:`SandboxedWorkspaceExtBase.snapshot` with a
        real implementation: the snapshot is a ``shutil.copytree`` of
        ``_host_workdir`` into ``<_snapshots_root>/<tag>/``.  The
        copy is written to a temp sibling first and only renamed into
        place once complete, so a crash mid-copy never leaves a
        half-written snapshot at the tag path.  Replacing an existing
        tag removes the old dir before the rename; there is a brief
        window in which a concurrent ``restore(tag)`` would observe
        ``KeyError`` (sequential snapshot/restore of the same tag is
        the norm — callers retry on ``KeyError``).

        Requires the workspace to be alive (backend provisioned) so
        the workdir tree exists and is quiescent — caller responsibility.

        Args:
            tag (`str`):
                Snapshot identifier.  Namespaced under
                :attr:`_snapshots_root`; reusing a tag replaces it
                (with the brief window noted above).

        Returns:
            `str`:
                Absolute path of the snapshot directory.
        """
        if self._vfs_backend is None:
            raise RuntimeError(
                "VFS workspace not provisioned — call initialize() "
                "before snapshot()",
            )
        if not tag or os.sep in tag or tag in (".", ".."):
            raise ValueError(
                f"Invalid snapshot tag {tag!r}: must be a single path "
                f"component, not empty / '.' / '..' / containing "
                f"{os.sep!r}.",
            )
        os.makedirs(self._snapshots_root, exist_ok=True)
        # Clean up stale temp dirs from a previous crash before
        # writing the new snapshot.  Only touch our own ``.tmp`` /
        # ``.obsolete`` suffixes so a concurrent snapshot of a
        # *different* tag is unaffected.
        self._cleanup_stale_snapshot_dirs(tag)
        dest = os.path.join(self._snapshots_root, tag)
        # Copy to a temp sibling first so a crash mid-copy never leaves
        # a half-written tree at *dest*.  The final move into place is
        # ``rmtree(dest) + os.replace(tmp, dest)``: ``os.replace`` of
        # a non-empty dir fails with ``ENOTEMPTY`` on Linux, so the
        # old snapshot must be removed first.  This leaves a brief
        # window (between the rmtree and the rename) in which a
        # concurrent ``restore(tag)`` would observe ``KeyError`` —
        # acceptable for the snapshot use case (sequential
        # snapshot/restore of the same tag is the norm; the caller
        # retries on ``KeyError``).
        tmp_dest = (
            dest
            + f".tmp.{os.getpid()}.{asyncio.get_event_loop().time():.6f}"
        )
        if os.path.exists(tmp_dest):
            shutil.rmtree(tmp_dest)
        try:
            await self._snapshot_to(tmp_dest)
            if os.path.exists(dest):
                shutil.rmtree(dest)
            os.replace(tmp_dest, dest)
        except Exception:
            if os.path.exists(tmp_dest):
                shutil.rmtree(tmp_dest, ignore_errors=True)
            raise
        logger.debug(
            "VFSWorkspace(%s): snapshot %r -> %s",
            self.sandbox_kind,
            tag,
            dest,
        )
        return dest

    def _cleanup_stale_snapshot_dirs(self, tag: str) -> None:
        """Remove stale ``.tmp`` / ``.obsolete`` dirs for *tag* only.

        Called at the start of :meth:`snapshot` so a prior crash that
        left a half-written temp dir does not wedge the next snapshot.
        Only touches dirs whose name starts with ``<tag>.tmp.`` or
        ``<tag>.obsolete.`` — a concurrent snapshot of a *different*
        tag is unaffected.
        """
        if not os.path.isdir(self._snapshots_root):
            return
        prefixes = (f"{tag}.tmp.", f"{tag}.obsolete.")
        try:
            for entry in os.scandir(self._snapshots_root):
                if entry.is_dir() and entry.name.startswith(prefixes):
                    shutil.rmtree(entry.path, ignore_errors=True)
        except OSError:
            # Best-effort — a scandir failure must not block snapshot.
            pass

    async def restore(self, tag: str) -> None:
        """Reset the VFS tree to the snapshot identified by *tag*.

        Overrides :meth:`SandboxedWorkspaceExtBase.restore` with a
        real implementation: wipes ``_host_workdir`` and deep-copies
        ``<_snapshots_root>/<tag>/`` into it.  The workspace stays
        alive — no re-provision, no re-seed — so this is the cheap
        rollback path.

        The snapshot itself is preserved (so the same tag can be
        restored repeatedly).  Requires the workspace to be alive so
        the backend's ``_workdir`` pointer stays valid after the
        wipe-and-replace (the path is the same, only the contents
        change).

        Args:
            tag (`str`):
                Identifier previously passed to :meth:`snapshot`.

        Raises:
            KeyError: If *tag* does not exist.
            RuntimeError: If the workspace is not alive.
        """
        if self._vfs_backend is None:
            raise RuntimeError(
                "VFS workspace not provisioned — call initialize() "
                "before restore()",
            )
        src = os.path.join(self._snapshots_root, tag)
        if not os.path.isdir(src):
            raise KeyError(
                f"No snapshot named {tag!r} under {self._snapshots_root!r}",
            )
        # Wipe-and-replace via a temp dir + os.replace so a crash
        # mid-restore does not leave the workdir empty.  The temp dir
        # is created next to the workdir so ``os.replace`` stays
        # inside the same filesystem (rename(2) is not cross-fs).
        parent = os.path.dirname(self._host_workdir.rstrip(os.sep)) or "."
        tmp_workdir = (
            self._host_workdir.rstrip(os.sep)
            + f".restore.{os.getpid()}.{asyncio.get_event_loop().time():.6f}"
        )
        if os.path.exists(tmp_workdir):
            shutil.rmtree(tmp_workdir)
        try:
            await self._restore_from(src, tmp_workdir)
            # Swap: move the current workdir aside, move the restored
            # tree into place, then delete the aside copy.  Each
            # rename is atomic on the same filesystem.
            aside = self._host_workdir.rstrip(os.sep) + ".aside"
            if os.path.exists(aside):
                shutil.rmtree(aside)
            os.replace(self._host_workdir, aside)
            try:
                os.replace(tmp_workdir, self._host_workdir)
            except Exception:
                # Roll back: put the original workdir back.
                os.replace(aside, self._host_workdir)
                raise
            shutil.rmtree(aside, ignore_errors=True)
        except Exception:
            if os.path.exists(tmp_workdir):
                shutil.rmtree(tmp_workdir, ignore_errors=True)
            raise
        logger.debug(
            "VFSWorkspace(%s): restore %r <- %s",
            self.sandbox_kind,
            tag,
            src,
        )

    # ── snapshot / restore subclass hooks ───────────────────────
    #
    # The default implementations use ``shutil.copytree`` (deep copy).
    # Subclasses with a cheaper copy primitive (overlayfs upper-dir
    # swap, btrfs reflink, 9p server-side snapshot, ...) override
    # these to use it without touching the public ``snapshot`` /
    # ``restore`` template methods.

    async def _snapshot_to(self, dest: str) -> None:
        """Copy the live workdir tree into *dest* (which must not exist).

        Default implementation: ``shutil.copytree`` deep copy
        preserving symlinks and metadata.  Subclasses override to use
        a cheaper primitive when the translation layer guarantees
        copy-on-write semantics.

        Args:
            dest (`str`):
                Absolute path of the snapshot destination.  Must not
                exist on entry; created by this call.  Caller wraps
                the call in an atomic rename so a crash leaves no
                partial snapshot at *dest*.
        """
        shutil.copytree(
            self._host_workdir,
            dest,
            symlinks=True,
            ignore_dangling_symlinks=True,
        )

    async def _restore_from(self, src: str, dest: str) -> None:
        """Copy the snapshot tree at *src* into *dest* (which must not exist).

        Default implementation: ``shutil.copytree`` deep copy.  The
        caller handles wiping the live workdir and atomically swapping
        *dest* into place, so this hook only needs to materialise the
        tree.

        Args:
            src (`str`):
                Absolute path of the snapshot source dir.
            dest (`str`):
                Absolute path of the restore destination.  Must not
                exist on entry; created by this call.
        """
        shutil.copytree(
            src,
            dest,
            symlinks=True,
            ignore_dangling_symlinks=True,
        )

    # ── subclass contract ────────────────────────────────────────

    @abc.abstractmethod
    async def _make_backend(self, workdir: str) -> VFSBackendBase:
        """Construct the concrete VFS backend bound to *workdir*."""

    # ── metrics ─────────────────────────────────────────────────

    async def metrics(self) -> dict[str, Any]:
        base = await super().metrics()
        # Snapshot count is read defensively — a missing or removed
        # snapshots root should not break ``metrics``.
        snapshot_count = 0
        if os.path.isdir(self._snapshots_root):
            try:
                snapshot_count = sum(
                    1
                    for entry in os.scandir(self._snapshots_root)
                    if entry.is_dir() and not entry.name.startswith(".")
                )
            except OSError:
                snapshot_count = 0
        base.update(
            {
                "vfs_backend_type": type(self._vfs_backend).__name__
                if self._vfs_backend is not None
                else None,
                "host_workdir": self._host_workdir,
                "snapshots_root": self._snapshots_root,
                "snapshot_count": snapshot_count,
            },
        )
        return base


__all__ = ["VFSBackendBase", "VFSWorkspaceBase"]
