# -*- coding: utf-8 -*-
"""Checkpoint bridge — ties an :class:`Actor` to the durable snapshot store.

The existing :class:`SnapshotStore` / :class:`TieredSnapshotStore`
(``_runtime`` package) is a durable, multi-tier, content-addressed
snapshot store.  The existing :meth:`SandboxedWorkspaceExtBase.snapshot`
/ :meth:`restore` API is an *in-workspace* snapshot (the snapshot lives
next to the workdir and the workspace stays alive).

The orchestration layer needs a third thing: a **durable checkpoint
that outlives the worker**.  When an actor suspends, its worker is
freed (the sandbox returns to the pool); the checkpoint must survive
that so a later resume can restore the actor's state into a *different*
worker.  This module bridges the two existing primitives to provide
exactly that.

Two scopes (mirroring the reference runtime):

* :data:`CheckpointScope.DATA` — durable filesystem snapshot only.
  Portable; every backend can satisfy it today via the
  :class:`SnapshotStore` tree helpers.  Resume is a cold-boot over the
  restored data.
* :data:`CheckpointScope.FULL` — memory + FS delta + durable.  Requires
  a backend with a real memory-snapshot primitive.  Backends without it
  **degrade to DATA with a warning** rather than failing, so callers
  can request FULL unconditionally and let the bridge decide.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
from typing import Any

from agentscope._logging import logger

from .._base import SandboxedWorkspaceExtBase
from .._runtime.snapshot_store import SnapshotStore
from .._runtime.tiered_store import TieredSnapshotStore
from .model import Actor, CheckpointScope, Worker


class CheckpointError(Exception):
    """Base class for checkpoint failures."""


class CheckpointBridge:
    """Durable checkpoint/restore for actors, backed by a snapshot store.

    Args:
        store (`SnapshotStore | TieredSnapshotStore`):
            The durable store.  A :class:`TieredSnapshotStore` gives
            hot/warm/cold tiering with eviction; a bare
            :class:`LocalSnapshotStore` is the simplest choice.
    """

    def __init__(self, store: SnapshotStore) -> None:
        self._store = store
        #: Observability log of ``(actor_id, snapshot_ref)`` restore
        #: calls, in order.  Lets tests and operators verify which
        #: snapshot a restore actually used (e.g. the template's golden
        #: vs. the actor's own).  Not a correctness mechanism.
        self.restore_log: list[tuple[str, str]] = []

    async def checkpoint(
        self,
        actor: Actor,
        worker: Worker,
        scope: CheckpointScope,
    ) -> str:
        """Snapshot the worker's state into the durable store.

        Returns the durable ``snapshot_ref`` (stored on
        :attr:`Actor.latest_snapshot_ref` by the orchestrator).  The
        checkpoint outlives the worker — freeing the worker does not
        delete it.

        For :data:`CheckpointScope.FULL`, if the worker's sandbox
        implements ``snapshot()`` (i.e. does not raise
        ``NotImplementedError``), the in-workspace snapshot is taken
        first and then mirrored into the durable store — this captures
        the backend's best-effort memory state.  If the backend does
        not support snapshots, FULL degrades to DATA with a warning.

        Args:
            actor: Owning actor (the snapshot is namespaced per actor).
            worker: The worker hosting the actor (its sandbox is snapshotted).
            scope: :data:`CheckpointScope.FULL` or :data:`CheckpointScope.DATA`.

        Returns:
            The durable ``snapshot_ref``.

        Raises:
            CheckpointError: If the worker has no sandbox or the workdir
                cannot be located.
        """
        sandbox = worker.sandbox
        if sandbox is None:
            raise CheckpointError(
                f"worker {worker.worker_id!r} has no live sandbox to checkpoint"
            )

        tag = f"{scope.value}-{int(time.time() * 1000)}"
        effective_scope = scope

        # FULL scope: try the backend's in-workspace snapshot first so
        # we capture whatever memory state the backend can give us.
        # Backends without the primitive degrade to DATA.
        local_snapshot_dir: str | None = None
        if scope == CheckpointScope.FULL:
            local_snapshot_dir = await self._try_backend_snapshot(sandbox, tag)
            if local_snapshot_dir is None:
                logger.warning(
                    "CheckpointBridge: backend %s has no snapshot primitive; "
                    "FULL scope degrading to DATA for actor %s",
                    type(sandbox).__name__,
                    actor.actor_id,
                )
                effective_scope = CheckpointScope.DATA

        source_dir = local_snapshot_dir or await self._workdir_of(sandbox)
        if source_dir is None or not os.path.isdir(source_dir):
            raise CheckpointError(
                f"cannot locate workdir to checkpoint for worker "
                f"{worker.worker_id!r}"
            )

        meta = await self._store.put_tree(
            actor_id=actor.actor_id,
            tag=f"{effective_scope.value}:{tag}",
            source_dir=source_dir,
            template_id=actor.template_id,
        )
        # Clean up the transient in-workspace snapshot if we made one
        # purely to feed the durable store (DATA scope reads the live
        # workdir directly, so there is nothing to clean).
        if local_snapshot_dir is not None:
            shutil.rmtree(local_snapshot_dir, ignore_errors=True)
        logger.debug(
            "CheckpointBridge: checkpointed actor %s (scope=%s->%s) -> %s",
            actor.actor_id,
            scope.value,
            effective_scope.value,
            meta.snapshot_ref,
        )
        return meta.snapshot_ref

    async def restore(
        self,
        actor: Actor,
        worker: Worker,
        snapshot_ref: str,
    ) -> None:
        """Restore a durable checkpoint into the worker's sandbox.

        Materialises the snapshot tree into a temp dir, then swaps it
        into the worker's workdir.  If the backend implements
        ``restore()`` and the snapshot was originally a FULL-scope
        in-workspace snapshot, the backend's restore is attempted
        first; otherwise the durable tree is restored directly.

        Args:
            actor: Owning actor.
            worker: The worker whose sandbox will receive the state.
            snapshot_ref: Previously returned by :meth:`checkpoint`.

        Raises:
            CheckpointError: If the worker has no sandbox or the
                snapshot is missing.
        """
        sandbox = worker.sandbox
        if sandbox is None:
            raise CheckpointError(
                f"worker {worker.worker_id!r} has no live sandbox to restore into"
            )
        workdir = await self._workdir_of(sandbox)
        if workdir is None:
            raise CheckpointError(
                f"cannot locate workdir for worker {worker.worker_id!r}"
            )
        self.restore_log.append((actor.actor_id, snapshot_ref))

        # Materialise the durable snapshot into a temp dir, then swap
        # into the workdir.  We do not rely on the backend's
        # in-workspace restore because the durable snapshot is keyed by
        # the store's snapshot_ref, not the backend's tag namespace.
        tmp_parent = os.path.dirname(workdir.rstrip(os.sep)) or "."
        tmp_dir = tempfile.mkdtemp(
            prefix=f"ckpt-restore-{actor.actor_id}-",
            dir=tmp_parent,
        )
        try:
            await self._store.restore_tree(snapshot_ref, tmp_dir)
            self._swap_workdir(workdir, tmp_dir)
        except Exception:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise
        logger.debug(
            "CheckpointBridge: restored snapshot %s into worker %s",
            snapshot_ref,
            worker.worker_id,
        )

    # ── internals ───────────────────────────────────────────────

    async def _try_backend_snapshot(
        self, sandbox: SandboxedWorkspaceExtBase, tag: str
    ) -> str | None:
        """Try the backend's in-workspace snapshot; return path or None."""
        try:
            path = await sandbox.snapshot(tag)
            return str(path)
        except NotImplementedError:
            return None
        except Exception:
            logger.exception(
                "CheckpointBridge: backend snapshot raised; degrading to DATA"
            )
            return None

    async def _workdir_of(self, sandbox: SandboxedWorkspaceExtBase) -> str | None:
        """Locate the host-side workdir for *sandbox*.

        VFS workspaces expose ``_host_workdir``; other backends may
        expose ``getcwd`` via their backend.  Returns ``None`` if no
        host-accessible workdir can be found (the caller then cannot
        do a DATA-scope checkpoint).
        """
        host_workdir = getattr(sandbox, "_host_workdir", None)
        if host_workdir and os.path.isdir(host_workdir):
            return host_workdir
        backend = getattr(sandbox, "_backend", None) or getattr(
            sandbox, "_vfs_backend", None
        )
        if backend is not None:
            try:
                cwd = await backend.getcwd()
            except Exception:
                return None
            if cwd and os.path.isdir(cwd):
                return cwd
        return None

    @staticmethod
    def _swap_workdir(workdir: str, tmp_dir: str) -> None:
        """Atomically swap *tmp_dir* into *workdir*.

        Moves the current workdir aside, renames the restored tree into
        place, then deletes the aside copy.  Each rename is atomic on
        the same filesystem.  On failure the original workdir is moved
        back.
        """
        aside = workdir.rstrip(os.sep) + ".aside"
        if os.path.exists(aside):
            shutil.rmtree(aside, ignore_errors=True)
        if os.path.exists(workdir):
            os.replace(workdir, aside)
        else:
            aside = ""
        try:
            os.replace(tmp_dir, workdir)
        except Exception:
            if aside:
                os.replace(aside, workdir)
            raise
        if aside:
            shutil.rmtree(aside, ignore_errors=True)


__all__ = ["CheckpointBridge", "CheckpointError"]
