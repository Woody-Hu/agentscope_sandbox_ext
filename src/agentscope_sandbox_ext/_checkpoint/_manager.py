# -*- coding: utf-8 -*-
"""Checkpoint manager: two-level (pause / suspend) snapshot orchestration.

Sits above :class:`SnapshotStore` (durable) and the :class:`SandboxRuntime`
(live capture/restore).  Three operations capture state; one restores it:

* **pause** — capture to the worker's *node-local* snapshot slot.  The
  snapshot stays on that node; a subsequent resume pinned to the same
  node restores it without any remote fetch.  This is the fast,
  short-lived checkpoint.
* **suspend** — capture locally, then **upload** the tree to the durable
  :class:`SnapshotStore`.  Location-independent; any worker can resume
  from it (at the cost of a download + stage).
* **bake_golden** — one-time capture for a template, uploaded to the
  durable store.  Shared, immutable; new actors clone-restore from it
  instead of cold-booting.
* **resume** — activate a snapshot on a worker.  ``pause`` snapshots
  restore locally (same node only — otherwise
  :class:`PauseSnapshotNotLocal`); ``last`` / ``golden`` snapshots are
  downloaded, staged into the worker's snapshot slot, then restored.

:class:`Singleflight` dedups concurrent suspend/pause on the same actor
so two racing suspend calls produce one snapshot, not two.
"""

from __future__ import annotations

import re
import shutil
import tempfile

from .._actor._types import (
    KIND_GOLDEN,
    KIND_LAST,
    KIND_PAUSE,
    SCOPE_FULL,
    ActorRef,
    ActorSnapshotRef,
    TemplateRef,
)
from .._worker._types import Worker
from ._singleflight import Singleflight
from ._types import CheckpointConfig, PauseSnapshotNotLocal

_SAFE = re.compile(r"[^a-zA-Z0-9-]")


def _safe_component(s: str) -> str:
    """Reduce any string to a single path component (no ``os.sep``).

    Snapshot tags are used as directory names under a backend's
    ``_snapshots_root``; they must not contain ``/``.  Actor namespaces /
    names are expected to be DNS-1123, but this sanitises arbitrary input
    so a hostile or sloppy caller cannot escape the snapshot dir.
    """
    return _SAFE.sub("-", s).strip("-") or "x"


def _tag(actor: ActorRef, suffix: str) -> str:
    return f"{_safe_component(actor.namespace)}_{_safe_component(actor.name)}_{suffix}"


class CheckpointManager:
    """Orchestrates pause / suspend / resume over a durable store.

    Args:
        config: Durable store + pause/suspend scopes.
        singleflight: Optional shared singleflight for per-actor dedup.
    """

    def __init__(
        self,
        config: CheckpointConfig,
        *,
        singleflight: Singleflight | None = None,
    ) -> None:
        self._config = config
        self._sf = singleflight or Singleflight()

    # ── capture ──────────────────────────────────────────────────

    async def pause(
        self,
        actor: ActorRef,
        worker: Worker,
        *,
        scope: str | None = None,
    ) -> ActorSnapshotRef:
        """Node-local capture.  Stays on ``worker.node``; no upload."""
        scope = scope or self._config.on_pause
        tag = _tag(actor, "pause")

        async def _do() -> ActorSnapshotRef:
            await worker.runtime.snapshot(tag)
            return ActorSnapshotRef(
                snapshot_id=tag,
                kind=KIND_PAUSE,
                scope=scope,
                node=worker.node,
            )

        return await self._sf.run(f"pause:{actor.key}", _do)

    async def suspend(
        self,
        actor: ActorRef,
        worker: Worker,
        *,
        scope: str | None = None,
    ) -> ActorSnapshotRef:
        """Capture locally then upload the tree to the durable store."""
        scope = scope or self._config.on_suspend
        tag = _tag(actor, "last")

        async def _do() -> ActorSnapshotRef:
            local_path = await worker.runtime.snapshot(tag)
            meta = await self._config.durable_store.put_tree(
                actor_id=actor.key,
                tag="last",
                source_dir=local_path,
            )
            return ActorSnapshotRef(
                snapshot_id=meta.snapshot_ref,
                kind=KIND_LAST,
                scope=scope,
            )

        return await self._sf.run(f"suspend:{actor.key}", _do)

    async def bake_golden(
        self,
        template: TemplateRef,
        worker: Worker,
        *,
        scope: str = SCOPE_FULL,
    ) -> ActorSnapshotRef:
        """One-time per-template capture, uploaded to the durable store."""
        tag = f"golden_{_safe_component(template.name)}_v{template.version}"

        async def _do() -> ActorSnapshotRef:
            local_path = await worker.runtime.snapshot(tag)
            # Content-addressed actor_id for the golden snapshot so the
            # same template always resolves to the same store key.
            actor_id = f"template:{template.key}"
            meta = await self._config.durable_store.put_tree(
                actor_id=actor_id,
                tag="golden",
                source_dir=local_path,
                template_id=template.key,
            )
            return ActorSnapshotRef(
                snapshot_id=meta.snapshot_ref,
                kind=KIND_GOLDEN,
                scope=scope,
            )

        return await self._sf.run(f"golden:{template.key}", _do)

    # ── restore ──────────────────────────────────────────────────

    async def resume(
        self,
        actor: ActorRef,
        worker: Worker,
        snapshot: ActorSnapshotRef,
    ) -> None:
        """Activate *snapshot* on *worker*.

        Pause snapshots restore locally only (same node); otherwise
        :class:`PauseSnapshotNotLocal` is raised so the caller can fall
        back to a durable snapshot.  ``last`` / ``golden`` snapshots are
        downloaded from the durable store, staged into the worker's
        snapshot slot, and restored.
        """
        if snapshot.kind == KIND_PAUSE:
            if snapshot.node != worker.node:
                raise PauseSnapshotNotLocal(
                    f"pause snapshot {snapshot.snapshot_id!r} is on node "
                    f"{snapshot.node!r} but worker is on {worker.node!r}",
                )
            await worker.runtime.restore(snapshot.snapshot_id)
            return

        # Durable path: download tree → stage into a local slot → restore.
        tmp_dir = tempfile.mkdtemp(prefix="ckpt_resume_")
        try:
            await self._config.durable_store.restore_tree(
                snapshot.snapshot_id, tmp_dir,
            )
            tag = _tag(actor, "resume")
            await worker.runtime.stage(tag, tmp_dir)
            await worker.runtime.restore(tag)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # ── helpers ──────────────────────────────────────────────────

    async def list_snapshots(self, actor: ActorRef) -> list[ActorSnapshotRef]:
        """List durable (last/golden) snapshots for *actor*."""
        metas = await self._config.durable_store.list(actor.key)
        return [
            ActorSnapshotRef(
                snapshot_id=m.snapshot_ref,
                kind=KIND_LAST,
                scope=self._config.on_suspend,
            )
            for m in metas
        ]


__all__ = ["CheckpointManager"]
