# -*- coding: utf-8 -*-
"""Actor lifecycle: the state machine that orchestrates resume / suspend / pause.

:class:`ActorLifecycle` is the only component that mutates an actor's
state.  Each workflow is serialised per-actor by the keyed
:class:`LockProvider` (so two concurrent ``suspend`` calls on the same
actor run one at a time) and every registry write is conditional on the
record's optimistic ``version`` (so a stale writer cannot clobber a
fresh one).  This mirrors the reference runtime's
optimistic-version + distributed-lock combination.

State machine::

        create
    ┌──────────────────────────────┐
    ▼                               │
   SUSPENDED ──resume──▶ RESUMING ──ok──▶ RUNNING
       ▲                   │              │
       │               fail/timeout       │ suspend / pause
       │                   ▼              ▼
       └───────────── SUSPENDING ◀────────┘
                            │ ok
                            ▼
                        SUSPENDED

Resume picks the snapshot to restore from (the actor's ``last_snapshot``
or, on first resume, the template's golden snapshot).  A pause snapshot
is node-local, so resume passes its node as a ``prefer_node`` hint; if
the scheduler lands on a different node the checkpoint manager raises
:class:`PauseSnapshotNotLocal` and the lifecycle falls back to the
durable golden snapshot.
"""

from __future__ import annotations

import asyncio

from agentscope._logging import logger

from .._checkpoint._manager import CheckpointManager
from .._checkpoint._types import PauseSnapshotNotLocal
from .._template._template import ActorTemplateRecord, TemplateRegistry
from .._worker._pool import WorkerPool
from ._registry import ActorRegistry, InProcessLockProvider, LockProvider
from ._types import (
    KIND_PAUSE,
    STATUS_RESUMING,
    STATUS_RUNNING,
    STATUS_SUSPENDED,
    STATUS_SUSPENDING,
    ActorRecord,
    ActorRef,
    Constraints,
    TemplateRef,
    VersionConflict,
)


class IllegalTransition(RuntimeError):
    """An actor is not in a state that allows the requested operation."""


class ActorLifecycle:
    """Owns the actor state machine and the resume/suspend/pause workflows.

    Args:
        registry: Actor record store.
        templates: Template record store (for golden-snapshot lookup).
        pool: Worker pool to borrow/release workers from.
        checkpoint: Checkpoint manager for pause/suspend/resume.
        lock_provider: Keyed lock serialising per-actor workflows.
    """

    def __init__(
        self,
        registry: ActorRegistry,
        templates: TemplateRegistry,
        pool: WorkerPool,
        checkpoint: CheckpointManager,
        *,
        lock_provider: LockProvider | None = None,
    ) -> None:
        self._registry = registry
        self._templates = templates
        self._pool = pool
        self._checkpoint = checkpoint
        self._locks = lock_provider or InProcessLockProvider()

    # ── create / delete / read ───────────────────────────────────

    async def create_actor(
        self,
        ref: ActorRef,
        template_ref: TemplateRef,
        constraints: Constraints,
        *,
        tags: dict[str, str] | None = None,
    ) -> ActorRecord:
        """Register a new actor in ``SUSPENDED`` state.

        The actor's ``last_snapshot`` is seeded with the template's
        golden snapshot (if baked) so the first ``resume`` clone-restores
        from it instead of cold-booting.
        """
        template = await self._templates.get(template_ref)
        if template.sandbox_class != constraints.sandbox_class:
            raise ValueError(
                f"constraints.sandbox_class {constraints.sandbox_class} "
                f"!= template {template.sandbox_class}",
            )
        record = ActorRecord(
            ref=ref,
            template=template_ref,
            constraints=constraints,
            status=STATUS_SUSPENDED,
            last_snapshot=template.golden_snapshot,
            tags=dict(tags or {}),
        )
        return await self._registry.create(record)

    async def delete_actor(self, ref: ActorRef) -> None:
        async with self._locks.acquire(ref.key):
            await self._registry.delete(ref)

    async def get_actor(self, ref: ActorRef) -> ActorRecord:
        return await self._registry.get(ref)

    async def list_actors(self, namespace: str) -> list[ActorRecord]:
        return await self._registry.list(namespace)

    # ── resume ───────────────────────────────────────────────────

    async def resume_actor(
        self,
        ref: ActorRef,
        *,
        prefer_node: str | None = None,
        timeout: float | None = None,
    ) -> ActorRecord:
        """SUSPENDED → RESUMING → RUNNING: borrow a worker, restore a snapshot."""
        async with self._locks.acquire(ref.key):
            record = await self._registry.get(ref)
            if record.status != STATUS_SUSPENDED:
                raise IllegalTransition(
                    f"actor {ref.key} is {record.status}, cannot resume",
                )

            snapshot = record.last_snapshot
            if snapshot is None:
                template = await self._templates.get(record.template)
                snapshot = template.golden_snapshot
            if snapshot is None:
                raise RuntimeError(
                    f"no snapshot to resume actor {ref.key} from "
                    "(template has no golden snapshot)",
                )

            # Pause locality: prefer the node the pause snapshot lives on.
            hint = prefer_node
            if snapshot.kind == KIND_PAUSE and snapshot.node:
                hint = snapshot.node or hint

            record = await self._set_status(
                record, STATUS_RESUMING, worker_id=None,
            )

            worker = None
            try:
                worker = await self._pool.acquire(
                    record.constraints, ref, prefer_node=hint, timeout=timeout,
                )
                try:
                    await self._checkpoint.resume(ref, worker, snapshot)
                except PauseSnapshotNotLocal:
                    # Landed on a different node than the pause snapshot;
                    # fall back to the durable golden snapshot.
                    template = await self._templates.get(record.template)
                    if template.golden_snapshot is None:
                        raise
                    logger.info(
                        "resume %s: pause not local on %s, falling back "
                        "to golden snapshot", ref.key, worker.node,
                    )
                    await self._checkpoint.resume(
                        ref, worker, template.golden_snapshot,
                    )
                record = await self._set_status(
                    record, STATUS_RUNNING, worker_id=worker.worker_id,
                )
                return record
            except Exception:
                # Roll back to SUSPENDED and release the worker (if any)
                # as broken — its state is now indeterminate.
                if worker is not None:
                    await self._pool.release(worker, broken=True)
                try:
                    current = await self._registry.get(ref)
                    if current.status == STATUS_RESUMING:
                        await self._set_status(
                            current, STATUS_SUSPENDED, worker_id=None,
                        )
                except Exception:
                    logger.exception(
                        "resume %s: failed to roll back to SUSPENDED",
                        ref.key,
                    )
                raise

    # ── suspend / pause ──────────────────────────────────────────

    async def suspend_actor(
        self,
        ref: ActorRef,
        *,
        scope: str | None = None,
    ) -> ActorRecord:
        """RUNNING → SUSPENDING → SUSPENDED: capture to durable store, release worker."""
        return await self._teardown(ref, pause=False, scope=scope)

    async def pause_actor(
        self,
        ref: ActorRef,
        *,
        scope: str | None = None,
    ) -> ActorRecord:
        """RUNNING → SUSPENDING → SUSPENDED: capture node-locally, release worker."""
        return await self._teardown(ref, pause=True, scope=scope)

    async def _teardown(
        self,
        ref: ActorRef,
        *,
        pause: bool,
        scope: str | None,
    ) -> ActorRecord:
        async with self._locks.acquire(ref.key):
            record = await self._registry.get(ref)
            if record.status != STATUS_RUNNING:
                raise IllegalTransition(
                    f"actor {ref.key} is {record.status}, cannot "
                    f"{'pause' if pause else 'suspend'}",
                )
            record = await self._set_status(record, STATUS_SUSPENDING)

            workers = await self._pool.workers()
            worker = next(
                (w for w in workers if w.worker_id == record.worker_id),
                None,
            )
            if worker is None:
                raise RuntimeError(
                    f"actor {ref.key} references missing worker "
                    f"{record.worker_id!r}",
                )

            try:
                if pause:
                    snap = await self._checkpoint.pause(ref, worker, scope=scope)
                else:
                    snap = await self._checkpoint.suspend(ref, worker, scope=scope)
            except Exception:
                # Leave the worker assigned and the actor SUSPENDING so
                # an operator can inspect; re-raise.
                raise

            await self._pool.release(worker)
            record = await self._set_status(
                record,
                STATUS_SUSPENDED,
                worker_id=None,
                last_snapshot=snap,
            )
            return record

    # ── helpers ──────────────────────────────────────────────────

    async def _set_status(
        self,
        record: ActorRecord,
        status: str,
        *,
        worker_id: str | None | None = None,
        last_snapshot=None,
    ) -> ActorRecord:
        """Transition *record* to *status* with optimistic-version retry.

        A concurrent writer (unlikely under the keyed lock, but possible
        if a side-channel mutates the record) bumps the version; we retry
        a bounded number of times rather than failing the workflow.
        """
        updated = ActorRecord(
            ref=record.ref,
            template=record.template,
            constraints=record.constraints,
            status=status,
            version=record.version,
            worker_id=record.worker_id if worker_id is None and status != STATUS_SUSPENDED else (worker_id if worker_id is not None else record.worker_id),
            last_snapshot=last_snapshot if last_snapshot is not None else record.last_snapshot,
            tags=record.tags,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
        # ``worker_id is None`` is a valid "clear" signal only on
        # SUSPENDED; otherwise None means "leave unchanged".
        if worker_id is None and status != STATUS_SUSPENDED:
            updated.worker_id = record.worker_id
        elif worker_id is not None:
            updated.worker_id = worker_id
        else:
            updated.worker_id = None  # SUSPENDED + None ⇒ clear

        for _ in range(4):
            try:
                return await self._registry.update(
                    updated, expected_version=record.version,
                )
            except VersionConflict:
                current = await self._registry.get(record.ref)
                # Preserve the intent against the freshest version.
                updated.version = current.version
                record = current
        raise VersionConflict(
            f"actor {record.ref.key}: lost too many version races",
        )


__all__ = ["ActorLifecycle", "IllegalTransition"]
