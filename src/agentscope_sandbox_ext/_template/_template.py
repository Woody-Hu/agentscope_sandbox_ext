# -*- coding: utf-8 -*-
"""Actor templates: immutable definitions + golden-snapshot baking.

An :class:`ActorTemplateRecord` is the immutable definition of a workload
class — the sandbox class, the backend provision spec, and the
pause/suspend scopes.  Bumping any field requires a new version (and thus
a new record + a new golden snapshot); templates are never mutated in
place except for the bake ``phase`` transition.

**Golden snapshot baking**: on creation, a template goes
``Initial → Baking → Ready`` (or ``Failed``).  The
:class:`TemplateBaker` provisions a *fresh, pristine* worker directly
from the factory (not borrowed from the free list, so it cannot be
contaminated by a prior actor), snapshots it, uploads the result to the
durable store as a ``golden`` snapshot, then closes the worker.  New
actors of that template clone-restore from the golden snapshot —
sub-second instead of a cold boot.
"""

from __future__ import annotations

import abc
import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

from agentscope._logging import logger

from .._actor._types import (
    PHASE_BAKING,
    PHASE_FAILED,
    PHASE_INITIAL,
    PHASE_READY,
    SCOPE_DATA,
    SCOPE_FULL,
    ActorSnapshotRef,
    SandboxClass,
    TemplateRef,
)
from .._checkpoint._manager import CheckpointManager
from .._worker._types import Worker, WorkerFactory


@dataclass
class ActorTemplateRecord:
    """Immutable template definition + bake state.

    Attributes:
        ref: ``(name, version)`` identity.
        sandbox_class: Hard scheduling constraint for actors of this template.
        spec: Backend-specific provision spec (image, env, ...).  Opaque
            to the runtime; consumed by the worker factory.
        golden_snapshot: The shared, immutable golden snapshot once baked.
        phase: ``Initial`` / ``Baking`` / ``Ready`` / ``Failed``.
        on_pause / on_suspend: Checkpoint scopes (``on_suspend ⊆ on_pause``).
    """

    ref: TemplateRef
    sandbox_class: SandboxClass
    spec: dict[str, Any] = field(default_factory=dict)
    golden_snapshot: ActorSnapshotRef | None = None
    phase: str = PHASE_INITIAL
    on_pause: str = SCOPE_FULL
    on_suspend: str = SCOPE_FULL
    created_at: float = 0.0
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        # on_suspend must be a subset of on_pause.
        if self.on_pause == SCOPE_DATA and self.on_suspend == SCOPE_FULL:
            raise ValueError(
                "on_suspend=full is not a subset of on_pause=data",
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ref": self.ref.to_dict(),
            "sandbox_class": self.sandbox_class.to_dict(),
            "spec": dict(self.spec),
            "golden_snapshot": (
                self.golden_snapshot.to_dict()
                if self.golden_snapshot is not None
                else None
            ),
            "phase": self.phase,
            "on_pause": self.on_pause,
            "on_suspend": self.on_suspend,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class TemplateRegistry(abc.ABC):
    """Store of :class:`ActorTemplateRecord`."""

    @abc.abstractmethod
    async def create(self, record: ActorTemplateRecord) -> ActorTemplateRecord:
        ...

    @abc.abstractmethod
    async def get(self, ref: TemplateRef) -> ActorTemplateRecord:
        ...

    @abc.abstractmethod
    async def list(self) -> list[ActorTemplateRecord]:
        ...

    @abc.abstractmethod
    async def update(
        self, record: ActorTemplateRecord
    ) -> ActorTemplateRecord:
        """Replace the stored record (used for phase transitions)."""
        ...


class InProcessTemplateRegistry(TemplateRegistry):
    """In-memory template registry."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._records: dict[str, ActorTemplateRecord] = {}

    async def create(self, record: ActorTemplateRecord) -> ActorTemplateRecord:
        async with self._lock:
            if record.ref.key in self._records:
                raise KeyError(f"template already exists: {record.ref.key}")
            now = time.time()
            record.created_at = now
            record.updated_at = now
            self._records[record.ref.key] = record
            return record

    async def get(self, ref: TemplateRef) -> ActorTemplateRecord:
        async with self._lock:
            rec = self._records.get(ref.key)
            if rec is None:
                raise KeyError(f"no such template: {ref.key}")
            return rec

    async def list(self) -> list[ActorTemplateRecord]:
        async with self._lock:
            return list(self._records.values())

    async def update(self, record: ActorTemplateRecord) -> ActorTemplateRecord:
        async with self._lock:
            existing = self._records.get(record.ref.key)
            if existing is None:
                raise KeyError(f"no such template: {record.ref.key}")
            record.created_at = existing.created_at
            record.updated_at = time.time()
            self._records[record.ref.key] = record
            return record


class TemplateBaker:
    """Bakes a template's golden snapshot from a fresh worker.

    Uses the worker :class:`WorkerFactory` directly (not the pool's free
    list) so the baked snapshot reflects a pristine provision, never a
    prior actor's state.  The worker is closed after baking.
    """

    def __init__(
        self,
        factory: WorkerFactory | Callable[[], Awaitable[Worker]],
        checkpoint: CheckpointManager,
        registry: TemplateRegistry,
    ) -> None:
        self._factory = factory
        self._checkpoint = checkpoint
        self._registry = registry

    async def bake(self, template: ActorTemplateRecord) -> ActorTemplateRecord:
        """Bake the golden snapshot and transition *template* to ``Ready``.

        Idempotent-ish: if the template is already ``Ready`` the existing
        golden snapshot is returned without re-baking.  A concurrent bake
        is guarded by the checkpoint manager's singleflight on the
        ``golden:<template>`` key.
        """
        if template.phase == PHASE_READY and template.golden_snapshot is not None:
            return template

        template.phase = PHASE_BAKING
        await self._registry.update(template)

        worker: Worker | None = None
        try:
            worker = await self._factory()  # type: ignore[misc]
            golden = await self._checkpoint.bake_golden(
                template.ref,
                worker,
                scope=template.on_pause,
            )
            template.golden_snapshot = golden
            template.phase = PHASE_READY
            return await self._registry.update(template)
        except Exception:
            template.phase = PHASE_FAILED
            await self._registry.update(template)
            raise
        finally:
            if worker is not None:
                try:
                    await worker.runtime.close()
                except Exception:
                    logger.exception(
                        "TemplateBaker: failed to close bake worker %s",
                        worker.worker_id,
                    )


__all__ = [
    "ActorTemplateRecord",
    "TemplateRegistry",
    "InProcessTemplateRegistry",
    "TemplateBaker",
]
