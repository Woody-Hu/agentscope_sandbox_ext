# -*- coding: utf-8 -*-
"""State stores for actors and workers, with optimistic concurrency.

The orchestration layer keeps actor and worker state in two stores so
the hot path (assignment, suspend/resume) does not touch the
sandbox-provisioning layer.  Both stores expose an **optimistic
concurrency** contract keyed on the record's ``version`` field: every
mutating call takes ``expected_version`` and raises
:class:`VersionConflict` if the stored version no longer matches.

This is the in-process analogue of the reference runtime's Redis
``WATCH/MULTI`` CAS on worker assignment — a contended ``claim()`` on
the same worker sees exactly one winner and N−1 ``VersionConflict``
losers, which the orchestrator retries with a fresh candidate.

The ABCs (:class:`ActorStore`, :class:`WorkerStore`) are the
pluggability seam: a Redis-backed store can drop in for multi-node
deployments without changing the orchestrator.
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any

from agentscope._logging import logger

from .model import (
    Actor,
    ActorRef,
    ActorStatus,
    Constraints,
    SandboxClass,
    Worker,
    WorkerAssignment,
    WorkerState,
)


class VersionConflict(Exception):
    """Raised when a CAS update's ``expected_version`` does not match.

    The caller should re-read the record and retry.  Carries the
    current stored version so the caller can decide whether to
    re-attempt or give up.
    """

    def __init__(self, what: str, expected: int, actual: int) -> None:
        super().__init__(
            f"{what}: expected version {expected}, but stored version is {actual}"
        )
        self.expected = expected
        self.actual = actual


class ActorNotFound(KeyError):
    """Raised by :class:`ActorStore.get` when the actor does not exist."""


class WorkerNotFound(KeyError):
    """Raised by :class:`WorkerStore.get` when the worker does not exist."""


# ── ActorStore ──────────────────────────────────────────────────


class ActorStore(ABC):
    """Abstract store for :class:`Actor` records with optimistic CAS.

    Actor identity is the composite ``(namespace, actor_id)`` — the same
    ``actor_id`` in two namespaces is two distinct actors (mirrors the
    :class:`ActorRef` identity).  ``put`` derives the key from the
    actor record; ``get`` / ``delete`` take both parts.
    """

    @abstractmethod
    async def get(
        self, actor_id: str, *, namespace: str = "default"
    ) -> Actor:
        """Return the actor, raising :class:`ActorNotFound` if absent."""

    @abstractmethod
    async def put(
        self,
        actor: Actor,
        *,
        expected_version: int | None = None,
    ) -> Actor:
        """Insert or update an actor.

        Args:
            actor: The actor record to store.  Its ``(namespace,
                actor_id)`` pair is the composite key.
            expected_version: If ``None``, this is a create (the store
                rejects if the actor already exists).  Otherwise the
                stored version must match or :class:`VersionConflict`
                is raised.

        Returns:
            The stored actor with a bumped ``version`` and refreshed
            ``updated_at``.
        """

    @abstractmethod
    async def delete(
        self, actor_id: str, *, namespace: str = "default"
    ) -> None:
        """Delete an actor.  No-op if absent."""

    @abstractmethod
    async def list(self, namespace: str | None = None) -> list[Actor]:
        """List actors, optionally filtered by namespace."""


class InMemoryActorStore(ActorStore):
    """In-process :class:`ActorStore` backed by a dict + asyncio lock.

    Sufficient for single-node deployments and tests.  A Redis-backed
    store can implement the same contract for multi-node fan-out.

    The dict is keyed on the composite ``(namespace, actor_id)`` tuple
    so the same ``actor_id`` in two namespaces is two distinct records.
    """

    def __init__(self) -> None:
        self._actors: dict[tuple[str, str], Actor] = {}
        self._lock = asyncio.Lock()

    async def get(
        self, actor_id: str, *, namespace: str = "default"
    ) -> Actor:
        async with self._lock:
            actor = self._actors.get((namespace, actor_id))
        if actor is None:
            raise ActorNotFound(f"{namespace}/{actor_id}")
        # Return a shallow copy so callers cannot mutate the stored
        # record without going through put().
        return _copy_actor(actor)

    async def put(
        self,
        actor: Actor,
        *,
        expected_version: int | None = None,
    ) -> Actor:
        key = (actor.namespace, actor.actor_id)
        async with self._lock:
            existing = self._actors.get(key)
            if expected_version is None:
                if existing is not None:
                    raise VersionConflict(
                        f"actor {actor.namespace}/{actor.actor_id!r}",
                        expected=-1,
                        actual=existing.version,
                    )
                new_version = 1
            else:
                if existing is None:
                    raise ActorNotFound(f"{actor.namespace}/{actor.actor_id}")
                if existing.version != expected_version:
                    raise VersionConflict(
                        f"actor {actor.namespace}/{actor.actor_id!r}",
                        expected=expected_version,
                        actual=existing.version,
                    )
                new_version = existing.version + 1
            stored = _copy_actor(actor)
            stored.version = new_version
            now = time.time()
            if stored.created_at == 0.0:
                stored.created_at = now
            stored.updated_at = now
            self._actors[key] = stored
            return _copy_actor(stored)

    async def delete(
        self, actor_id: str, *, namespace: str = "default"
    ) -> None:
        async with self._lock:
            self._actors.pop((namespace, actor_id), None)

    async def list(self, namespace: str | None = None) -> list[Actor]:
        async with self._lock:
            return [
                _copy_actor(a)
                for a in self._actors.values()
                if namespace is None or a.namespace == namespace
            ]


# ── WorkerStore ─────────────────────────────────────────────────


class WorkerStore(ABC):
    """Abstract store for :class:`Worker` records with optimistic CAS."""

    @abstractmethod
    async def get(self, worker_id: str) -> Worker:
        """Return the worker, raising :class:`WorkerNotFound` if absent."""

    @abstractmethod
    async def register(self, worker: Worker) -> Worker:
        """Insert a new worker.  Raises if already present."""

    @abstractmethod
    async def claim(
        self,
        worker_id: str,
        assignment: WorkerAssignment,
        *,
        expected_version: int,
    ) -> Worker:
        """CAS transition: ``assignment: None → assignment``.

        Atomically: the stored version must match ``expected_version``,
        the worker must be :attr:`WorkerState.ACTIVE`, and
        ``assignment`` must currently be ``None``.  On any mismatch
        raises :class:`VersionConflict` (version) or
        :class:`WorkerBusy` (state/assignment).
        """

    @abstractmethod
    async def release(
        self,
        worker_id: str,
        *,
        expected_version: int,
    ) -> Worker:
        """CAS transition: ``assignment → None`` (the inverse of claim)."""

    @abstractmethod
    async def set_state(
        self,
        worker_id: str,
        state: WorkerState,
        *,
        expected_version: int,
    ) -> Worker:
        """CAS transition of :attr:`Worker.state`."""

    @abstractmethod
    async def list_idle(self, constraints: Constraints) -> list[Worker]:
        """Idle workers matching *constraints* (eligible for assignment)."""

    @abstractmethod
    async def delete(self, worker_id: str) -> None:
        """Remove a worker record (after teardown)."""


class WorkerBusy(Exception):
    """Raised by :meth:`WorkerStore.claim` when the worker is not idle."""


class InMemoryWorkerStore(WorkerStore):
    """In-process :class:`WorkerStore` backed by a dict + asyncio lock.

    The :meth:`claim` / :meth:`release` / :meth:`set_state` transitions
    are performed atomically under the lock so concurrent claims on the
    same worker are serialised — exactly one succeeds, the rest raise
    :class:`VersionConflict`.  This is the contention behaviour the
    orchestrator's scheduler relies on to assign workers without
    double-booking.
    """

    def __init__(self) -> None:
        self._workers: dict[str, Worker] = {}
        self._lock = asyncio.Lock()

    async def get(self, worker_id: str) -> Worker:
        async with self._lock:
            worker = self._workers.get(worker_id)
        if worker is None:
            raise WorkerNotFound(worker_id)
        return _copy_worker(worker)

    async def register(self, worker: Worker) -> Worker:
        async with self._lock:
            if worker.worker_id in self._workers:
                raise VersionConflict(
                    f"worker {worker.worker_id!r} already registered",
                    expected=-1,
                    actual=self._workers[worker.worker_id].version,
                )
            stored = _copy_worker(worker)
            self._workers[worker.worker_id] = stored
            return _copy_worker(stored)

    async def claim(
        self,
        worker_id: str,
        assignment: WorkerAssignment,
        *,
        expected_version: int,
    ) -> Worker:
        async with self._lock:
            existing = self._workers.get(worker_id)
            if existing is None:
                raise WorkerNotFound(worker_id)
            if existing.version != expected_version:
                raise VersionConflict(
                    f"worker {worker_id!r}",
                    expected=expected_version,
                    actual=existing.version,
                )
            if existing.state != WorkerState.ACTIVE:
                raise WorkerBusy(
                    f"worker {worker_id!r} is {existing.state.value}, "
                    f"cannot claim"
                )
            if existing.assignment is not None:
                raise WorkerBusy(
                    f"worker {worker_id!r} already hosts actor "
                    f"{existing.assignment.actor_id!r}"
                )
            stored = _copy_worker(existing)
            stored.assignment = assignment
            stored.version = existing.version + 1
            self._workers[worker_id] = stored
            return _copy_worker(stored)

    async def release(
        self,
        worker_id: str,
        *,
        expected_version: int,
    ) -> Worker:
        async with self._lock:
            existing = self._workers.get(worker_id)
            if existing is None:
                raise WorkerNotFound(worker_id)
            if existing.version != expected_version:
                raise VersionConflict(
                    f"worker {worker_id!r}",
                    expected=expected_version,
                    actual=existing.version,
                )
            stored = _copy_worker(existing)
            stored.assignment = None
            stored.version = existing.version + 1
            self._workers[worker_id] = stored
            return _copy_worker(stored)

    async def set_state(
        self,
        worker_id: str,
        state: WorkerState,
        *,
        expected_version: int,
    ) -> Worker:
        async with self._lock:
            existing = self._workers.get(worker_id)
            if existing is None:
                raise WorkerNotFound(worker_id)
            if existing.version != expected_version:
                raise VersionConflict(
                    f"worker {worker_id!r}",
                    expected=expected_version,
                    actual=existing.version,
                )
            stored = _copy_worker(existing)
            stored.state = state
            stored.version = existing.version + 1
            self._workers[worker_id] = stored
            return _copy_worker(stored)

    async def list_idle(self, constraints: Constraints) -> list[Worker]:
        async with self._lock:
            idle = [
                _copy_worker(w)
                for w in self._workers.values()
                if w.is_idle
            ]
        return [w for w in idle if _matches(w, constraints)]

    async def delete(self, worker_id: str) -> None:
        async with self._lock:
            self._workers.pop(worker_id, None)


# ── helpers ─────────────────────────────────────────────────────


def _matches(worker: Worker, constraints: Constraints) -> bool:
    """True if *worker* satisfies *constraints* (all fields AND'd)."""
    if worker.sandbox_class != constraints.sandbox_class:
        return False
    if constraints.required_node is not None:
        if worker.node != constraints.required_node:
            return False
    for k, v in constraints.template_selector.items():
        if worker.labels.get(k) != v:
            return False
    # actor_selector filters on the worker's current assignment — but
    # idle workers have no assignment, so it only applies to the labels
    # we mirror onto the worker at acquire time.  Treat it as a label
    # selector for idle workers too.
    for k, v in constraints.actor_selector.items():
        if worker.labels.get(k) != v:
            return False
    return True


def _copy_actor(actor: Actor) -> Actor:
    return Actor(
        actor_id=actor.actor_id,
        namespace=actor.namespace,
        template_id=actor.template_id,
        status=actor.status,
        version=actor.version,
        worker_assignment=actor.worker_assignment,
        latest_snapshot_ref=actor.latest_snapshot_ref,
        worker_selector=dict(actor.worker_selector),
        created_at=actor.created_at,
        updated_at=actor.updated_at,
    )


def _copy_worker(worker: Worker) -> Worker:
    # NOTE: ``sandbox`` is shared by reference — it is the live
    # SandboxedWorkspaceExtBase instance and must not be copied.
    return Worker(
        worker_id=worker.worker_id,
        state=worker.state,
        sandbox_class=worker.sandbox_class,
        version=worker.version,
        assignment=worker.assignment,
        labels=dict(worker.labels),
        node=worker.node,
        sandbox=worker.sandbox,
    )


__all__ = [
    "VersionConflict",
    "ActorNotFound",
    "WorkerNotFound",
    "WorkerBusy",
    "ActorStore",
    "InMemoryActorStore",
    "WorkerStore",
    "InMemoryWorkerStore",
]
