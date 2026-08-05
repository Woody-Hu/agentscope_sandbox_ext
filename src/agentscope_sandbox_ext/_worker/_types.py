# -*- coding: utf-8 -*-
"""Worker-side data model: ``Worker``, ``WorkerRecord``, ``SandboxRuntime``.

A *worker* is the physical execution unit — a pre-warmed sandbox that
hosts **at most one actor at a time**.  Many actors time-multiplex a
smaller pool of workers; this is the density win of the virtual-actor
model.

The ``SandboxRuntime`` protocol abstracts the concrete sandbox backend
so the worker pool / scheduler never depend on a specific backend
(Firecracker / gVisor / VFS / ...).  A :class:`WorkspaceSandboxRuntime`
adapter wraps the existing :class:`SandboxedWorkspaceExtBase` backends
into this protocol; tests can supply a direct in-process runtime.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from .._actor._types import (
    WORKER_ACTIVE,
    WORKER_BUSY,
    ActorRef,
    SandboxClass,
)


@runtime_checkable
class SandboxRuntime(Protocol):
    """Uniform surface over any sandbox backend.

    The worker pool drives a runtime through ``provision`` /
    ``snapshot`` / ``restore`` / ``close``.  ``snapshot``/``restore``
    use opaque string *tags*; the runtime decides what the tag resolves
    to (a deep-copy dir, a Firecracker memfile pair, ...).

    A runtime also exposes its :attr:`sandbox_class` (a hard scheduling
    constraint) and the :attr:`node` it runs on (for pause locality).
    """

    worker_id: str
    sandbox_class: SandboxClass
    node: str

    @property
    def is_alive(self) -> bool: ...

    async def provision(self) -> None: ...

    async def snapshot(self, tag: str) -> str:
        """Capture live state under *tag*; return the local artifact path."""
        ...

    async def restore(self, tag: str) -> None:
        """Restore live state from a locally-staged snapshot *tag*."""
        ...

    async def stage(self, tag: str, source_path: str) -> None:
        """Stage an external snapshot tree (*source_path*) into local slot *tag*.

        Used by the checkpoint manager to materialise a durable snapshot
        (downloaded from the :class:`SnapshotStore`) so a subsequent
        :meth:`restore` of *tag* can activate it on this worker.
        """
        ...

    async def close(self) -> None: ...


@dataclass(eq=False)
class Worker:
    """A pooled execution unit wrapping a :class:`SandboxRuntime`.

    The worker pool tracks ``status`` and ``assigned_actor``; the
    invariant is that a ``BUSY`` worker has exactly one ``assigned_actor``
    and an ``ACTIVE`` worker has none.

    ``eq=False`` gives identity-based equality / hashing so workers can
    be stored in ``set[Worker]`` and looked up by instance identity
    (the pool never needs value-based equality — two distinct workers
    with the same fields are still different workers).

    Attributes:
        worker_id: Stable unique id.
        runtime: The wrapped sandbox runtime.
        pool: Name of the owning worker pool.
        labels: Free-form labels matched by scheduling selectors.
        status: ``ACTIVE`` / ``BUSY`` / ``DRAINING``.
        assigned_actor: The actor currently hosted, or ``None``.
    """

    worker_id: str
    runtime: SandboxRuntime
    pool: str = "default"
    labels: dict[str, str] = field(default_factory=dict)
    status: str = WORKER_ACTIVE
    assigned_actor: ActorRef | None = None

    @property
    def sandbox_class(self) -> SandboxClass:
        return self.runtime.sandbox_class

    @property
    def node(self) -> str:
        return self.runtime.node

    @property
    def is_alive(self) -> bool:
        return self.runtime.is_alive

    def to_record(self) -> "WorkerRecord":
        return WorkerRecord(
            worker_id=self.worker_id,
            pool=self.pool,
            sandbox_class=self.sandbox_class,
            status=self.status,
            node=self.node,
            labels=dict(self.labels),
            assigned_actor=self.assigned_actor,
        )


@dataclass
class WorkerRecord:
    """Serialisable snapshot of a worker's state (control-plane view)."""

    worker_id: str
    pool: str
    sandbox_class: SandboxClass
    status: str
    node: str
    labels: dict[str, str]
    assigned_actor: ActorRef | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "pool": self.pool,
            "sandbox_class": self.sandbox_class.to_dict(),
            "status": self.status,
            "node": self.node,
            "labels": dict(self.labels),
            "assigned_actor": (
                self.assigned_actor.to_dict()
                if self.assigned_actor is not None
                else None
            ),
        }


class WorkerFactory(abc.ABC):
    """Produces fresh, provisioned :class:`Worker` instances for the pool.

    Concrete factories bind a backend (Firecracker / gVisor / VFS / ...)
    plus pool-wide settings (node name, labels) and return a worker whose
    runtime is already provisioned and alive.  The pool calls this to
    grow the warm pool; the factory may use :class:`Singleflight`
    internally to dedup identical provisions.
    """

    @abc.abstractmethod
    async def __call__(self) -> Worker:
        """Provision and return a fresh, alive worker."""


__all__ = [
    "SandboxRuntime",
    "Worker",
    "WorkerRecord",
    "WorkerFactory",
    "WORKER_ACTIVE",
    "WORKER_BUSY",
]
