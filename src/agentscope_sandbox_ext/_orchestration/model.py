# -*- coding: utf-8 -*-
"""Core data models for the agent orchestration runtime.

This module defines the cheap, serialisable records the orchestration
layer operates on: :class:`Actor`, :class:`Worker`, the immutable
:class:`ActorTemplate`, plus the supporting enums and constraint
dataclasses.  None of these touch the network or the filesystem — they
are pure data so the orchestration layer can be reasoned about and
tested without a live sandbox.

The actor/worker split mirrors the reference runtime's "many actors onto
fewer workers, at most one actor per worker at a time" model.  See
``docs/ORCHESTRATION.md`` for the full design and the state machine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .._base import SandboxedWorkspaceExtBase


# ── enums ───────────────────────────────────────────────────────


class ActorStatus(str, Enum):
    """Lifecycle status of an :class:`Actor`.

    Transitions are CAS-guarded by :attr:`Actor.version` and enforced by
    the orchestrator.  See the state machine in ``docs/ORCHESTRATION.md``.
    """

    SUSPENDED = "suspended"      # idle; owns no worker, owns a snapshot
    RESUMING = "resuming"        # worker claimed, restore in flight
    RUNNING = "running"          # live on a worker
    SUSPENDING = "suspending"    # snapshot in flight, worker not yet freed
    PAUSING = "pausing"          # node-local snapshot in flight
    PAUSED = "paused"            # idle; snapshot is node-local (locality hint)
    CRASHED = "crashed"          # worker lost mid-activation; resume re-binds
    DELETING = "deleting"        # terminal; snapshot being garbage-collected


class WorkerState(str, Enum):
    """Lifecycle state of a :class:`Worker`."""

    ACTIVE = "active"            # accepts new actor claims
    DRAINING = "draining"        # rejecting new claims; will be torn down


class SandboxClass(str, Enum):
    """Isolation class of a worker.

    Snapshots are **not portable across classes** — a snapshot taken in a
    microVM is not restorable into a container worker — so the class is a
    hard scheduling gate, not a soft preference.
    """

    CONTAINER = "container"      # gVisor / Kata / Sysbox / Docker
    MICROVM = "microvm"          # Firecracker / Cloud Hypervisor
    VFS = "vfs"                  # agentfs (no isolation; dev / CI)


class CheckpointScope(str, Enum):
    """What a checkpoint captures.

    * ``FULL`` — memory + filesystem delta + durable dir.  Requires a
      backend with a real memory-snapshot primitive (VMM snapshot /
      runsc checkpoint); not portable into pure Python.  Backends
      without it degrade to ``DATA`` with a warning.
    * ``DATA`` — durable filesystem only; the guest is discarded, so
      resume is a cold-boot over the restored data.  This is the
      portable scope every backend can satisfy today.
    """

    FULL = "full"
    DATA = "data"


# ── value objects ───────────────────────────────────────────────


@dataclass(frozen=True)
class ActorRef:
    """Stable, hashable reference to an actor.

    Identity is ``(namespace, actor_id)`` so the same ``actor_id`` in two
    namespaces is two distinct actors — mirrors the reference runtime's
    "atespace" namespacing.
    """

    namespace: str
    actor_id: str

    def __str__(self) -> str:
        return f"{self.namespace}/{self.actor_id}"


@dataclass(frozen=True)
class WorkerAssignment:
    """A binding of an actor to a worker for one activation.

    Carries everything the routing / restore path needs to reach the
    live sandbox without re-reading the worker record.
    """

    actor_id: str
    worker_id: str
    sandbox_class: SandboxClass
    node: str | None = None
    address: str | None = None   # reachable address (host:port / unix path / vsock cid)
    activated_at: float = 0.0


@dataclass
class Constraints:
    """Scheduling constraints for acquiring a worker.

    All fields are AND'd together.  ``sandbox_class`` is a hard gate
    (snapshots are not portable across classes); the selectors are
    label matching; ``required_node`` enforces locality for paused
    snapshots that are node-local.
    """

    sandbox_class: SandboxClass
    template_selector: dict[str, str] = field(default_factory=dict)
    actor_selector: dict[str, str] = field(default_factory=dict)
    required_node: str | None = None


# ── core records ────────────────────────────────────────────────


@dataclass
class Actor:
    """A logical agent identity that can be suspended and resumed.

    An actor is cheap: while ``SUSPENDED`` it owns no compute, only a
    snapshot reference.  Resuming it binds it to one worker for the
    duration of an activation; suspending it snapshots and frees the
    worker again.

    The ``version`` field is the optimistic-concurrency token used by
    :class:`ActorStore.put` — every update must carry the version it
    read, and the store rejects updates whose expected version does not
    match.  This is the in-process analogue of the reference runtime's
    Redis ``WATCH/MULTI`` CAS.
    """

    actor_id: str
    namespace: str
    template_id: str
    status: ActorStatus
    version: int = 1
    worker_assignment: WorkerAssignment | None = None
    latest_snapshot_ref: str | None = None
    worker_selector: dict[str, str] = field(default_factory=dict)
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def ref(self) -> ActorRef:
        return ActorRef(self.namespace, self.actor_id)

    @property
    def is_active(self) -> bool:
        """True when the actor currently holds a worker."""
        return self.status in (ActorStatus.RUNNING, ActorStatus.RESUMING)


@dataclass
class Worker:
    """A sandbox slot that hosts at most one :class:`Actor` at a time.

    In-process, a worker *is* a :class:`SandboxedWorkspaceExtBase`
    instance plus the bookkeeping the orchestrator needs around it
    (assignment, state, labels, locality).  The sandbox itself is the
    expensive part; the worker record is cheap.

    Invariant: ``assignment is not None`` iff the worker is currently
    hosting an actor.  ``claim()`` is the CAS transition
    ``assignment: None → WorkerAssignment``; ``release()`` is the
    inverse.  A ``DRAINING`` worker rejects new claims.
    """

    worker_id: str
    state: WorkerState
    sandbox_class: SandboxClass
    version: int = 1
    assignment: WorkerAssignment | None = None
    labels: dict[str, str] = field(default_factory=dict)
    node: str | None = None
    #: The live sandbox.  ``None`` when the worker record exists but the
    #: sandbox has not been materialised yet (or has been torn down).
    sandbox: Any = None  # SandboxedWorkspaceExtBase | None

    @property
    def is_idle(self) -> bool:
        """True when the worker can accept a new actor claim."""
        return (
            self.state == WorkerState.ACTIVE
            and self.assignment is None
            and self.sandbox is not None
        )


@dataclass(frozen=True)
class ActorTemplate:
    """Immutable provision spec for a class of actors.

    Immutable like the reference runtime's ``ActorTemplate`` CRD: a new
    image / config produces a new ``template_id``.  This is required
    because **changing the image invalidates snapshots** — a snapshot
    taken from template v1 is not restorable into a v2 worker.

    The ``golden_snapshot_ref`` is the one-time "known-good" snapshot of
    a fresh materialisation.  The first actor of a template cold-boots
    and (if the backend supports snapshots) captures a golden snapshot;
    subsequent actors of the same template ``restore("golden")`` instead
    of cold-booting — boot becomes a restore, not a cold start.

    ``golden_snapshot_ref`` is mutable in practice (set once after the
    first materialisation) but the template's *spec* is immutable; we
    model that by keeping the spec fields on the frozen dataclass and
    tracking the golden ref out-of-band in the orchestrator's template
    registry.
    """

    template_id: str
    sandbox_class: SandboxClass
    backend_kind: str              # "firecracker" | "gvisor" | "kata" | "sysbox" | "agentfs"
    provision_config: dict[str, Any] = field(default_factory=dict)
    skill_paths: list[str] = field(default_factory=list)
    default_mcps: list[Any] = field(default_factory=list)
    snapshot_scope_on_suspend: CheckpointScope = CheckpointScope.DATA
    snapshot_scope_on_pause: CheckpointScope = CheckpointScope.DATA
    #: One-time golden snapshot of a fresh materialisation.  ``None``
    #: until the first actor of this template has booted and been
    #: snapshotted; thereafter restored by every new actor of the
    #: template instead of cold-booting.
    golden_snapshot_ref: str | None = None
    version: int = 1


__all__ = [
    "ActorStatus",
    "WorkerState",
    "SandboxClass",
    "CheckpointScope",
    "ActorRef",
    "WorkerAssignment",
    "Constraints",
    "Actor",
    "Worker",
    "ActorTemplate",
]
