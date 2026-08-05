# -*- coding: utf-8 -*-
"""Core data model for the actor / worker / template / checkpoint layering.

These types are the *lingua franca* shared across the modular runtime.
They are deliberately transport-agnostic: every dataclass exposes
``to_dict`` / ``from_dict`` so the same model serialises to JSON (HTTP)
or protobuf (gRPC) at the control-plane boundary.

Design notes
------------
* Identity is ``(namespace, name)`` — a global addressing scheme where
  ``namespace`` is an isolation boundary (distinct from any container /
  K8s namespace).  This mirrors the reference runtime's separation of
  "who an actor is" from "where it runs".
* ``SandboxClass`` is a first-class scheduling constraint: snapshots
  never cross classes, and a worker's class must match an actor's.
* ``ActorRecord.version`` drives optimistic concurrency: every mutation
  is conditional on the expected version; a stale writer gets
  :class:`VersionConflict` instead of a silent overwrite.
* Snapshot refs carry a ``kind`` (golden / last / pause) and ``scope``
  (full / data) so the checkpoint manager can pick the right restore
  source without re-encoding policy at every call site.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ── sandbox class ────────────────────────────────────────────────────

#: Allowed sandbox-class identifiers.  These line up 1:1 with the
#: existing ``SandboxedWorkspaceExtBase.sandbox_kind`` discriminators so
#: a worker's class is just the wrapped backend's kind.
SANDBOX_CLASSES: tuple[str, ...] = (
    "firecracker",
    "gvisor",
    "kata",
    "sysbox",
    "vfs",
)


@dataclass(frozen=True)
class SandboxClass:
    """A sandbox runtime class — a hard scheduling constraint.

    Snapshots are class-scoped: a snapshot taken on a ``gvisor`` worker
    cannot be restored on a ``firecracker`` worker.  The scheduler
    therefore never relaxes the class constraint.

    Attributes:
        value: One of :data:`SANDBOX_CLASSES`.
    """

    value: str

    def __post_init__(self) -> None:
        if self.value not in SANDBOX_CLASSES:
            raise ValueError(
                f"Unknown sandbox class {self.value!r}; "
                f"expected one of {SANDBOX_CLASSES}",
            )

    @classmethod
    def of(cls, value: str) -> "SandboxClass":
        return cls(value)

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SandboxClass":
        return cls(d["value"])


# ── identity ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ActorRef:
    """Identity of a logical actor instance.

    Attributes:
        namespace: Isolation boundary.  Two actors with the same
            ``name`` in different namespaces are independent.
        name: Per-namespace unique actor name.
    """

    namespace: str
    name: str

    def __post_init__(self) -> None:
        if not self.namespace or not self.name:
            raise ValueError("namespace and name must be non-empty")

    @property
    def key(self) -> str:
        """Stable string key used for locks / singleflight / registry."""
        return f"{self.namespace}/{self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {"namespace": self.namespace, "name": self.name}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ActorRef":
        return cls(d["namespace"], d["name"])


@dataclass(frozen=True)
class TemplateRef:
    """Identity of an (immutable) actor template version.

    Attributes:
        name: Template name.
        version: Monotonic version.  Bumping any spec field requires a
            new version (and thus a new template record + golden
            snapshot); templates are never mutated in place.
    """

    name: str
    version: int

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("template name must be non-empty")
        if self.version < 1:
            raise ValueError("template version must be >= 1")

    @property
    def key(self) -> str:
        return f"{self.name}@{self.version}"

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "version": self.version}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TemplateRef":
        return cls(d["name"], int(d["version"]))


# ── scheduling constraints ──────────────────────────────────────────


@dataclass(frozen=True)
class Constraints:
    """Scheduling constraints for placing an actor onto a worker.

    Attributes:
        sandbox_class: Hard constraint — worker class must equal this.
        template_selector: Worker labels that must match (subset).
        actor_selector: Worker labels that must match (subset).
        required_nodes: When non-empty, the worker's ``node`` must be in
            this set.  Used for *pause locality*: a paused snapshot
            lives on a node, so the next resume is pinned to that node
            to avoid a remote fetch.
    """

    sandbox_class: SandboxClass
    template_selector: dict[str, str] = field(default_factory=dict)
    actor_selector: dict[str, str] = field(default_factory=dict)
    required_nodes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "sandbox_class": self.sandbox_class.to_dict(),
            "template_selector": dict(self.template_selector),
            "actor_selector": dict(self.actor_selector),
            "required_nodes": list(self.required_nodes),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Constraints":
        return cls(
            sandbox_class=SandboxClass.from_dict(d["sandbox_class"]),
            template_selector=dict(d.get("template_selector", {})),
            actor_selector=dict(d.get("actor_selector", {})),
            required_nodes=tuple(d.get("required_nodes", [])),
        )


# ── snapshots ───────────────────────────────────────────────────────


#: Snapshot scope: how much state is captured.
SCOPE_FULL = "full"  # rootfs delta + durable (+ mem where supported)
SCOPE_DATA = "data"  # durable dir only; resume cold-boots + restores data
SNAPSHOT_SCOPES: tuple[str, ...] = (SCOPE_FULL, SCOPE_DATA)

#: Snapshot kind: provenance / lifecycle role.
KIND_GOLDEN = "golden"  # per-template, shared, immutable
KIND_LAST = "last"  # per-actor, overwritten on each suspend
KIND_PAUSE = "pause"  # node-local, short-lived, drives locality
SNAPSHOT_KINDS: tuple[str, ...] = (KIND_GOLDEN, KIND_LAST, KIND_PAUSE)


@dataclass(frozen=True)
class ActorSnapshotRef:
    """Reference to a stored snapshot.

    Attributes:
        snapshot_id: Opaque store-specific reference (content-addressed
            for golden, unified-ref for last/pause).
        kind: One of :data:`SNAPSHOT_KINDS`.
        scope: One of :data:`SNAPSHOT_SCOPES`.
        node: For ``pause`` snapshots, the node the artifact lives on.
            ``None`` for golden / last (durable, location-independent).
    """

    snapshot_id: str
    kind: str
    scope: str
    node: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in SNAPSHOT_KINDS:
            raise ValueError(f"bad snapshot kind {self.kind!r}")
        if self.scope not in SNAPSHOT_SCOPES:
            raise ValueError(f"bad snapshot scope {self.scope!r}")
        if self.kind == KIND_PAUSE and not self.node:
            raise ValueError("pause snapshots require a node")

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "kind": self.kind,
            "scope": self.scope,
            "node": self.node,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ActorSnapshotRef":
        return cls(
            snapshot_id=d["snapshot_id"],
            kind=d["kind"],
            scope=d["scope"],
            node=d.get("node"),
        )


# ── actor / worker / template records ───────────────────────────────


#: Actor lifecycle states.
STATUS_SUSPENDED = "SUSPENDED"
STATUS_RESUMING = "RESUMING"
STATUS_RUNNING = "RUNNING"
STATUS_SUSPENDING = "SUSPENDING"
ACTOR_STATUSES: tuple[str, ...] = (
    STATUS_SUSPENDED,
    STATUS_RESUMING,
    STATUS_RUNNING,
    STATUS_SUSPENDING,
)

#: Worker states.
WORKER_ACTIVE = "ACTIVE"  # idle, available for assignment
WORKER_BUSY = "BUSY"  # hosting an actor
WORKER_DRAINING = "DRAINING"  # being torn down

#: Template bake phases.
PHASE_INITIAL = "Initial"
PHASE_BAKING = "Baking"
PHASE_READY = "Ready"
PHASE_FAILED = "Failed"


class VersionConflict(RuntimeError):
    """Raised when an optimistic-concurrency update sees a stale version."""


@dataclass
class ActorRecord:
    """Mutable record of an actor's lifecycle state.

    Stored in the :class:`ActorRegistry`; mutations go through
    ``update(expected_version=...)`` to enforce optimistic concurrency.
    """

    ref: ActorRef
    template: TemplateRef
    constraints: Constraints
    status: str = STATUS_SUSPENDED
    version: int = 1
    worker_id: str | None = None
    last_snapshot: ActorSnapshotRef | None = None
    tags: dict[str, str] = field(default_factory=dict)
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ref": self.ref.to_dict(),
            "template": self.template.to_dict(),
            "constraints": self.constraints.to_dict(),
            "status": self.status,
            "version": self.version,
            "worker_id": self.worker_id,
            "last_snapshot": (
                self.last_snapshot.to_dict()
                if self.last_snapshot is not None
                else None
            ),
            "tags": dict(self.tags),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ActorRecord":
        last = d.get("last_snapshot")
        return cls(
            ref=ActorRef.from_dict(d["ref"]),
            template=TemplateRef.from_dict(d["template"]),
            constraints=Constraints.from_dict(d["constraints"]),
            status=d["status"],
            version=int(d["version"]),
            worker_id=d.get("worker_id"),
            last_snapshot=(
                ActorSnapshotRef.from_dict(last) if last else None
            ),
            tags=dict(d.get("tags", {})),
            created_at=float(d.get("created_at", 0.0)),
            updated_at=float(d.get("updated_at", 0.0)),
        )


__all__ = [
    "SANDBOX_CLASSES",
    "SandboxClass",
    "ActorRef",
    "TemplateRef",
    "Constraints",
    "SCOPE_FULL",
    "SCOPE_DATA",
    "SNAPSHOT_SCOPES",
    "KIND_GOLDEN",
    "KIND_LAST",
    "KIND_PAUSE",
    "SNAPSHOT_KINDS",
    "ActorSnapshotRef",
    "STATUS_SUSPENDED",
    "STATUS_RESUMING",
    "STATUS_RUNNING",
    "STATUS_SUSPENDING",
    "ACTOR_STATUSES",
    "WORKER_ACTIVE",
    "WORKER_BUSY",
    "WORKER_DRAINING",
    "PHASE_INITIAL",
    "PHASE_BAKING",
    "PHASE_READY",
    "PHASE_FAILED",
    "VersionConflict",
    "ActorRecord",
]
