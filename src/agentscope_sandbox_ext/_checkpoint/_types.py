# -*- coding: utf-8 -*-
"""Checkpoint-layer types and re-exports.

The snapshot kind/scope enums live in :mod:`._actor._types` (they are
shared across the control-plane data model); this module re-exports them
for checkpoint-layer callers and adds the :class:`CheckpointConfig`
that binds a durable :class:`SnapshotStore` to the pause/suspend policy.
"""

from __future__ import annotations

from dataclasses import dataclass

from .._actor._types import (
    KIND_GOLDEN,
    KIND_LAST,
    KIND_PAUSE,
    SCOPE_DATA,
    SCOPE_FULL,
    SNAPSHOT_KINDS,
    SNAPSHOT_SCOPES,
    ActorSnapshotRef,
)
from .._runtime.snapshot_store import SnapshotStore


@dataclass(frozen=True)
class CheckpointConfig:
    """Per-template checkpoint policy.

    Mirrors the reference runtime's ``SnapshotsConfig``: a durable
    :class:`SnapshotStore` location plus the scopes captured on pause
    (node-local) and on suspend (remote upload).  The invariant
    ``on_suspend ⊆ on_pause`` is enforced at construction.

    Attributes:
        durable_store: Store used for ``last`` / ``golden`` snapshots.
        on_pause: Scope captured when an actor pauses (node-local).
        on_suspend: Scope captured when an actor suspends (remote).
    """

    durable_store: SnapshotStore
    on_pause: str = SCOPE_FULL
    on_suspend: str = SCOPE_FULL

    def __post_init__(self) -> None:
        if self.on_pause not in SNAPSHOT_SCOPES:
            raise ValueError(f"bad on_pause scope {self.on_pause!r}")
        if self.on_suspend not in SNAPSHOT_SCOPES:
            raise ValueError(f"bad on_suspend scope {self.on_suspend!r}")
        # on_suspend must be a subset of on_pause: a suspend cannot
        # capture more than the preceding pause promised.
        if self.on_pause == SCOPE_DATA and self.on_suspend == SCOPE_FULL:
            raise ValueError(
                "on_suspend=full is not a subset of on_pause=data",
            )


class PauseSnapshotNotLocal(RuntimeError):
    """A pause snapshot lives on a different node than the chosen worker.

    Raised by :meth:`CheckpointManager.resume` when the requested
    snapshot is a node-local ``pause`` but the worker is on a different
    node.  The caller (actor lifecycle) catches this to fall back to the
    durable golden / last snapshot.
    """


__all__ = [
    "CheckpointConfig",
    "PauseSnapshotNotLocal",
    "KIND_GOLDEN",
    "KIND_LAST",
    "KIND_PAUSE",
    "SCOPE_FULL",
    "SCOPE_DATA",
    "SNAPSHOT_KINDS",
    "SNAPSHOT_SCOPES",
    "ActorSnapshotRef",
]
