# -*- coding: utf-8 -*-
"""Tests for :mod:`agentscope_sandbox_ext._actor._types`.

The data model is the *lingua franca* of the modular runtime — every
control-plane message serialises through ``to_dict`` / ``from_dict``.
These tests exercise validation, serialisation round-trip, and the
invariants the rest of the system relies on (immutable identity,
class-constrained scheduling, snapshot-kind / scope validation).
"""

from __future__ import annotations

import pytest

from agentscope_sandbox_ext._actor._types import (
    ACTOR_STATUSES,
    KIND_GOLDEN,
    KIND_LAST,
    KIND_PAUSE,
    PHASE_FAILED,
    PHASE_INITIAL,
    PHASE_READY,
    SANDBOX_CLASSES,
    SCOPE_DATA,
    SCOPE_FULL,
    SNAPSHOT_KINDS,
    SNAPSHOT_SCOPES,
    STATUS_RUNNING,
    STATUS_SUSPENDED,
    ActorRecord,
    ActorRef,
    ActorSnapshotRef,
    Constraints,
    SandboxClass,
    TemplateRef,
    VersionConflict,
)


# ── SandboxClass ────────────────────────────────────────────────


def test_sandbox_class_accepts_known_values():
    for v in SANDBOX_CLASSES:
        sc = SandboxClass.of(v)
        assert sc.value == v


def test_sandbox_class_rejects_unknown_value():
    with pytest.raises(ValueError, match="Unknown sandbox class"):
        SandboxClass("not-a-class")


def test_sandbox_class_round_trip():
    sc = SandboxClass.of("gvisor")
    d = sc.to_dict()
    assert d == {"value": "gvisor"}
    assert SandboxClass.from_dict(d) == sc


# ── ActorRef ────────────────────────────────────────────────────


def test_actor_ref_key_is_stable_string():
    ref = ActorRef(namespace="ns-a", name="actor-1")
    assert ref.key == "ns-a/actor-1"


def test_actor_ref_rejects_empty_parts():
    with pytest.raises(ValueError):
        ActorRef(namespace="", name="x")
    with pytest.raises(ValueError):
        ActorRef(namespace="x", name="")


def test_actor_ref_is_hashable_and_frozen():
    ref = ActorRef(namespace="ns", name="a")
    with pytest.raises(Exception):
        ref.namespace = "other"  # type: ignore[misc]
    # Hashable ⇒ usable as dict key.
    d = {ref: 1}
    assert d[ActorRef(namespace="ns", name="a")] == 1


def test_actor_ref_round_trip():
    ref = ActorRef(namespace="ns", name="a")
    assert ActorRef.from_dict(ref.to_dict()) == ref


# ── TemplateRef ─────────────────────────────────────────────────


def test_template_ref_rejects_zero_version():
    with pytest.raises(ValueError, match="version"):
        TemplateRef(name="t", version=0)


def test_template_ref_key_includes_version():
    ref = TemplateRef(name="t", version=3)
    assert ref.key == "t@3"


def test_template_ref_round_trip():
    ref = TemplateRef(name="t", version=2)
    assert TemplateRef.from_dict(ref.to_dict()) == ref


# ── Constraints ─────────────────────────────────────────────────


def test_constraints_default_selectors_empty():
    c = Constraints(sandbox_class=SandboxClass.of("vfs"))
    assert c.template_selector == {}
    assert c.actor_selector == {}
    assert c.required_nodes == ()


def test_constraints_round_trip_preserves_all_fields():
    c = Constraints(
        sandbox_class=SandboxClass.of("kata"),
        template_selector={"tier": "gold"},
        actor_selector={"region": "us"},
        required_nodes=("node-1", "node-2"),
    )
    d = c.to_dict()
    assert Constraints.from_dict(d) == c


def test_constraints_required_nodes_defaults_to_empty_tuple():
    """``required_nodes`` defaults to ``()`` (not ``None``) so it is
    always iterable."""
    c = Constraints(sandbox_class=SandboxClass.of("vfs"))
    assert isinstance(c.required_nodes, tuple)
    assert len(c.required_nodes) == 0


# ── ActorSnapshotRef ────────────────────────────────────────────


def test_snapshot_ref_pause_requires_node():
    with pytest.raises(ValueError, match="node"):
        ActorSnapshotRef(snapshot_id="s1", kind=KIND_PAUSE, scope=SCOPE_FULL)


def test_snapshot_ref_rejects_unknown_kind():
    with pytest.raises(ValueError, match="kind"):
        ActorSnapshotRef(snapshot_id="s1", kind="bogus", scope=SCOPE_FULL)


def test_snapshot_ref_rejects_unknown_scope():
    with pytest.raises(ValueError, match="scope"):
        ActorSnapshotRef(snapshot_id="s1", kind=KIND_LAST, scope="bogus")


def test_snapshot_ref_golden_does_not_require_node():
    snap = ActorSnapshotRef(
        snapshot_id="golden-1", kind=KIND_GOLDEN, scope=SCOPE_FULL,
    )
    assert snap.node is None


def test_snapshot_ref_round_trip_all_kinds():
    for kind in SNAPSHOT_KINDS:
        node = "n1" if kind == KIND_PAUSE else None
        snap = ActorSnapshotRef(
            snapshot_id="sid", kind=kind,
            scope=SCOPE_DATA if kind != KIND_PAUSE else SCOPE_FULL,
            node=node,
        )
        assert ActorSnapshotRef.from_dict(snap.to_dict()) == snap


def test_snapshot_scope_constants_distinct():
    assert SCOPE_FULL != SCOPE_DATA
    assert {SCOPE_FULL, SCOPE_DATA} == set(SNAPSHOT_SCOPES)


# ── ActorRecord ─────────────────────────────────────────────────


def _make_record() -> ActorRecord:
    return ActorRecord(
        ref=ActorRef(namespace="ns", name="a"),
        template=TemplateRef(name="t", version=1),
        constraints=Constraints(sandbox_class=SandboxClass.of("vfs")),
    )


def test_actor_record_defaults():
    r = _make_record()
    assert r.status == STATUS_SUSPENDED
    assert r.version == 1
    assert r.worker_id is None
    assert r.last_snapshot is None
    assert r.tags == {}


def test_actor_record_round_trip():
    r = _make_record()
    r.status = STATUS_RUNNING
    r.worker_id = "w-1"
    r.last_snapshot = ActorSnapshotRef(
        snapshot_id="snap", kind=KIND_LAST, scope=SCOPE_FULL,
    )
    r.tags = {"env": "test"}
    d = r.to_dict()
    r2 = ActorRecord.from_dict(d)
    assert r2.ref == r.ref
    assert r2.template == r.template
    assert r2.status == STATUS_RUNNING
    assert r2.worker_id == "w-1"
    assert r2.last_snapshot == r.last_snapshot
    assert r2.tags == {"env": "test"}


def test_actor_record_round_trip_with_pause_snapshot():
    r = _make_record()
    r.last_snapshot = ActorSnapshotRef(
        snapshot_id="pause-1", kind=KIND_PAUSE, scope=SCOPE_FULL, node="n1",
    )
    r2 = ActorRecord.from_dict(r.to_dict())
    assert r2.last_snapshot is not None
    assert r2.last_snapshot.kind == KIND_PAUSE
    assert r2.last_snapshot.node == "n1"


# ── phase / status constants ────────────────────────────────────


def test_phase_constants_form_full_set():
    assert {PHASE_INITIAL, "Baking", PHASE_READY, PHASE_FAILED} == {
        "Initial", "Baking", "Ready", "Failed",
    }


def test_actor_statuses_form_full_set():
    assert set(ACTOR_STATUSES) == {
        STATUS_SUSPENDED, "RESUMING", STATUS_RUNNING, "SUSPENDING",
    }


# ── VersionConflict ─────────────────────────────────────────────


def test_version_conflict_is_runtime_error():
    """Optimistic-concurrency failures are runtime errors so callers
    that catch ``RuntimeError`` for general cleanup also catch stale
    writes."""
    assert issubclass(VersionConflict, RuntimeError)
