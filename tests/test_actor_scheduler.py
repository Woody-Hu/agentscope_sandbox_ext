# -*- coding: utf-8 -*-
"""Tests for :mod:`agentscope_sandbox_ext._actor._scheduler`.

The scheduler picks an idle worker satisfying an actor's hard
constraints, then random-spreads among survivors.  ``prefer_node``
gives pause-locality a fast path.  These tests assert each constraint
in isolation plus the locality fast path, using real :class:`Worker`
instances built on the in-process runtime.
"""

from __future__ import annotations

import random

import pytest

from agentscope_sandbox_ext._actor._scheduler import (
    NoCapacityError,
    Scheduler,
    _labels_match,
)
from agentscope_sandbox_ext._actor._types import (
    WORKER_ACTIVE,
    WORKER_BUSY,
    Constraints,
    SandboxClass,
)
from agentscope_sandbox_ext._worker._types import Worker
from tests._helpers.fake_runtime import FakeSandboxRuntime


# ── helpers ─────────────────────────────────────────────────────


def _worker(
    *,
    cls: str = "vfs",
    node: str = "n1",
    labels: dict[str, str] | None = None,
    status: str = WORKER_ACTIVE,
    alive: bool = True,
    assigned: bool = False,
) -> Worker:
    rt = FakeSandboxRuntime(
        sandbox_class_value=cls, node_value=node, labels=labels or {},
    )
    rt.is_alive = alive
    w = Worker(
        worker_id=f"w-{node}",
        runtime=rt,
        labels=dict(labels or {}),
        status=status,
        assigned_actor=None if not assigned else None,  # set below
    )
    if assigned:
        from agentscope_sandbox_ext._actor._types import ActorRef
        w.assigned_actor = ActorRef(namespace="ns", name="other")
    return w


def _constraints(
    *,
    cls: str = "vfs",
    template_selector: dict[str, str] | None = None,
    actor_selector: dict[str, str] | None = None,
    required_nodes: tuple[str, ...] = (),
) -> Constraints:
    return Constraints(
        sandbox_class=SandboxClass.of(cls),
        template_selector=template_selector or {},
        actor_selector=actor_selector or {},
        required_nodes=required_nodes,
    )


# ── Scheduler.applies ───────────────────────────────────────────


def test_applies_accepts_matching_idle_worker():
    w = _worker()
    assert Scheduler.applies(w, _constraints())


def test_applies_rejects_busy_worker():
    w = _worker(status=WORKER_BUSY)
    assert not Scheduler.applies(w, _constraints())


def test_applies_rejects_worker_with_assigned_actor():
    w = _worker(assigned=True)
    assert not Scheduler.applies(w, _constraints())


def test_applies_rejects_dead_worker():
    w = _worker(alive=False)
    assert not Scheduler.applies(w, _constraints())


def test_applies_rejects_mismatched_sandbox_class():
    w = _worker(cls="gvisor")
    assert not Scheduler.applies(w, _constraints(cls="vfs"))


def test_applies_rejects_missing_template_label():
    w = _worker(labels={"tier": "gold"})
    c = _constraints(template_selector={"tier": "silver"})
    assert not Scheduler.applies(w, c)


def test_applies_accepts_matching_template_label():
    w = _worker(labels={"tier": "gold", "extra": "x"})
    c = _constraints(template_selector={"tier": "gold"})
    assert Scheduler.applies(w, c)


def test_applies_rejects_missing_actor_label():
    w = _worker(labels={"region": "us"})
    c = _constraints(actor_selector={"region": "eu"})
    assert not Scheduler.applies(w, c)


def test_applies_respects_required_nodes():
    w = _worker(node="n1")
    assert Scheduler.applies(w, _constraints(required_nodes=("n1", "n2")))
    assert not Scheduler.applies(w, _constraints(required_nodes=("n3",)))


def test_applies_empty_required_nodes_allows_any_node():
    w = _worker(node="anywhere")
    assert Scheduler.applies(w, _constraints(required_nodes=()))


# ── _labels_match ───────────────────────────────────────────────


def test_labels_match_empty_selector_always_matches():
    assert _labels_match({}, {})
    assert _labels_match({"a": "1"}, {})


def test_labels_match_subset_check():
    assert _labels_match({"a": "1", "b": "2"}, {"a": "1"})
    assert not _labels_match({"a": "1"}, {"a": "2"})
    assert not _labels_match({"a": "1"}, {"b": "1"})


# ── Scheduler.pick ──────────────────────────────────────────────


def test_pick_raises_when_no_candidate():
    s = Scheduler()
    with pytest.raises(NoCapacityError):
        s.pick([], _constraints())


def test_pick_returns_only_candidate():
    s = Scheduler()
    w = _worker()
    assert s.pick([w], _constraints()) is w


def test_pick_filters_by_class():
    s = Scheduler()
    good = _worker(cls="vfs", node="n1")
    bad = _worker(cls="gvisor", node="n2")
    chosen = s.pick([bad, good], _constraints(cls="vfs"))
    assert chosen is good


def test_pick_random_spreads_among_candidates():
    """With a deterministic RNG, pick chooses among candidates
    pseudo-randomly — the test asserts the RNG is actually consulted
    (i.e. multiple candidates are eligible and the choice varies)."""
    rng = random.Random(42)
    s = Scheduler(rng=rng)
    workers = [_worker(node=f"n{i}") for i in range(5)]
    chosen_ids = {s.pick(workers, _constraints()).worker_id for _ in range(20)}
    # At least two distinct workers chosen across 20 calls.
    assert len(chosen_ids) >= 2


def test_pick_prefer_node_chooses_local_candidate_immediately():
    s = Scheduler()
    local = _worker(node="n1")
    other = _worker(node="n2")
    chosen = s.pick([other, local], _constraints(), prefer_node="n1")
    assert chosen is local


def test_pick_prefer_node_falls_back_to_random_when_no_local():
    """If the hinted node has no candidate, the scheduler does not
    fail — it picks randomly among the survivors."""
    s = Scheduler()
    w = _worker(node="n2")
    chosen = s.pick([w], _constraints(), prefer_node="n1")
    assert chosen is w


def test_pick_skips_busy_when_local_hinted():
    """If the only worker on the hinted node is busy, the locality
    fast path must not pick it."""
    s = Scheduler()
    busy_local = _worker(node="n1", status=WORKER_BUSY)
    free_remote = _worker(node="n2")
    chosen = s.pick([busy_local, free_remote], _constraints(), prefer_node="n1")
    assert chosen is free_remote


def test_pick_required_nodes_combined_with_prefer_node():
    """required_nodes pins the candidate set; prefer_node picks within
    that set."""
    s = Scheduler()
    a = _worker(node="n1")
    b = _worker(node="n2")
    chosen = s.pick(
        [a, b],
        _constraints(required_nodes=("n1", "n2")),
        prefer_node="n2",
    )
    assert chosen is b
