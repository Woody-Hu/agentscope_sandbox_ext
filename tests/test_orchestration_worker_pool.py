# -*- coding: utf-8 -*-
"""Tests for :class:`WorkerPool` + :class:`Scheduler`.

The pool is exercised with a **real** factory that produces real
:class:`AgentFSWorkspace` instances backed by a host tempdir — no
mocking.  The sandboxes really open files, really spawn subprocesses,
and really track an ``is_alive`` flag, so the warm-idle reuse path,
the idle-TTL sweeper, and the drain path all run against genuine
lifecycle behaviour.

This gives the worker pool a genuine workout of:

* :func:`infer_sandbox_class` mapping (``agentfs`` → ``VFS``).
* :class:`Scheduler.pick` — random among eligible, constraint filtering.
* :meth:`WorkerPool.acquire_worker` — fresh materialisation from
  :class:`SandboxPool` and the warm-idle reuse fast path.
* :meth:`WorkerPool.claim_for_actor` — CAS at-most-one-actor-per-worker.
* :meth:`WorkerPool.release_worker` — warm-idle return vs drain.
* warm-idle cap enforcement and idle-TTL retirement.
* concurrent acquire producing distinct workers.
"""

from __future__ import annotations

import asyncio
import os
import random
import tempfile
from typing import Any

import pytest

from agentscope_sandbox_ext import AgentFSWorkspace, SandboxPool
from agentscope_sandbox_ext._orchestration import (
    Constraints,
    InMemoryWorkerStore,
    SandboxClass,
    Scheduler,
    Worker,
    WorkerAssignment,
    WorkerBusy,
    WorkerPool,
    WorkerState,
    VersionConflict,
    infer_sandbox_class,
)


# ── real factory ───────────────────────────────────────────────


def _make_factory():
    """Return an async factory that provisions a real AgentFSWorkspace."""

    async def _make() -> AgentFSWorkspace:
        workdir = tempfile.mkdtemp(prefix="as_wpool_")
        ws = AgentFSWorkspace(host_workdir=workdir)
        await ws._provision_backend()
        await ws.initialize()
        return ws

    return _make


@pytest.fixture
async def sandbox_pool():
    pool = SandboxPool(_make_factory(), max_size=8, enable_prewarm=False)
    await pool.start()
    try:
        yield pool
    finally:
        await pool.aclose()


@pytest.fixture
async def worker_pool(sandbox_pool):
    wp = WorkerPool(
        InMemoryWorkerStore(),
        sandbox_pool,
        Scheduler(),
        max_warm_idle=4,
        idle_ttl=300.0,
        sweep_interval=60.0,
        node="test-node",
    )
    await wp.start()
    try:
        yield wp
    finally:
        await wp.aclose()


def _assignment_for(worker: Worker, actor_id: str = "a1") -> WorkerAssignment:
    return WorkerAssignment(
        actor_id=actor_id,
        worker_id=worker.worker_id,
        sandbox_class=worker.sandbox_class,
        node=worker.node,
    )


# ── infer_sandbox_class ────────────────────────────────────────


def test_infer_sandbox_class_maps_agentfs_to_vfs():
    ws = AgentFSWorkspace.__new__(AgentFSWorkspace)
    ws.sandbox_kind = "agentfs"
    assert infer_sandbox_class(ws) == SandboxClass.VFS


def test_infer_sandbox_class_maps_known_kinds():
    class _Stub:
        def __init__(self, kind: str) -> None:
            self.sandbox_kind = kind

    assert infer_sandbox_class(_Stub("firecracker")) == SandboxClass.MICROVM
    assert infer_sandbox_class(_Stub("gvisor")) == SandboxClass.CONTAINER
    assert infer_sandbox_class(_Stub("kata")) == SandboxClass.CONTAINER
    assert infer_sandbox_class(_Stub("docker")) == SandboxClass.CONTAINER
    assert infer_sandbox_class(_Stub("vfs")) == SandboxClass.VFS


def test_infer_sandbox_class_unknown_kind_defaults_to_vfs():
    class _Stub:
        sandbox_kind = "exotic"

    assert infer_sandbox_class(_Stub()) == SandboxClass.VFS


def test_infer_sandbox_class_missing_attr_defaults_to_vfs():
    class _Stub:
        pass  # no sandbox_kind

    assert infer_sandbox_class(_Stub()) == SandboxClass.VFS


# ── Scheduler ──────────────────────────────────────────────────


def _idle_worker(
    worker_id: str = "w1",
    *,
    sandbox_class: SandboxClass = SandboxClass.VFS,
    labels: dict[str, str] | None = None,
    node: str | None = None,
) -> Worker:
    return Worker(
        worker_id=worker_id,
        state=WorkerState.ACTIVE,
        sandbox_class=sandbox_class,
        labels=dict(labels or {}),
        node=node,
        sandbox=object(),  # stand-in; is_idle requires sandbox not None
    )


async def test_scheduler_pick_returns_none_when_no_candidates():
    sched = Scheduler()
    assert await sched.pick([], Constraints(sandbox_class=SandboxClass.VFS)) is None


async def test_scheduler_pick_returns_eligible_worker():
    sched = Scheduler(rng=random.Random(0))
    w = _idle_worker("w1")
    picked = await sched.pick([w], Constraints(sandbox_class=SandboxClass.VFS))
    assert picked is not None
    assert picked.worker_id == "w1"


async def test_scheduler_pick_filters_by_sandbox_class():
    sched = Scheduler()
    v = _idle_worker("w1", sandbox_class=SandboxClass.VFS)
    c = _idle_worker("w2", sandbox_class=SandboxClass.MICROVM)
    picked = await sched.pick([v, c], Constraints(sandbox_class=SandboxClass.MICROVM))
    assert picked is not None
    assert picked.worker_id == "w2"


async def test_scheduler_pick_filters_by_required_node():
    sched = Scheduler()
    w1 = _idle_worker("w1", node="node-a")
    w2 = _idle_worker("w2", node="node-b")
    picked = await sched.pick(
        [w1, w2],
        Constraints(sandbox_class=SandboxClass.VFS, required_node="node-b"),
    )
    assert picked is not None
    assert picked.worker_id == "w2"


async def test_scheduler_pick_filters_by_label_selector():
    sched = Scheduler()
    w1 = _idle_worker("w1", labels={"app": "bash"})
    w2 = _idle_worker("w2", labels={"app": "python"})
    picked = await sched.pick(
        [w1, w2],
        Constraints(
            sandbox_class=SandboxClass.VFS,
            template_selector={"app": "python"},
        ),
    )
    assert picked is not None
    assert picked.worker_id == "w2"


async def test_scheduler_pick_excludes_busy_workers():
    sched = Scheduler()
    busy = _idle_worker("w1")
    busy.assignment = WorkerAssignment(
        actor_id="other",
        worker_id="w1",
        sandbox_class=SandboxClass.VFS,
    )
    idle = _idle_worker("w2")
    picked = await sched.pick([busy, idle], Constraints(sandbox_class=SandboxClass.VFS))
    assert picked is not None
    assert picked.worker_id == "w2"


async def test_scheduler_pick_is_uniform_over_eligible():
    """With a fixed RNG, pick returns an eligible worker (not always the first)."""
    eligible = [_idle_worker(f"w{i}") for i in range(10)]
    sched = Scheduler(rng=random.Random(42))
    picks = [
        (await sched.pick(eligible, Constraints(sandbox_class=SandboxClass.VFS))).worker_id
        for _ in range(50)
    ]
    # At least two distinct workers picked over 50 draws → not always-first.
    assert len(set(picks)) >= 2


# ── WorkerPool.acquire_worker ──────────────────────────────────


async def test_acquire_worker_materialises_fresh_from_sandbox_pool(worker_pool):
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    worker = await worker_pool.acquire_worker(constraints)
    try:
        assert worker.state == WorkerState.ACTIVE
        assert worker.assignment is None  # idle, not yet claimed
        assert worker.sandbox is not None
        assert worker.sandbox_class == SandboxClass.VFS
        assert worker.node == "test-node"
        assert worker.is_alive if hasattr(worker, "is_alive") else True
        # The sandbox is genuinely alive.
        assert worker.sandbox.is_alive is True
    finally:
        # Clean up: release with drain so the sandbox returns to the pool.
        await worker_pool.release_worker(worker, drain=True)


async def test_acquire_worker_assigns_unique_worker_ids(worker_pool):
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    workers = await asyncio.gather(
        *[worker_pool.acquire_worker(constraints) for _ in range(4)]
    )
    try:
        ids = {w.worker_id for w in workers}
        assert len(ids) == 4  # all distinct
        # Each carries its own live sandbox.
        assert all(w.sandbox is not None for w in workers)
        assert all(w.sandbox.is_alive for w in workers)
    finally:
        await asyncio.gather(
            *[worker_pool.release_worker(w, drain=True) for w in workers]
        )


# ── WorkerPool.claim_for_actor ─────────────────────────────────


async def test_claim_for_actor_binds_assignment(worker_pool):
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    worker = await worker_pool.acquire_worker(constraints)
    try:
        assignment = _assignment_for(worker, "a1")
        claimed = await worker_pool.claim_for_actor(worker, assignment)
        assert claimed.assignment is not None
        assert claimed.assignment.actor_id == "a1"
        assert claimed.version == worker.version + 1
    finally:
        # Release the *claimed* worker (its version is current); the
        # pre-claim ``worker`` is now stale and would fail the CAS.
        await worker_pool.release_worker(claimed, drain=True)


async def test_claim_on_already_claimed_worker_raises_busy(worker_pool):
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    worker = await worker_pool.acquire_worker(constraints)
    claimed = await worker_pool.claim_for_actor(worker, _assignment_for(worker, "a1"))
    try:
        # A second claim with the stale (pre-claim) version must fail.
        with pytest.raises((VersionConflict, WorkerBusy)):
            await worker_pool.claim_for_actor(worker, _assignment_for(worker, "a2"))
    finally:
        await worker_pool.release_worker(claimed, drain=True)


# ── WorkerPool.release_worker ──────────────────────────────────


async def test_release_returns_worker_to_warm_idle(worker_pool):
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    worker = await worker_pool.acquire_worker(constraints)
    claimed = await worker_pool.claim_for_actor(worker, _assignment_for(worker, "a1"))
    # Release (no drain) → worker returns to warm-idle registry.
    await worker_pool.release_worker(claimed)
    assert await worker_pool.warm_idle_count() == 1
    # The sandbox is still alive (not torn down).
    fresh = await worker_pool._store.get(worker.worker_id)
    assert fresh.sandbox is not None
    assert fresh.sandbox.is_alive is True
    assert fresh.assignment is None  # binding cleared


async def test_release_with_drain_retires_worker(worker_pool):
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    worker = await worker_pool.acquire_worker(constraints)
    claimed = await worker_pool.claim_for_actor(worker, _assignment_for(worker, "a1"))
    await worker_pool.release_worker(claimed, drain=True)
    assert await worker_pool.warm_idle_count() == 0
    # The worker record is gone (retired).
    from agentscope_sandbox_ext._orchestration import WorkerNotFound

    with pytest.raises(WorkerNotFound):
        await worker_pool._store.get(worker.worker_id)


async def test_warm_idle_reuse_is_fast_path(worker_pool):
    """A released worker is re-acquired from the warm-idle registry
    rather than materialising a fresh sandbox from the pool."""
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    worker = await worker_pool.acquire_worker(constraints)
    claimed = await worker_pool.claim_for_actor(worker, _assignment_for(worker, "a1"))
    await worker_pool.release_worker(claimed)

    reused = await worker_pool.acquire_worker(constraints)
    try:
        assert reused.worker_id == worker.worker_id  # same worker
        assert reused.sandbox is worker.sandbox  # same live sandbox
        assert await worker_pool.warm_idle_count() == 0  # popped
    finally:
        await worker_pool.release_worker(reused, drain=True)


async def test_warm_idle_cap_retires_excess(worker_pool):
    """Beyond ``max_warm_idle`` (4), released workers are retired."""
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    workers = await asyncio.gather(
        *[worker_pool.acquire_worker(constraints) for _ in range(6)]
    )
    # Claim + release all → only 4 enter warm-idle, 2 are retired.
    claimed = [
        await worker_pool.claim_for_actor(w, _assignment_for(w, f"a{w.worker_id}"))
        for w in workers
    ]
    for c in claimed:
        await worker_pool.release_worker(c)
    assert await worker_pool.warm_idle_count() == 4


# ── idle-TTL sweeper ───────────────────────────────────────────


async def test_idle_ttl_sweeper_retires_expired_workers(sandbox_pool):
    """A warm-idle worker past ``idle_ttl`` is retired by the sweeper."""
    wp = WorkerPool(
        InMemoryWorkerStore(),
        sandbox_pool,
        Scheduler(),
        max_warm_idle=4,
        idle_ttl=0.05,           # 50 ms idle TTL
        sweep_interval=0.02,     # sweep every 20 ms
        node="test-node",
    )
    await wp.start()
    try:
        constraints = Constraints(sandbox_class=SandboxClass.VFS)
        worker = await wp.acquire_worker(constraints)
        claimed = await wp.claim_for_actor(worker, _assignment_for(worker, "a1"))
        await wp.release_worker(claimed)
        assert await wp.warm_idle_count() == 1
        # Wait long enough for the sweeper to retire it.
        await asyncio.sleep(0.25)
        assert await wp.warm_idle_count() == 0
    finally:
        await wp.aclose()


async def test_idle_ttl_sweeper_keeps_unexpired_workers(sandbox_pool):
    """A warm-idle worker within its TTL is not retired."""
    wp = WorkerPool(
        InMemoryWorkerStore(),
        sandbox_pool,
        Scheduler(),
        max_warm_idle=4,
        idle_ttl=10.0,           # 10 s — far longer than the test
        sweep_interval=0.05,
        node="test-node",
    )
    await wp.start()
    try:
        constraints = Constraints(sandbox_class=SandboxClass.VFS)
        worker = await wp.acquire_worker(constraints)
        claimed = await wp.claim_for_actor(worker, _assignment_for(worker, "a1"))
        await wp.release_worker(claimed)
        await asyncio.sleep(0.15)
        assert await wp.warm_idle_count() == 1
    finally:
        await wp.aclose()


# ── aclose ─────────────────────────────────────────────────────


async def test_aclose_retires_all_warm_idle(sandbox_pool):
    wp = WorkerPool(
        InMemoryWorkerStore(),
        sandbox_pool,
        Scheduler(),
        max_warm_idle=4,
        idle_ttl=300.0,
        sweep_interval=300.0,  # disable sweeper; we test aclose directly
        node="test-node",
    )
    await wp.start()
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    workers = await asyncio.gather(
        *[wp.acquire_worker(constraints) for _ in range(3)]
    )
    for w in workers:
        c = await wp.claim_for_actor(w, _assignment_for(w, f"a{w.worker_id}"))
        await wp.release_worker(c)
    assert await wp.warm_idle_count() == 3
    await wp.aclose()
    assert await wp.warm_idle_count() == 0


async def test_aclose_is_idempotent(sandbox_pool):
    wp = WorkerPool(
        InMemoryWorkerStore(),
        sandbox_pool,
        Scheduler(),
        node="test-node",
    )
    await wp.start()
    await wp.aclose()
    await wp.aclose()  # must not raise


# ── metrics ────────────────────────────────────────────────────


async def test_metrics_reports_warm_idle_count(worker_pool):
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    w = await worker_pool.acquire_worker(constraints)
    claimed = await worker_pool.claim_for_actor(w, _assignment_for(w, "a1"))
    await worker_pool.release_worker(claimed)
    m = await worker_pool.metrics()
    assert m["warm_idle"] == 1
    assert m["max_warm_idle"] == 4
    assert m["closed"] is False


# ── concurrency: at-most-one-actor-per-worker ─────────────────


async def test_concurrent_claim_on_same_worker_has_one_winner(worker_pool):
    """N concurrent claims on the same acquired worker → 1 winner,
    N−1 losers (VersionConflict / WorkerBusy).  This is the
    at-most-one-actor-per-worker invariant the scheduler relies on."""
    constraints = Constraints(sandbox_class=SandboxClass.VFS)
    worker = await worker_pool.acquire_worker(constraints)
    try:

        async def try_claim(actor_id: str) -> str:
            try:
                # Each caller re-reads the worker to get the freshest
                # version, mirroring the orchestrator's retry loop.
                fresh = await worker_pool._store.get(worker.worker_id)
                await worker_pool.claim_for_actor(fresh, _assignment_for(worker, actor_id))
                return "won"
            except (VersionConflict, WorkerBusy):
                return "lost"

        results = await asyncio.gather(*[try_claim(f"a{i}") for i in range(8)])
        assert results.count("won") == 1
        assert results.count("lost") == 7
    finally:
        # Re-read the winner's current version before releasing.
        fresh = await worker_pool._store.get(worker.worker_id)
        await worker_pool.release_worker(fresh, drain=True)
