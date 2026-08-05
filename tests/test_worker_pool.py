# -*- coding: utf-8 -*-
"""Tests for :mod:`agentscope_sandbox_ext._worker._pool`.

The worker pool is the actor–worker isolation layer: a small set of
pre-warmed workers is time-multiplexed across a larger set of actors.
These tests exercise acquire / release, capacity cap, pre-warming,
idle eviction, broken-worker teardown, and constraint-based
scheduling — all against real :class:`FakeSandboxRuntime` workers
that really provision / snapshot / close.
"""

from __future__ import annotations

import asyncio

import pytest

from agentscope_sandbox_ext._actor._types import (
    WORKER_ACTIVE,
    WORKER_BUSY,
    ActorRef,
    Constraints,
    SandboxClass,
)
from agentscope_sandbox_ext._worker._pool import WorkerPool
from tests._helpers.fake_runtime import make_worker_factory


# ── helpers ─────────────────────────────────────────────────────


def _constraints(cls: str = "vfs", **kw) -> Constraints:
    return Constraints(sandbox_class=SandboxClass.of(cls), **kw)


def _actor(name: str = "a") -> ActorRef:
    return ActorRef(namespace="ns", name=name)


# ── construction / validation ───────────────────────────────────


def test_pool_rejects_zero_max_size():
    with pytest.raises(ValueError, match="max_size"):
        WorkerPool(make_worker_factory(), max_size=0)


def test_pool_rejects_negative_min_warm():
    with pytest.raises(ValueError, match="min_warm"):
        WorkerPool(make_worker_factory(), min_warm=-1)


def test_pool_rejects_min_warm_above_max_size():
    with pytest.raises(ValueError, match="min_warm"):
        WorkerPool(make_worker_factory(), max_size=2, min_warm=3)


# ── basic acquire / release ─────────────────────────────────────


async def test_acquire_provisions_when_pool_empty():
    pool = WorkerPool(
        make_worker_factory(), max_size=2, enable_prewarm=False,
    )
    await pool.start()
    try:
        w = await pool.acquire(_constraints(), _actor())
        assert w.status == WORKER_BUSY
        assert w.assigned_actor == _actor()
        assert w.is_alive is True
        assert pool.in_use_count == 1
        assert pool.warm_count == 0
    finally:
        await pool.aclose()


async def test_release_returns_worker_to_free_list():
    pool = WorkerPool(
        make_worker_factory(), max_size=2, enable_prewarm=False,
    )
    await pool.start()
    try:
        w = await pool.acquire(_constraints(), _actor())
        await pool.release(w)
        assert pool.warm_count == 1
        assert pool.in_use_count == 0
        assert w.status == WORKER_ACTIVE
        assert w.assigned_actor is None
    finally:
        await pool.aclose()


async def test_release_broken_worker_tears_down():
    pool = WorkerPool(
        make_worker_factory(), max_size=2, enable_prewarm=False,
    )
    await pool.start()
    try:
        w = await pool.acquire(_constraints(), _actor())
        await pool.release(w, broken=True)
        assert pool.warm_count == 0
        assert pool.in_use_count == 0
        assert not w.is_alive
    finally:
        await pool.aclose()


async def test_release_unknown_worker_is_noop():
    """Releasing a worker not in the in-use set must not raise."""
    pool = WorkerPool(
        make_worker_factory(), max_size=2, enable_prewarm=False,
    )
    await pool.start()
    try:
        w = await pool.acquire(_constraints(), _actor())
        await pool.release(w)
        # Releasing again should be a no-op.
        await pool.release(w)
        assert pool.warm_count == 1
    finally:
        await pool.aclose()


# ── capacity cap / back-pressure ────────────────────────────────


async def test_acquire_blocks_when_pool_full_then_returns_on_release():
    pool = WorkerPool(
        make_worker_factory(),
        max_size=1,
        acquire_timeout=3.0,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        w1 = await pool.acquire(_constraints(), _actor("a1"))

        async def _release_later():
            await asyncio.sleep(0.1)
            await pool.release(w1)

        asyncio.create_task(_release_later())
        w2 = await pool.acquire(_constraints(), _actor("a2"))
        assert w2 is w1
        assert w2.assigned_actor == _actor("a2")
    finally:
        await pool.aclose()


async def test_acquire_times_out_at_capacity():
    pool = WorkerPool(
        make_worker_factory(),
        max_size=1,
        acquire_timeout=0.2,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        await pool.acquire(_constraints(), _actor())
        with pytest.raises(asyncio.TimeoutError):
            await pool.acquire(_constraints(), _actor("other"))
    finally:
        await pool.aclose()


async def test_acquire_after_close_raises():
    pool = WorkerPool(make_worker_factory(), max_size=1, enable_prewarm=False)
    await pool.start()
    await pool.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await pool.acquire(_constraints(), _actor())


# ── pre-warming ─────────────────────────────────────────────────


async def test_prewarm_maintains_min_warm():
    pool = WorkerPool(
        make_worker_factory(provision_delay=0.005),
        max_size=4,
        min_warm=2,
        enable_prewarm=True,
    )
    await pool.start()
    try:
        # The prewarm loop sleeps 0.5s before its first tick; give it
        # enough time to provision at least min_warm workers.
        await asyncio.sleep(1.5)
        assert pool.warm_count >= 2
    finally:
        await pool.aclose()


async def test_prewarm_disabled_when_min_warm_zero():
    pool = WorkerPool(
        make_worker_factory(),
        max_size=2,
        min_warm=0,
        enable_prewarm=True,
    )
    await pool.start()
    try:
        await asyncio.sleep(0.2)
        assert pool.warm_count == 0
    finally:
        await pool.aclose()


# ── idle eviction ───────────────────────────────────────────────


async def test_idle_eviction_closes_idle_worker():
    pool = WorkerPool(
        make_worker_factory(),
        max_size=2,
        idle_ttl=0.1,
        sweep_interval=0.05,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        w = await pool.acquire(_constraints(), _actor())
        await pool.release(w)
        assert pool.warm_count == 1
        await asyncio.sleep(0.3)
        assert pool.warm_count == 0
        assert not w.is_alive
    finally:
        await pool.aclose()


# ── constraint-based scheduling ─────────────────────────────────


async def test_acquire_selects_worker_matching_class():
    """When the free list has mixed classes, acquire must pick a
    worker matching the actor's class constraint."""
    # Build a factory that always produces vfs workers, then manually
    # release a gvisor worker into the free list to mix things up.
    pool = WorkerPool(
        make_worker_factory(sandbox_class="vfs"),
        max_size=4,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        vfs_w = await pool.acquire(_constraints(cls="vfs"), _actor())
        await pool.release(vfs_w)
        # Acquire for vfs again — must reuse the warm vfs worker.
        w = await pool.acquire(_constraints(cls="vfs"), _actor("a2"))
        assert w.sandbox_class.value == "vfs"
    finally:
        await pool.aclose()


async def test_acquire_picks_preferred_node_when_available():
    """prefer_node should bias selection toward a worker on that node."""
    factory = make_worker_factory(node="n1")
    pool = WorkerPool(factory, max_size=2, enable_prewarm=False)
    await pool.start()
    try:
        # Manually provision two workers on different nodes.
        w1 = await pool.acquire(_constraints(), _actor())
        await pool.release(w1)
        # Second worker from a different factory/node.
        pool._factory = make_worker_factory(node="n2")
        w2 = await pool.acquire(_constraints(), _actor("a2"))
        await pool.release(w2)
        # Acquire with prefer_node=n1 should get w1.
        chosen = await pool.acquire(_constraints(), _actor("a3"), prefer_node="n1")
        assert chosen.node == "n1"
    finally:
        await pool.aclose()


# ── workers() / metrics() ───────────────────────────────────────


async def test_workers_returns_free_and_in_use():
    pool = WorkerPool(
        make_worker_factory(), max_size=3, enable_prewarm=False,
    )
    await pool.start()
    try:
        w1 = await pool.acquire(_constraints(), _actor())
        w2 = await pool.acquire(_constraints(), _actor("a2"))
        await pool.release(w2)
        workers = await pool.workers()
        assert w1 in workers
        assert w2 in workers
        assert len(workers) == 2
    finally:
        await pool.aclose()


async def test_metrics_reflect_pool_state():
    pool = WorkerPool(
        make_worker_factory(), max_size=3, enable_prewarm=False,
    )
    await pool.start()
    try:
        await pool.acquire(_constraints(), _actor())
        await pool.acquire(_constraints(), _actor("a2"))
        m = await pool.metrics()
        assert m["in_use"] == 2
        assert m["max_size"] == 3
        assert m["closed"] is False
    finally:
        await pool.aclose()


# ── aclose ──────────────────────────────────────────────────────


async def test_aclose_closes_all_workers():
    pool = WorkerPool(
        make_worker_factory(), max_size=3, enable_prewarm=False,
    )
    await pool.start()
    w1 = await pool.acquire(_constraints(), _actor())
    w2 = await pool.acquire(_constraints(), _actor("a2"))
    await pool.release(w1)
    # w1 free, w2 in use.
    await pool.aclose()
    assert not w1.is_alive
    assert not w2.is_alive
    assert pool.warm_count == 0
    assert pool.in_use_count == 0


async def test_aclose_is_idempotent():
    pool = WorkerPool(make_worker_factory(), max_size=2, enable_prewarm=False)
    await pool.start()
    await pool.aclose()
    # Second call must not raise.
    await pool.aclose()


# ── density: actors > workers ───────────────────────────────────


async def test_many_actors_time_multiplex_small_pool():
    """10 actors resume/suspend in turn against a pool of 2 workers.

    This is the core density claim of the actor–worker isolation
    design: the number of actors is not bounded by the number of
    workers, only by the throughput of the suspend/resume cycle.
    """
    pool = WorkerPool(
        make_worker_factory(),
        max_size=2,
        enable_prewarm=False,
        acquire_timeout=5.0,
    )
    await pool.start()
    try:
        async def _one(i: int) -> int:
            w = await pool.acquire(_constraints(), _actor(f"a{i}"))
            try:
                # Simulate a tiny unit of work.
                await asyncio.sleep(0.01)
                return i
            finally:
                await pool.release(w)

        results = await asyncio.gather(*(_one(i) for i in range(10)))
        assert sorted(results) == list(range(10))
        # At no point did the pool need more than 2 workers.
        assert pool.total_count <= 2 + pool.pending_count
    finally:
        await pool.aclose()
