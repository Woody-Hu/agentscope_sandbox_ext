# -*- coding: utf-8 -*-
"""Tests for :class:`Singleflight` — real concurrency, no mocking.

Verifies the dedup contract borrowed from the reference runtime's
ingress router: concurrent calls with the same key collapse onto one
in-flight ``fn``; every caller receives the leader's result; the
leader's budget is detached from any single caller (a joiner timing
out does not cancel the leader); late joiners inherit the remaining
budget.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from agentscope_sandbox_ext._orchestration import (
    BudgetExhausted,
    Singleflight,
)


# ── dedup ───────────────────────────────────────────────────────


async def test_concurrent_same_key_runs_fn_once():
    """N concurrent calls with the same key execute fn exactly once."""
    sf = Singleflight()
    call_count = 0

    async def slow():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return "result"

    results = await asyncio.gather(*[sf.run("k", slow) for _ in range(8)])
    assert call_count == 1
    assert all(r == "result" for r in results)


async def test_different_keys_run_independently():
    """Different keys do not dedup against each other."""
    sf = Singleflight()
    calls: list[str] = []

    async def fn(name: str):
        calls.append(name)
        await asyncio.sleep(0.01)
        return name

    a, b = await asyncio.gather(sf.run("k1", lambda: fn("a")), sf.run("k2", lambda: fn("b")))
    assert sorted(calls) == ["a", "b"]
    assert a == "a" and b == "b"


async def test_result_shared_is_identical_object():
    """Joiners receive the exact same object the leader produced."""
    sf = Singleflight()

    class _Box:
        pass

    async def make():
        await asyncio.sleep(0.02)
        return _Box()

    # Stagger so the second caller is a joiner, not a leader.
    task_a = asyncio.create_task(sf.run("k", make))
    await asyncio.sleep(0.005)
    task_b = asyncio.create_task(sf.run("k", make))
    a, b = await asyncio.gather(task_a, task_b)
    assert a is b


# ── error propagation ───────────────────────────────────────────


async def test_leader_exception_propagates_to_joiners():
    """If the leader raises, every joiner sees the same exception."""
    sf = Singleflight()
    calls = 0

    async def boom():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        raise ValueError("boom")

    task_a = asyncio.create_task(sf.run("k", boom))
    await asyncio.sleep(0.005)
    task_b = asyncio.create_task(sf.run("k", boom))
    with pytest.raises(ValueError, match="boom"):
        await task_a
    with pytest.raises(ValueError, match="boom"):
        await task_b
    assert calls == 1


# ── budget detachment ───────────────────────────────────────────


async def test_joiner_budget_exhaustion_does_not_cancel_leader():
    """A joiner timing out must NOT cancel the leader's in-flight work.

    This is the core "budget detached from caller context" property:
    if caller 1 disconnects, callers 2/3 still get the result.
    """
    sf = Singleflight(default_budget=None)  # leader: no budget
    leader_started = asyncio.Event()
    leader_done = asyncio.Event()

    async def long_leader():
        leader_started.set()
        await asyncio.sleep(0.2)
        leader_done.set()
        return "done"

    task_leader = asyncio.create_task(sf.run("k", long_leader, budget=None))
    await leader_started.wait()

    # Late joiner with a tiny budget — should time out, but the leader
    # must keep running and eventually complete.
    with pytest.raises(BudgetExhausted):
        await sf.run("k", lambda: asyncio.sleep(0), budget=0.01)

    result = await task_leader
    assert result == "done"
    assert leader_done.is_set()


async def test_late_joiner_inherits_remaining_budget():
    """A joiner that arrives partway through inherits the remaining budget."""
    sf = Singleflight()

    async def leader():
        await asyncio.sleep(0.1)
        return "ok"

    task_a = asyncio.create_task(sf.run("k", leader, budget=1.0))
    await asyncio.sleep(0.02)
    # Remaining budget is ~0.98s — plenty.  Joiner should succeed.
    result = await sf.run("k", lambda: asyncio.sleep(0), budget=1.0)
    assert result == "ok"
    await task_a


async def test_zero_remaining_budget_raises_immediately():
    """A joiner whose remaining budget is <= 0 raises BudgetExhausted."""
    sf = Singleflight(default_budget=0.0)  # immediate
    started = asyncio.Event()

    async def leader():
        started.set()
        await asyncio.sleep(0.1)
        return "ok"

    task_a = asyncio.create_task(sf.run("k", leader))
    await started.wait()
    with pytest.raises(BudgetExhausted):
        await sf.run("k", lambda: asyncio.sleep(0))
    await task_a


# ── leader runs to completion for future callers ────────────────


async def test_leader_result_cached_for_post_completion_caller():
    """After a flight finalizes, a new call is a fresh leader (no dedup)."""
    sf = Singleflight()
    calls = 0

    async def fn():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return calls

    first = await sf.run("k", fn)
    await asyncio.sleep(0)  # let finalize run
    second = await sf.run("k", fn)
    assert first == 1 and second == 2  # two separate flights


# ── introspection ───────────────────────────────────────────────


async def test_in_flight_keys_tracks_active_flights():
    sf = Singleflight()
    started = asyncio.Event()

    async def fn():
        started.set()
        await asyncio.sleep(0.05)
        return "x"

    task = asyncio.create_task(sf.run("k", fn))
    await started.wait()
    assert "k" in sf.in_flight_keys
    await task
    assert "k" not in sf.in_flight_keys


async def test_flight_stats_reports_joiners():
    sf = Singleflight()
    started = asyncio.Event()

    async def fn():
        started.set()
        await asyncio.sleep(0.05)
        return "x"

    task_a = asyncio.create_task(sf.run("k", fn))
    await started.wait()
    task_b = asyncio.create_task(sf.run("k", lambda: asyncio.sleep(0)))
    await asyncio.sleep(0.005)
    stats = sf.flight_stats("k")
    assert stats is not None
    assert stats["joiner_count"] >= 1
    await asyncio.gather(task_a, task_b)
