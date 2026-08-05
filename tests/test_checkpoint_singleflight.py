# -*- coding: utf-8 -*-
"""Tests for :mod:`agentscope_sandbox_ext._checkpoint._singleflight`.

Singleflight collapses concurrent identical async calls into one
execution: N callers with the same key ⇒ 1 factory invocation, all
share the result (or exception).  These tests verify the leader /
follower behaviour, exception broadcast, and that the entry is
removed after resolution so a later call re-executes.
"""

from __future__ import annotations

import asyncio

import pytest

from agentscope_sandbox_ext._checkpoint._singleflight import Singleflight


# ── leader / follower ───────────────────────────────────────────


async def test_concurrent_callers_share_single_execution():
    """Five concurrent callers, one factory invocation, all share the
    same result value."""
    sf = Singleflight()
    invocations = 0

    async def _factory() -> int:
        nonlocal invocations
        invocations += 1
        await asyncio.sleep(0.05)  # let followers pile up
        return 42

    results = await asyncio.gather(
        *(sf.run("k", _factory) for _ in range(5))
    )
    assert results == [42, 42, 42, 42, 42]
    assert invocations == 1


async def test_followers_wait_for_leader_to_finish():
    """A follower that arrives while the leader is still running
    receives the leader's result once the leader resolves."""
    sf = Singleflight()
    started = asyncio.Event()
    release = asyncio.Event()

    async def _factory() -> str:
        started.set()
        await release.wait()
        return "done"

    async def _follower() -> str:
        await started.wait()
        return await sf.run("k", _factory)

    leader_task = asyncio.create_task(sf.run("k", _factory))
    follower_task = asyncio.create_task(_follower())
    await asyncio.sleep(0.02)
    assert not leader_task.done()
    assert not follower_task.done()
    release.set()
    assert await leader_task == "done"
    assert await follower_task == "done"


# ── distinct keys ───────────────────────────────────────────────


async def test_distinct_keys_run_independently():
    sf = Singleflight()
    counts = {"a": 0, "b": 0}

    async def _mk(key: str):
        async def _f() -> str:
            counts[key] += 1
            await asyncio.sleep(0.01)
            return key
        return await sf.run(key, _f)

    await asyncio.gather(
        *(_mk(k) for k in ("a", "b", "a", "b", "a", "b"))
    )
    assert counts == {"a": 1, "b": 1}


# ── exception broadcast ─────────────────────────────────────────


async def test_exception_broadcast_to_all_followers():
    sf = Singleflight()
    calls = 0

    async def _factory() -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        raise ValueError("boom")

    results = await asyncio.gather(
        *(sf.run("k", _factory) for _ in range(3)),
        return_exceptions=True,
    )
    assert calls == 1
    for r in results:
        assert isinstance(r, ValueError)
        assert "boom" in str(r)


async def test_exception_does_not_leave_inflight_entry():
    """After a failed factory the entry must be removed so the next
    call re-executes rather than awaiting a settled future."""
    sf = Singleflight()

    async def _fail() -> None:
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        await sf.run("k", _fail)
    assert sf.inflight_count == 0
    assert "k" not in sf._inflight


# ── entry lifecycle ─────────────────────────────────────────────


async def test_entry_removed_after_success_so_next_call_re_executes():
    sf = Singleflight()
    calls = 0

    async def _factory() -> int:
        nonlocal calls
        calls += 1
        return calls

    first = await sf.run("k", _factory)
    second = await sf.run("k", _factory)
    assert first == 1
    assert second == 2
    assert sf.inflight_count == 0


async def test_inflight_count_reflects_active_executions():
    sf = Singleflight()
    release = asyncio.Event()

    async def _factory() -> None:
        await release.wait()

    task = asyncio.create_task(sf.run("k", _factory))
    await asyncio.sleep(0.02)
    assert sf.inflight_count == 1
    release.set()
    await task
    assert sf.inflight_count == 0


# ── factory does not block other keys ───────────────────────────


async def test_slow_factory_does_not_block_other_keys():
    """A slow factory on key 'a' must not delay a fast factory on
    key 'b' — the table lock is only held briefly."""
    sf = Singleflight()
    release = asyncio.Event()
    order: list[str] = []

    async def _slow() -> str:
        order.append("slow-start")
        await release.wait()
        order.append("slow-end")
        return "slow"

    async def _fast() -> str:
        order.append("fast")
        return "fast"

    slow_task = asyncio.create_task(sf.run("a", _slow))
    await asyncio.sleep(0.02)
    fast_result = await sf.run("b", _fast)
    release.set()
    await slow_task
    assert fast_result == "fast"
    # 'fast' was recorded before 'slow-end'.
    assert order.index("fast") < order.index("slow-end")
