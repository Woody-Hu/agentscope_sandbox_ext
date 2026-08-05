# -*- coding: utf-8 -*-
"""Tests for :mod:`agentscope_sandbox_ext._actor._registry`.

The registry is the persistence layer for actor records; the lock
provider is the keyed-serialisation primitive used by the lifecycle.
Together they implement the optimistic-version + distributed-lock
combination that protects actor state mutations.

These tests exercise the **real** in-process implementations with
**real** asyncio concurrency — no mocks.  Concurrency tests use
``asyncio.gather`` to drive genuine interleaving and assert on the
observed ordering.
"""

from __future__ import annotations

import asyncio

import pytest

from agentscope_sandbox_ext._actor._registry import (
    InProcessActorRegistry,
    InProcessLockProvider,
)
from agentscope_sandbox_ext._actor._types import (
    STATUS_RUNNING,
    STATUS_SUSPENDED,
    ActorRecord,
    ActorRef,
    Constraints,
    SandboxClass,
    TemplateRef,
    VersionConflict,
)


# ── helpers ─────────────────────────────────────────────────────


def _record(name: str = "a", status: str = STATUS_SUSPENDED) -> ActorRecord:
    return ActorRecord(
        ref=ActorRef(namespace="ns", name=name),
        template=TemplateRef(name="t", version=1),
        constraints=Constraints(sandbox_class=SandboxClass.of("vfs")),
        status=status,
    )


# ── InProcessActorRegistry: create / get ────────────────────────


async def test_create_returns_record_with_timestamps():
    reg = InProcessActorRegistry()
    rec = await reg.create(_record())
    assert rec.created_at > 0
    assert rec.updated_at == rec.created_at
    assert rec.version == 1


async def test_create_duplicate_raises_keyerror():
    reg = InProcessActorRegistry()
    await reg.create(_record())
    with pytest.raises(KeyError, match="already exists"):
        await reg.create(_record())


async def test_get_missing_raises_keyerror():
    reg = InProcessActorRegistry()
    with pytest.raises(KeyError, match="no such actor"):
        await reg.get(ActorRef(namespace="ns", name="ghost"))


async def test_get_returns_stored_record():
    reg = InProcessActorRegistry()
    await reg.create(_record())
    fetched = await reg.get(ActorRef(namespace="ns", name="a"))
    assert fetched.ref.name == "a"


# ── optimistic version ──────────────────────────────────────────


async def test_update_with_correct_version_succeeds_and_bumps():
    reg = InProcessActorRegistry()
    rec = await reg.create(_record())
    rec.status = STATUS_RUNNING
    updated = await reg.update(rec, expected_version=1)
    assert updated.version == 2
    assert updated.status == STATUS_RUNNING
    assert updated.updated_at >= rec.created_at


async def test_update_with_stale_version_raises_conflict():
    reg = InProcessActorRegistry()
    rec = await reg.create(_record())
    # First writer bumps to v2.
    rec.status = STATUS_RUNNING
    await reg.update(rec, expected_version=1)
    # Stale writer still expecting v1.
    with pytest.raises(VersionConflict):
        await reg.update(rec, expected_version=1)


async def test_update_missing_record_raises_keyerror():
    reg = InProcessActorRegistry()
    rec = _record()
    with pytest.raises(KeyError):
        await reg.update(rec, expected_version=1)


async def test_concurrent_updates_only_one_wins_per_version():
    """Two writers racing with the same expected_version: exactly one
    succeeds, the other gets :class:`VersionConflict`.

    Both writers capture the base version *before* either writes, then
    both attempt to update with that same (now stale for one of them)
    expected_version.  Under optimistic concurrency exactly one wins.
    """
    reg = InProcessActorRegistry()
    base = await reg.create(_record())
    # Pre-fetch the version both writers will race on.
    race_version = base.version

    async def _write(status: str) -> bool:
        rec = await reg.get(base.ref)
        rec.status = status
        try:
            await reg.update(rec, expected_version=race_version)
            return True
        except VersionConflict:
            return False

    results = await asyncio.gather(_write(STATUS_RUNNING), _write("OTHER"))
    assert results.count(True) == 1
    assert results.count(False) == 1
    final = await reg.get(base.ref)
    assert final.version == 2


# ── delete ──────────────────────────────────────────────────────


async def test_delete_suspended_actor_succeeds():
    reg = InProcessActorRegistry()
    ref = ActorRef(namespace="ns", name="a")
    await reg.create(_record())
    await reg.delete(ref)
    with pytest.raises(KeyError):
        await reg.get(ref)


async def test_delete_running_actor_raises():
    reg = InProcessActorRegistry()
    ref = ActorRef(namespace="ns", name="a")
    rec = await reg.create(_record())
    rec.status = STATUS_RUNNING
    await reg.update(rec, expected_version=1)
    with pytest.raises(RuntimeError, match=STATUS_SUSPENDED + " actors"):
        await reg.delete(ref)


async def test_delete_missing_raises_keyerror():
    reg = InProcessActorRegistry()
    with pytest.raises(KeyError):
        await reg.delete(ActorRef(namespace="ns", name="ghost"))


# ── list ────────────────────────────────────────────────────────


async def test_list_filters_by_namespace():
    reg = InProcessActorRegistry()
    a1 = _record(name="a1")
    a2 = _record(name="a2")
    other = ActorRecord(
        ref=ActorRef(namespace="other", name="b"),
        template=TemplateRef(name="t", version=1),
        constraints=Constraints(sandbox_class=SandboxClass.of("vfs")),
    )
    await reg.create(a1)
    await reg.create(a2)
    await reg.create(other)
    ns_records = await reg.list("ns")
    assert {r.ref.name for r in ns_records} == {"a1", "a2"}


async def test_list_empty_namespace_returns_empty_list():
    reg = InProcessActorRegistry()
    assert await reg.list("nope") == []


# ── InProcessLockProvider ───────────────────────────────────────


async def test_lock_serialises_same_key():
    """Two coroutines acquiring the same key run one at a time."""
    lp = InProcessLockProvider()
    order: list[str] = []

    async def _hold(tag: str, delay: float) -> None:
        async with lp.acquire("k"):
            order.append(f"{tag}-start")
            await asyncio.sleep(delay)
            order.append(f"{tag}-end")

    await asyncio.gather(_hold("a", 0.05), _hold("b", 0.0))
    # a fully completes before b starts (or vice-versa).
    assert order in (
        ["a-start", "a-end", "b-start", "b-end"],
        ["b-start", "b-end", "a-start", "a-end"],
    )


async def test_lock_different_keys_run_concurrently():
    lp = InProcessLockProvider()
    started: list[str] = []
    done: list[str] = []

    async def _hold(key: str) -> None:
        async with lp.acquire(key):
            started.append(key)
            await asyncio.sleep(0.05)
            done.append(key)

    await asyncio.gather(_hold("k1"), _hold("k2"))
    # Both started before either finished ⇒ concurrency.
    assert started == ["k1", "k2"]
    assert sorted(done) == ["k1", "k2"]


async def test_lock_table_does_not_grow_unbounded():
    """After all holders release, the lock entry is removed."""
    lp = InProcessLockProvider()
    async with lp.acquire("k"):
        pass
    assert "k" not in lp._locks


async def test_lock_refcount_handles_concurrent_holders():
    """If two coroutines acquire the same key concurrently, the
    ref-count must not go negative and the entry must be cleaned up
    once both release."""
    lp = InProcessLockProvider()

    async def _acq() -> None:
        async with lp.acquire("shared"):
            await asyncio.sleep(0.01)

    await asyncio.gather(_acq(), _acq())
    assert "shared" not in lp._locks
