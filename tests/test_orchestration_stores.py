# -*- coding: utf-8 -*-
"""Tests for the CAS stores (:class:`InMemoryActorStore`, :class:`InMemoryWorkerStore`).

Verifies the optimistic-concurrency contract that the orchestrator's
worker assignment relies on: a contended ``claim()`` on the same
worker sees exactly one winner and N−1 ``VersionConflict`` losers.
"""

from __future__ import annotations

import asyncio

import pytest

from agentscope_sandbox_ext._orchestration import (
    Actor,
    ActorNotFound,
    ActorStatus,
    ActorStore,
    Constraints,
    InMemoryActorStore,
    InMemoryWorkerStore,
    SandboxClass,
    VersionConflict,
    Worker,
    WorkerAssignment,
    WorkerBusy,
    WorkerNotFound,
    WorkerState,
)


# ── ActorStore ──────────────────────────────────────────────────


def _actor(actor_id: str = "a1", version: int = 1) -> Actor:
    return Actor(
        actor_id=actor_id,
        namespace="default",
        template_id="t1",
        status=ActorStatus.SUSPENDED,
        version=version,
    )


async def test_actor_create_then_get_roundtrip():
    store = InMemoryActorStore()
    created = await store.put(_actor(), expected_version=None)
    assert created.version == 1
    fetched = await store.get("a1")
    assert fetched.actor_id == "a1"
    assert fetched.status == ActorStatus.SUSPENDED


async def test_actor_create_rejects_duplicate():
    store = InMemoryActorStore()
    await store.put(_actor(), expected_version=None)
    with pytest.raises(VersionConflict):
        await store.put(_actor(), expected_version=None)


async def test_actor_update_requires_matching_version():
    store = InMemoryActorStore()
    created = await store.put(_actor(), expected_version=None)
    # Stale version -> conflict.
    stale = _actor()
    stale.version = created.version
    running = _actor()
    running.status = ActorStatus.RUNNING
    running.version = created.version
    updated = await store.put(running, expected_version=created.version)
    assert updated.version == created.version + 1
    # Old expected version now conflicts.
    with pytest.raises(VersionConflict):
        await store.put(stale, expected_version=created.version)


async def test_actor_get_missing_raises():
    store = InMemoryActorStore()
    with pytest.raises(ActorNotFound):
        await store.get("nope")


async def test_actor_list_filters_by_namespace():
    store = InMemoryActorStore()
    a = _actor("a1"); a.namespace = "ns1"
    b = _actor("a2"); b.namespace = "ns2"
    await store.put(a, expected_version=None)
    await store.put(b, expected_version=None)
    assert {a.actor_id for a in await store.list("ns1")} == {"a1"}
    assert {a.actor_id for a in await store.list("ns2")} == {"a2"}
    assert len(await store.list()) == 2


async def test_actor_delete_is_idempotent():
    store = InMemoryActorStore()
    await store.put(_actor(), expected_version=None)
    await store.delete("a1")
    await store.delete("a1")  # no error
    with pytest.raises(ActorNotFound):
        await store.get("a1")


async def test_actor_put_returns_independent_copy():
    """Mutating the returned record must not affect the stored one."""
    store = InMemoryActorStore()
    created = await store.put(_actor(), expected_version=None)
    created.status = ActorStatus.RUNNING  # mutate the caller's copy
    fetched = await store.get("a1")
    assert fetched.status == ActorStatus.SUSPENDED  # stored unchanged


# ── WorkerStore CAS ─────────────────────────────────────────────


def _worker(worker_id: str = "w1", state=WorkerState.ACTIVE) -> Worker:
    return Worker(
        worker_id=worker_id,
        state=state,
        sandbox_class=SandboxClass.VFS,
        labels={"app": "bash"},
        node="node-a",
        sandbox=object(),  # stand-in live sandbox
    )


def _assignment(actor_id: str = "a1") -> WorkerAssignment:
    return WorkerAssignment(
        actor_id=actor_id,
        worker_id="w1",
        sandbox_class=SandboxClass.VFS,
        node="node-a",
    )


async def test_worker_register_then_get():
    store = InMemoryWorkerStore()
    await store.register(_worker())
    w = await store.get("w1")
    assert w.state == WorkerState.ACTIVE
    assert w.assignment is None


async def test_worker_register_rejects_duplicate():
    store = InMemoryWorkerStore()
    await store.register(_worker())
    with pytest.raises(VersionConflict):
        await store.register(_worker())


async def test_claim_binds_assignment_and_bumps_version():
    store = InMemoryWorkerStore()
    w = await store.register(_worker())
    claimed = await store.claim("w1", _assignment(), expected_version=w.version)
    assert claimed.assignment is not None
    assert claimed.assignment.actor_id == "a1"
    assert claimed.version == w.version + 1


async def test_claim_with_stale_version_conflicts():
    store = InMemoryWorkerStore()
    w = await store.register(_worker())
    with pytest.raises(VersionConflict):
        await store.claim("w1", _assignment(), expected_version=w.version + 99)


async def test_claim_on_busy_worker_raises_worker_busy():
    store = InMemoryWorkerStore()
    w = await store.register(_worker())
    await store.claim("w1", _assignment("a1"), expected_version=w.version)
    # A second claim on the now-busy worker must fail (not VersionConflict).
    busy = await store.get("w1")
    with pytest.raises(WorkerBusy):
        await store.claim("w1", _assignment("a2"), expected_version=busy.version)


async def test_claim_on_draining_worker_raises_worker_busy():
    store = InMemoryWorkerStore()
    w = await store.register(_worker())
    draining = await store.set_state(
        "w1", WorkerState.DRAINING, expected_version=w.version
    )
    assert draining.state == WorkerState.DRAINING
    with pytest.raises(WorkerBusy):
        await store.claim("w1", _assignment(), expected_version=draining.version)


async def test_release_clears_assignment():
    store = InMemoryWorkerStore()
    w = await store.register(_worker())
    claimed = await store.claim("w1", _assignment(), expected_version=w.version)
    released = await store.release("w1", expected_version=claimed.version)
    assert released.assignment is None
    assert released.version == claimed.version + 1


async def test_concurrent_claim_has_exactly_one_winner():
    """N concurrent claims on the same worker → 1 winner, N−1 conflicts.

    This is the contention behaviour the orchestrator's scheduler
    relies on for at-most-one-actor-per-worker assignment.
    """
    store = InMemoryWorkerStore()
    w = await store.register(_worker())

    async def try_claim(actor_id: str) -> str:
        try:
            fresh = await store.get("w1")
            await store.claim(
                "w1", _assignment(actor_id), expected_version=fresh.version
            )
            return "won"
        except (VersionConflict, WorkerBusy):
            return "lost"

    results = await asyncio.gather(*[try_claim(f"a{i}") for i in range(10)])
    assert results.count("won") == 1
    assert results.count("lost") == 9


# ── list_idle + constraints ─────────────────────────────────────


async def test_list_idle_filters_by_sandbox_class():
    store = InMemoryWorkerStore()
    v = _worker("w1"); v.sandbox_class = SandboxClass.VFS
    c = _worker("w2"); c.sandbox_class = SandboxClass.MICROVM
    await store.register(v)
    await store.register(c)
    idle_vfs = await store.list_idle(Constraints(sandbox_class=SandboxClass.VFS))
    idle_vm = await store.list_idle(Constraints(sandbox_class=SandboxClass.MICROVM))
    assert {w.worker_id for w in idle_vfs} == {"w1"}
    assert {w.worker_id for w in idle_vm} == {"w2"}


async def test_list_idle_excludes_busy_and_draining():
    store = InMemoryWorkerStore()
    w1 = await store.register(_worker("w1"))
    await store.register(_worker("w2"))
    # w1 becomes busy; w3 is draining.
    await store.claim("w1", _assignment(), expected_version=w1.version)
    w3 = await store.register(_worker("w3"))
    await store.set_state("w3", WorkerState.DRAINING, expected_version=w3.version)
    idle = await store.list_idle(Constraints(sandbox_class=SandboxClass.VFS))
    assert {w.worker_id for w in idle} == {"w2"}


async def test_list_idle_filters_by_required_node():
    store = InMemoryWorkerStore()
    await store.register(_worker("w1"))  # node-a
    w2 = _worker("w2"); w2.node = "node-b"
    await store.register(w2)
    idle = await store.list_idle(
        Constraints(sandbox_class=SandboxClass.VFS, required_node="node-b")
    )
    assert {w.worker_id for w in idle} == {"w2"}


async def test_list_idle_filters_by_label_selector():
    store = InMemoryWorkerStore()
    w1 = _worker("w1"); w1.labels = {"app": "bash"}
    w2 = _worker("w2"); w2.labels = {"app": "python"}
    await store.register(w1)
    await store.register(w2)
    idle = await store.list_idle(
        Constraints(
            sandbox_class=SandboxClass.VFS,
            template_selector={"app": "python"},
        )
    )
    assert {w.worker_id for w in idle} == {"w2"}
