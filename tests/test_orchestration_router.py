# -*- coding: utf-8 -*-
"""Tests for :class:`InMemoryRouter` — active-actor routing table.

Verifies the contract the orchestrator's resume/suspend path relies on:
a binding exists only while the actor is ``RUNNING`` / ``RESUMING``;
``resolve`` returns ``None`` for an idle actor (the caller must resume
before routing); bindings are namespaced by ``(namespace, actor_id)`` so
the same id in two namespaces is two distinct actors.
"""

from __future__ import annotations

import asyncio

import pytest

from agentscope_sandbox_ext._orchestration import (
    ActorRef,
    InMemoryRouter,
    SandboxClass,
    WorkerAssignment,
)


def _assignment(actor_id: str = "a1", worker_id: str = "w1") -> WorkerAssignment:
    return WorkerAssignment(
        actor_id=actor_id,
        worker_id=worker_id,
        sandbox_class=SandboxClass.VFS,
        node="node-a",
    )


def _ref(actor_id: str = "a1", namespace: str = "default") -> ActorRef:
    return ActorRef(namespace=namespace, actor_id=actor_id)


# ── bind / resolve ─────────────────────────────────────────────


async def test_resolve_returns_none_for_unbound_actor():
    router = InMemoryRouter()
    assert await router.resolve(_ref()) is None


async def test_bind_then_resolve_returns_assignment():
    router = InMemoryRouter()
    ref = _ref()
    assignment = _assignment()
    await router.bind(ref, assignment)
    resolved = await router.resolve(ref)
    assert resolved is not None
    assert resolved.worker_id == "w1"
    assert resolved.actor_id == "a1"


async def test_bind_overwrites_previous_assignment():
    """Re-binding the same actor replaces the prior assignment
    (models an actor resuming onto a different worker)."""
    router = InMemoryRouter()
    ref = _ref()
    await router.bind(ref, _assignment(actor_id="a1", worker_id="w1"))
    await router.bind(ref, _assignment(actor_id="a1", worker_id="w2"))
    resolved = await router.resolve(ref)
    assert resolved is not None
    assert resolved.worker_id == "w2"


async def test_unbind_clears_assignment():
    router = InMemoryRouter()
    ref = _ref()
    await router.bind(ref, _assignment())
    await router.unbind(ref)
    assert await router.resolve(ref) is None


async def test_unbind_is_idempotent():
    """Unbinding an actor with no binding is a no-op."""
    router = InMemoryRouter()
    await router.unbind(_ref())  # must not raise
    await router.unbind(_ref())  # still no-op


# ── namespacing ────────────────────────────────────────────────


async def test_same_actor_id_in_different_namespaces_are_distinct():
    """``default/a1`` and ``ns2/a1`` are two independent bindings."""
    router = InMemoryRouter()
    ref_a = _ref(actor_id="a1", namespace="default")
    ref_b = _ref(actor_id="a1", namespace="ns2")
    await router.bind(ref_a, _assignment(actor_id="a1", worker_id="w1"))
    await router.bind(ref_b, _assignment(actor_id="a1", worker_id="w2"))
    ra = await router.resolve(ref_a)
    rb = await router.resolve(ref_b)
    assert ra is not None and rb is not None
    assert ra.worker_id == "w1"
    assert rb.worker_id == "w2"
    # Unbinding one does not affect the other.
    await router.unbind(ref_a)
    assert await router.resolve(ref_a) is None
    assert (await router.resolve(ref_b)).worker_id == "w2"


# ── list_bindings ──────────────────────────────────────────────


async def test_list_bindings_returns_snapshot_copy():
    router = InMemoryRouter()
    await router.bind(_ref("a1"), _assignment(actor_id="a1", worker_id="w1"))
    await router.bind(_ref("a2"), _assignment(actor_id="a2", worker_id="w2"))
    snap = await router.list_bindings()
    assert set(snap.keys()) == {"default/a1", "default/a2"}
    # Mutating the snapshot must not affect the router's internal state.
    snap.clear()
    snap2 = await router.list_bindings()
    assert len(snap2) == 2


async def test_list_bindings_empty_when_no_active_actors():
    router = InMemoryRouter()
    assert await router.list_bindings() == {}


# ── concurrency ────────────────────────────────────────────────


async def test_concurrent_bind_unbind_are_serialised():
    """Concurrent bind/unbind on the same ref do not corrupt state."""
    router = InMemoryRouter()
    ref = _ref()

    async def _cycle(i: int) -> None:
        for _ in range(20):
            await router.bind(ref, _assignment(worker_id=f"w{i}"))
            await router.unbind(ref)

    await asyncio.gather(*[_cycle(i) for i in range(8)])
    # After everything settles, the binding must be gone.
    assert await router.resolve(ref) is None
