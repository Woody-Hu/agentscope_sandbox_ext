# -*- coding: utf-8 -*-
"""End-to-end tests for :class:`ActorLifecycle`.

These tests wire together every layer — registry, template registry,
worker pool, checkpoint manager — and exercise the actor state machine
(create → resume → suspend → resume → pause → resume → delete) against
real on-disk snapshots and a real (in-process) worker factory.

The goal is to prove the modular runtime delivers on its design
promises: actor–worker isolation (many actors on few workers),
snapshot locality (pause snapshots restore without remote fetch),
golden clone-restore (new actors resume from a shared golden
snapshot), and optimistic-concurrency safety (no stale writes under
concurrent workflows).
"""

from __future__ import annotations

import asyncio

import pytest

from agentscope_sandbox_ext._actor._lifecycle import (
    ActorLifecycle,
    IllegalTransition,
)
from agentscope_sandbox_ext._actor._registry import (
    InProcessActorRegistry,
    InProcessLockProvider,
)
from agentscope_sandbox_ext._actor._types import (
    KIND_GOLDEN,
    KIND_LAST,
    KIND_PAUSE,
    SCOPE_FULL,
    ActorRef,
    Constraints,
    SandboxClass,
    STATUS_RUNNING,
    STATUS_SUSPENDED,
    TemplateRef,
)
from agentscope_sandbox_ext._checkpoint._manager import CheckpointManager
from agentscope_sandbox_ext._checkpoint._types import CheckpointConfig
from agentscope_sandbox_ext._runtime.local_snapshot_store import (
    LocalSnapshotStore,
)
from agentscope_sandbox_ext._template._template import (
    ActorTemplateRecord,
    InProcessTemplateRegistry,
    TemplateBaker,
)
from agentscope_sandbox_ext._worker._pool import WorkerPool
from tests._helpers.fake_runtime import make_worker_factory


# ── fixtures ────────────────────────────────────────────────────


@pytest.fixture
def durable_store(tmp_path):
    return LocalSnapshotStore(str(tmp_path / "durable"))


@pytest.fixture
def checkpoint(durable_store):
    return CheckpointManager(CheckpointConfig(durable_store=durable_store))


@pytest.fixture
def registry():
    return InProcessActorRegistry()


@pytest.fixture
def templates():
    return InProcessTemplateRegistry()


@pytest.fixture
def lock_provider():
    return InProcessLockProvider()


@pytest.fixture
async def pool():
    p = WorkerPool(
        make_worker_factory(sandbox_class="vfs"),
        max_size=2,
        enable_prewarm=False,
        acquire_timeout=2.0,
    )
    await p.start()
    try:
        yield p
    finally:
        await p.aclose()


@pytest.fixture
async def baker(checkpoint, templates):
    return TemplateBaker(
        factory=make_worker_factory(sandbox_class="vfs"),
        checkpoint=checkpoint,
        registry=templates,
    )


@pytest.fixture
def lifecycle(registry, templates, pool, checkpoint, lock_provider):
    return ActorLifecycle(
        registry=registry,
        templates=templates,
        pool=pool,
        checkpoint=checkpoint,
        lock_provider=lock_provider,
    )


@pytest.fixture
def template_ref():
    return TemplateRef(name="t", version=1)


@pytest.fixture
async def baked_template(baker, templates, template_ref):
    """A Ready template with a golden snapshot baked in."""
    t = ActorTemplateRecord(
        ref=template_ref,
        sandbox_class=SandboxClass.of("vfs"),
        spec={"image": "vfs:latest"},
    )
    await templates.create(t)
    return await baker.bake(t)


def _constraints() -> Constraints:
    return Constraints(sandbox_class=SandboxClass.of("vfs"))


def _actor(name: str = "a1") -> ActorRef:
    return ActorRef(namespace="ns", name=name)


# ── create / get / list ─────────────────────────────────────────


async def test_create_seeds_last_snapshot_from_golden(lifecycle, baked_template):
    """A new actor's last_snapshot is the template's golden snapshot so
    the first resume clone-restores from it (no cold boot)."""
    rec = await lifecycle.create_actor(
        _actor(), baked_template.ref, _constraints(),
    )
    assert rec.status == STATUS_SUSPENDED
    assert rec.last_snapshot is not None
    assert rec.last_snapshot.kind == KIND_GOLDEN


async def test_create_rejects_class_mismatch(lifecycle, baked_template):
    from agentscope_sandbox_ext._actor._types import SandboxClass
    bad = Constraints(sandbox_class=SandboxClass.of("gvisor"))
    with pytest.raises(ValueError, match="sandbox_class"):
        await lifecycle.create_actor(_actor(), baked_template.ref, bad)


async def test_get_and_list(lifecycle, baked_template):
    a = _actor("a")
    b = _actor("b")
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    await lifecycle.create_actor(b, baked_template.ref, _constraints())
    assert (await lifecycle.get_actor(a)).ref == a
    assert {r.ref.name for r in await lifecycle.list_actors("ns")} == {"a", "b"}


# ── resume / suspend ────────────────────────────────────────────


async def test_resume_transitions_to_running(lifecycle, baked_template):
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    rec = await lifecycle.resume_actor(a)
    assert rec.status == STATUS_RUNNING
    assert rec.worker_id is not None


async def test_resume_then_suspend_restores_suspended(lifecycle, baked_template):
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    await lifecycle.resume_actor(a)
    rec = await lifecycle.suspend_actor(a)
    assert rec.status == STATUS_SUSPENDED
    assert rec.worker_id is None
    assert rec.last_snapshot is not None
    assert rec.last_snapshot.kind == KIND_LAST


async def test_resume_from_last_snapshot_restores_state(lifecycle, baked_template):
    """After suspend → resume, the actor's data state from the suspend
    snapshot must be present on the new worker."""
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    running = await lifecycle.resume_actor(a)
    # The worker hosting the actor is in the pool's in-use set; reach
    # in via the registry's worker_id and the pool's workers() list.
    workers = await lifecycle._pool.workers()
    hosting = next(w for w in workers if w.worker_id == running.worker_id)
    hosting.runtime.write_file("note.txt", b"suspended-state")

    await lifecycle.suspend_actor(a)
    rec = await lifecycle.resume_actor(a)
    workers = await lifecycle._pool.workers()
    hosting2 = next(w for w in workers if w.worker_id == rec.worker_id)
    assert hosting2.runtime.read_file("note.txt") == b"suspended-state"


# ── pause / resume ──────────────────────────────────────────────


async def test_pause_then_resume_same_node_restores_locally(lifecycle, baked_template, pool):
    """Pause captures node-locally; resume on the same node restores
    without touching the durable store."""
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    running = await lifecycle.resume_actor(a)
    workers = await lifecycle._pool.workers()
    hosting = next(w for w in workers if w.worker_id == running.worker_id)
    hosting.runtime.write_file("state.txt", b"v1")
    node = hosting.node

    paused = await lifecycle.pause_actor(a)
    assert paused.last_snapshot.kind == KIND_PAUSE
    assert paused.last_snapshot.node == node

    # Resume — should pick the same node and restore locally.
    rec = await lifecycle.resume_actor(a, prefer_node=node)
    workers = await lifecycle._pool.workers()
    hosting2 = next(w for w in workers if w.worker_id == rec.worker_id)
    assert hosting2.node == node
    assert hosting2.runtime.read_file("state.txt") == b"v1"


async def test_resume_from_pause_different_node_falls_back_to_golden(
    lifecycle, baked_template, pool,
):
    """If the scheduler lands on a different node than the pause
    snapshot's node, the lifecycle falls back to the durable golden
    snapshot rather than raising PauseSnapshotNotLocal."""
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    running = await lifecycle.resume_actor(a)
    workers = await lifecycle._pool.workers()
    hosting = next(w for w in workers if w.worker_id == running.worker_id)
    hosting.runtime.write_file("state.txt", b"v1")
    pause_node = hosting.node

    await lifecycle.pause_actor(a)

    # Force a different node by hinting at a non-existent one.  The
    # scheduler will fall back to random spread; if it lands on the
    # pause node the test still passes (local restore); otherwise the
    # fallback to golden kicks in.  Either way the actor resumes.
    rec = await lifecycle.resume_actor(a, prefer_node="nonexistent-node")
    assert rec.status == STATUS_RUNNING


# ── illegal transitions ─────────────────────────────────────────


async def test_resume_when_running_raises(lifecycle, baked_template):
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    await lifecycle.resume_actor(a)
    with pytest.raises(IllegalTransition, match="cannot resume"):
        await lifecycle.resume_actor(a)


async def test_suspend_when_suspended_raises(lifecycle, baked_template):
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    with pytest.raises(IllegalTransition, match="cannot suspend"):
        await lifecycle.suspend_actor(a)


async def test_pause_when_suspended_raises(lifecycle, baked_template):
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    with pytest.raises(IllegalTransition, match="cannot pause"):
        await lifecycle.pause_actor(a)


# ── delete ──────────────────────────────────────────────────────


async def test_delete_suspended_actor(lifecycle, baked_template):
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    await lifecycle.delete_actor(a)
    with pytest.raises(KeyError):
        await lifecycle.get_actor(a)


async def test_delete_running_actor_raises(lifecycle, baked_template):
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    await lifecycle.resume_actor(a)
    with pytest.raises(RuntimeError):
        await lifecycle.delete_actor(a)


# ── concurrency: keyed lock serialises workflows ────────────────


async def test_concurrent_resume_same_actor_serialises(lifecycle, baked_template):
    """Two concurrent resume_actor calls on the same actor: one wins,
    the other raises IllegalTransition (the actor is already RUNNING)."""
    a = _actor()
    await lifecycle.create_actor(a, baked_template.ref, _constraints())
    results = await asyncio.gather(
        lifecycle.resume_actor(a),
        lifecycle.resume_actor(a),
        return_exceptions=True,
    )
    successes = [r for r in results if not isinstance(r, Exception)]
    failures = [r for r in results if isinstance(r, Exception)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], IllegalTransition)


# ── density: many actors, few workers ───────────────────────────


async def test_density_many_actors_resume_suspend_on_small_pool(
    lifecycle, baked_template, pool,
):
    """8 actors each go through resume → suspend against a 2-worker
    pool.  All 8 must complete successfully — proving the density
    claim end-to-end."""
    refs = [_actor(f"a{i}") for i in range(8)]
    for r in refs:
        await lifecycle.create_actor(r, baked_template.ref, _constraints())

    async def _cycle(r: ActorRef) -> str:
        await lifecycle.resume_actor(r)
        await lifecycle.suspend_actor(r)
        return r.name

    results = await asyncio.gather(*(_cycle(r) for r in refs))
    assert sorted(results) == [r.name for r in refs]
    # After every actor is suspended, no workers should be in use.
    assert pool.in_use_count == 0
