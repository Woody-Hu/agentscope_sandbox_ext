# -*- coding: utf-8 -*-
"""Integration tests for :class:`Orchestrator` — the actor lifecycle façade.

Wires up the **real** components end-to-end — ``InMemoryActorStore``,
``WorkerPool`` over a real ``SandboxPool`` of ``AgentFSWorkspace``,
``CheckpointBridge`` over a real ``LocalSnapshotStore``, and
``InMemoryRouter`` — and exercises the full actor lifecycle the
reference runtime popularised:

* ``create_actor`` — cheap ``SUSPENDED`` record owning no compute.
* ``resume_actor`` — idempotent, singleflighted; cold-boots the first
  actor of a template (capturing a golden snapshot) and restores the
  golden for subsequent actors of the same template.
* ``suspend_actor`` — durable checkpoint + worker freed; the snapshot
  outlives the worker.
* ``resume`` after suspend — restores the actor's own snapshot into a
  fresh worker (the suspend/resume round-trip).
* ``pause_actor`` — node-local checkpoint with a locality hint.
* ``mark_crashed`` + resume — crash recovery re-binds a fresh worker.
* concurrent ``resume`` of the same actor collapses onto one provision
  (singleflight).
* ``delete_actor`` — terminal; rejected while active.

No mocking: real sandboxes, real tar archives, real subprocess exec.
"""

from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

from agentscope_sandbox_ext import AgentFSWorkspace, SandboxPool
from agentscope_sandbox_ext._orchestration import (
    Actor,
    ActorStateError,
    ActorStatus,
    ActorTemplate,
    CheckpointBridge,
    CheckpointScope,
    Constraints,
    InMemoryActorStore,
    InMemoryRouter,
    InMemoryWorkerStore,
    Orchestrator,
    SandboxClass,
    Scheduler,
    TemplateNotFound,
    WorkerPool,
)
from agentscope_sandbox_ext._runtime import LocalSnapshotStore


# ── shared wiring ──────────────────────────────────────────────


def _factory():
    """Real factory producing a provisioned AgentFSWorkspace."""

    async def _make() -> AgentFSWorkspace:
        workdir = tempfile.mkdtemp(prefix="as_orch_")
        ws = AgentFSWorkspace(host_workdir=workdir)
        await ws._provision_backend()
        await ws.initialize()
        return ws

    return _make


@pytest.fixture
def templates():
    return {
        "bash": ActorTemplate(
            template_id="bash",
            sandbox_class=SandboxClass.VFS,
            backend_kind="agentfs",
        ),
        "python": ActorTemplate(
            template_id="python",
            sandbox_class=SandboxClass.VFS,
            backend_kind="agentfs",
        ),
    }


@pytest.fixture
async def orchestrator(tmp_path, templates):
    """A fully-wired Orchestrator over real stores + real sandboxes."""
    sandbox_pool = SandboxPool(_factory(), max_size=8, enable_prewarm=False)
    await sandbox_pool.start()
    worker_pool = WorkerPool(
        InMemoryWorkerStore(),
        sandbox_pool,
        Scheduler(),
        max_warm_idle=4,
        node="test-node",
    )
    await worker_pool.start()
    checkpoint = CheckpointBridge(LocalSnapshotStore(str(tmp_path / "snaps")))
    orch = Orchestrator(
        actor_store=InMemoryActorStore(),
        worker_pool=worker_pool,
        checkpoint=checkpoint,
        router=InMemoryRouter(),
        templates=templates,
    )
    try:
        yield orch
    finally:
        await worker_pool.aclose()
        await sandbox_pool.aclose()


# ── helpers ────────────────────────────────────────────────────


def _seed_workdir(ws: AgentFSWorkspace, files: dict[str, bytes]) -> None:
    root = ws._host_workdir
    for rel, body in files.items():
        full = os.path.join(root, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(body)


def _read_workdir_file(ws: AgentFSWorkspace, rel: str) -> bytes:
    with open(os.path.join(ws._host_workdir, rel), "rb") as f:
        return f.read()


async def _worker_of(orch: Orchestrator, actor_id: str) -> "object":
    """Fetch the live sandbox backing *actor_id*'s worker."""
    actor = await orch.get_actor(actor_id)
    assert actor.worker_assignment is not None
    worker = await orch._workers._store.get(actor.worker_assignment.worker_id)
    return worker.sandbox


# ── create_actor ───────────────────────────────────────────────


async def test_create_actor_yields_suspended_record(orchestrator):
    actor = await orchestrator.create_actor("a1", "bash")
    assert actor.actor_id == "a1"
    assert actor.status == ActorStatus.SUSPENDED
    assert actor.worker_assignment is None  # owns no compute
    assert actor.latest_snapshot_ref is None
    # Persisted in the store.
    fetched = await orchestrator.get_actor("a1")
    assert fetched.status == ActorStatus.SUSPENDED


async def test_create_actor_rejects_duplicate(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    with pytest.raises(ActorStateError, match="already exists"):
        await orchestrator.create_actor("a1", "bash")


async def test_create_actor_rejects_unknown_template(orchestrator):
    with pytest.raises(TemplateNotFound):
        await orchestrator.create_actor("a1", "no-such-template")


async def test_create_actor_namespaces_are_independent(orchestrator):
    """The same actor_id in two namespaces is two distinct actors."""
    a = await orchestrator.create_actor("a1", "bash", namespace="ns1")
    b = await orchestrator.create_actor("a1", "bash", namespace="ns2")
    assert a.namespace == "ns1"
    assert b.namespace == "ns2"
    actors = await orchestrator.list_actors()
    assert len(actors) == 2
    assert {a.namespace for a in actors} == {"ns1", "ns2"}
    ns1 = await orchestrator.list_actors(namespace="ns1")
    assert len(ns1) == 1 and ns1[0].actor_id == "a1"


async def test_template_registry_round_trip(orchestrator):
    assert orchestrator.get_template("bash").template_id == "bash"
    new_tmpl = ActorTemplate(
        template_id="custom",
        sandbox_class=SandboxClass.VFS,
        backend_kind="agentfs",
    )
    orchestrator.register_template(new_tmpl)
    assert orchestrator.get_template("custom") is new_tmpl
    with pytest.raises(TemplateNotFound):
        orchestrator.get_template("absent")


# ── resume (cold boot + golden snapshot) ───────────────────────


async def test_resume_cold_boots_suspended_actor(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    assignment = await orchestrator.resume_actor("a1")
    assert assignment.actor_id == "a1"
    assert assignment.worker_id  # bound to a real worker
    actor = await orchestrator.get_actor("a1")
    assert actor.status == ActorStatus.RUNNING
    assert actor.worker_assignment is not None
    assert actor.worker_assignment.worker_id == assignment.worker_id


async def test_resume_captures_golden_snapshot_on_first_boot(orchestrator):
    """The first actor of a template cold-boots and captures a golden
    snapshot so subsequent actors of the same template restore instead
    of cold-booting."""
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    golden = orchestrator.golden_snapshots
    assert "bash" in golden
    assert golden["bash"]  # a real snapshot_ref


async def test_resume_is_idempotent_when_already_running(orchestrator):
    """Resuming an already-RUNNING actor returns its existing assignment
    without re-provisioning a worker."""
    await orchestrator.create_actor("a1", "bash")
    first = await orchestrator.resume_actor("a1")
    second = await orchestrator.resume_actor("a1")
    assert second.worker_id == first.worker_id  # same worker
    # Only one worker was ever materialised.
    idle = await orchestrator._workers._store.list_idle(
        Constraints(sandbox_class=SandboxClass.VFS)
    )
    assert len(idle) == 0


async def test_resume_binds_router(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    assignment = await orchestrator.resume_actor("a1")
    resolved = await orchestrator._router.resolve(
        (await orchestrator.get_actor("a1")).ref
    )
    assert resolved is not None
    assert resolved.worker_id == assignment.worker_id


async def test_resume_rejects_unknown_actor(orchestrator):
    from agentscope_sandbox_ext._orchestration import ActorNotFound

    with pytest.raises(ActorNotFound):
        await orchestrator.resume_actor("nope")


# ── resume after suspend (warm restore round-trip) ────────────


async def test_suspend_then_resume_round_trips_state(orchestrator):
    """The full suspend/resume cycle: resume → mutate → suspend → resume
    ⇒ the second resume restores the actor's own snapshot, so the
    mutated state is preserved across the suspend."""
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    ws = await _worker_of(orchestrator, "a1")
    _seed_workdir(ws, {"state.db": b"actor-state-v1", "data/x.txt": b"xxx"})

    # Suspend — checkpoints the state and frees the worker.
    snapshot_ref = await orchestrator.suspend_actor("a1")
    assert snapshot_ref  # durable ref
    actor = await orchestrator.get_actor("a1")
    assert actor.status == ActorStatus.SUSPENDED
    assert actor.worker_assignment is None  # worker freed
    assert actor.latest_snapshot_ref == snapshot_ref
    # Router binding dropped.
    assert await orchestrator._router.resolve(actor.ref) is None

    # Resume — restores the snapshot into a (possibly different) worker.
    assignment = await orchestrator.resume_actor("a1")
    actor = await orchestrator.get_actor("a1")
    assert actor.status == ActorStatus.RUNNING
    ws2 = await _worker_of(orchestrator, "a1")
    # The mutated state was preserved across suspend/resume.
    assert _read_workdir_file(ws2, "state.db") == b"actor-state-v1"
    assert _read_workdir_file(ws2, "data/x.txt") == b"xxx"


async def test_resume_after_suspend_uses_actor_snapshot_not_golden(orchestrator):
    """When an actor has its own snapshot, resume restores *that*
    (not the template's golden snapshot).  Verified by mutating the
    actor's state, suspending, and confirming the mutation survives."""
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    ws = await _worker_of(orchestrator, "a1")
    _seed_workdir(ws, {"marker.txt": b"actor-specific"})
    await orchestrator.suspend_actor("a1")
    await orchestrator.resume_actor("a1")
    ws2 = await _worker_of(orchestrator, "a1")
    assert _read_workdir_file(ws2, "marker.txt") == b"actor-specific"


async def test_suspend_rejects_non_running_actor(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    with pytest.raises(ActorStateError, match="cannot suspend"):
        await orchestrator.suspend_actor("a1")


async def test_suspend_frees_worker_to_warm_idle(orchestrator):
    """Suspending returns the worker to the warm-idle registry so a
    follow-up resume can reuse it (fast path)."""
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    assert await orchestrator._workers.warm_idle_count() == 0
    await orchestrator.suspend_actor("a1")
    # The worker is back in warm-idle (subject to the cap; only one
    # worker was ever materialised).
    assert await orchestrator._workers.warm_idle_count() == 1


# ── golden snapshot: second actor restores instead of cold-booting ─


async def test_second_actor_of_template_restores_golden(orchestrator):
    """After the first actor captures a golden snapshot, the second
    actor of the same template restores the golden rather than
    cold-booting.  Verified via the checkpoint bridge's restore log:
    a2's restore must use the golden ref captured from a1's first boot
    (a2 has no snapshot of its own, so the only ref it could restore is
    the golden)."""
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    # Golden is now captured from a1's freshly-booted worker.
    assert "bash" in orchestrator.golden_snapshots
    golden_ref = orchestrator.golden_snapshots["bash"]

    await orchestrator.create_actor("a2", "bash")
    # a2 has no snapshot of its own — its only restore path is the golden.
    a2 = await orchestrator.get_actor("a2")
    assert a2.latest_snapshot_ref is None
    await orchestrator.resume_actor("a2")
    actor2 = await orchestrator.get_actor("a2")
    assert actor2.status == ActorStatus.RUNNING

    # The restore log records (actor_id, snapshot_ref) per restore call.
    # a2's restore must have used the golden ref.
    a2_restores = [
        ref for aid, ref in orchestrator._checkpoint.restore_log if aid == "a2"
    ]
    assert a2_restores, "a2 was never restored from a snapshot"
    assert golden_ref in a2_restores, (
        f"a2 did not restore the golden ref {golden_ref!r}; "
        f"restored {a2_restores!r}"
    )


async def test_golden_snapshot_is_shared_across_actors_of_template(orchestrator):
    """Both actors of the same template share the one golden ref."""
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    golden_after_a1 = orchestrator.golden_snapshots["bash"]

    await orchestrator.create_actor("a2", "bash")
    await orchestrator.resume_actor("a2")
    golden_after_a2 = orchestrator.golden_snapshots["bash"]
    # Same ref — not recaptured.
    assert golden_after_a1 == golden_after_a2


async def test_different_templates_have_distinct_goldens(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    await orchestrator.create_actor("a2", "python")
    await orchestrator.resume_actor("a2")
    golden = orchestrator.golden_snapshots
    assert golden["bash"] != golden["python"]


# ── singleflight: concurrent resume collapses onto one provision ─


async def test_concurrent_resume_collapses_onto_one_provision(orchestrator):
    """N concurrent resume_actor calls for the same actor execute one
    provision; every caller receives the same assignment."""
    await orchestrator.create_actor("a1", "bash")
    assignments = await asyncio.gather(
        *[orchestrator.resume_actor("a1") for _ in range(6)]
    )
    worker_ids = {a.worker_id for a in assignments}
    assert len(worker_ids) == 1  # one worker, one provision
    actor = await orchestrator.get_actor("a1")
    assert actor.status == ActorStatus.RUNNING


async def test_concurrent_resume_of_distinct_actors_provision_independently(
    orchestrator,
):
    """Concurrent resumes of *different* actors do not dedup against
    each other — each gets its own worker."""
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.create_actor("a2", "bash")
    a1, a2 = await asyncio.gather(
        orchestrator.resume_actor("a1"),
        orchestrator.resume_actor("a2"),
    )
    assert a1.worker_id != a2.worker_id


# ── pause (node-local checkpoint + locality hint) ─────────────


async def test_pause_records_locality_hint(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    snapshot_ref = await orchestrator.pause_actor("a1")
    assert snapshot_ref
    actor = await orchestrator.get_actor("a1")
    assert actor.status == ActorStatus.PAUSED
    assert actor.worker_assignment is None
    assert actor.latest_snapshot_ref == snapshot_ref
    # Locality hint recorded for same-node resume preference.
    assert actor.worker_selector.get("__pause_node__") == "test-node"


async def test_resume_after_pause_restores_state(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    ws = await _worker_of(orchestrator, "a1")
    _seed_workdir(ws, {"paused-state": b"preserved"})
    await orchestrator.pause_actor("a1")
    await orchestrator.resume_actor("a1")
    ws2 = await _worker_of(orchestrator, "a1")
    assert _read_workdir_file(ws2, "paused-state") == b"preserved"


async def test_pause_rejects_non_running_actor(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    with pytest.raises(ActorStateError, match="cannot pause"):
        await orchestrator.pause_actor("a1")


# ── crash recovery ────────────────────────────────────────────


async def test_mark_crashed_clears_assignment(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    await orchestrator.mark_crashed("a1")
    actor = await orchestrator.get_actor("a1")
    assert actor.status == ActorStatus.CRASHED
    assert actor.worker_assignment is None
    assert await orchestrator._router.resolve(actor.ref) is None


async def test_resume_after_crash_rebinds_fresh_worker(orchestrator):
    """A crashed actor can be resumed — it re-binds a fresh worker and
    restores its latest durable snapshot (if any)."""
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    ws = await _worker_of(orchestrator, "a1")
    _seed_workdir(ws, {"pre-crash": b"survives"})
    # Suspend first to establish a durable snapshot, then crash the
    # resumed actor to simulate a worker loss mid-activation.
    await orchestrator.suspend_actor("a1")
    await orchestrator.resume_actor("a1")
    await orchestrator.mark_crashed("a1")

    assignment = await orchestrator.resume_actor("a1")
    actor = await orchestrator.get_actor("a1")
    assert actor.status == ActorStatus.RUNNING
    assert assignment.worker_id  # re-bound to a fresh worker
    ws2 = await _worker_of(orchestrator, "a1")
    # The pre-crash state was preserved via the durable snapshot.
    assert _read_workdir_file(ws2, "pre-crash") == b"survives"


# ── delete ────────────────────────────────────────────────────


async def test_delete_suspended_actor_succeeds(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.delete_actor("a1")
    from agentscope_sandbox_ext._orchestration import ActorNotFound

    with pytest.raises(ActorNotFound):
        await orchestrator.get_actor("a1")


async def test_delete_running_actor_is_rejected(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    with pytest.raises(ActorStateError, match="suspend first"):
        await orchestrator.delete_actor("a1")


async def test_delete_removes_router_binding(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    await orchestrator.suspend_actor("a1")
    await orchestrator.delete_actor("a1")
    # No binding lingers for the deleted actor.
    bindings = await orchestrator._router.list_bindings()
    assert all("a1" not in k for k in bindings)


# ── metrics ───────────────────────────────────────────────────


async def test_metrics_reports_actor_counts_by_status(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.create_actor("a2", "bash")
    await orchestrator.resume_actor("a1")
    m = await orchestrator.metrics()
    assert m["actors_total"] == 2
    assert m["actors_by_status"].get("running") == 1
    assert m["actors_by_status"].get("suspended") == 1
    assert m["templates_registered"] == 2
    assert m["golden_snapshots"] == 1
    assert m["router_bindings"] == 1


async def test_metrics_singleflight_reports_no_in_flight_when_idle(orchestrator):
    await orchestrator.create_actor("a1", "bash")
    await orchestrator.resume_actor("a1")
    m = await orchestrator.metrics()
    assert m["singleflight_in_flight"] == []


# ── full lifecycle smoke test ─────────────────────────────────


async def test_full_lifecycle_create_resume_suspend_resume_delete(orchestrator):
    """An end-to-end walk through every state transition, exercising
    the CAS state machine, the workflow engine, the worker pool, the
    checkpoint bridge, and the router in one test."""
    # 1. Create — SUSPENDED, no compute.
    await orchestrator.create_actor("a1", "bash")
    assert (await orchestrator.get_actor("a1")).status == ActorStatus.SUSPENDED

    # 2. Resume — RUNNING, worker bound, golden captured.
    await orchestrator.resume_actor("a1")
    assert (await orchestrator.get_actor("a1")).status == ActorStatus.RUNNING
    assert "bash" in orchestrator.golden_snapshots

    # 3. Mutate + suspend — snapshot durable, worker freed.
    ws = await _worker_of(orchestrator, "a1")
    _seed_workdir(ws, {"lifecycle.txt": b"v1"})
    await orchestrator.suspend_actor("a1")
    assert (await orchestrator.get_actor("a1")).status == ActorStatus.SUSPENDED

    # 4. Resume again — restores the snapshot.
    await orchestrator.resume_actor("a1")
    ws2 = await _worker_of(orchestrator, "a1")
    assert _read_workdir_file(ws2, "lifecycle.txt") == b"v1"

    # 5. Pause — node-local checkpoint.
    await orchestrator.pause_actor("a1")
    assert (await orchestrator.get_actor("a1")).status == ActorStatus.PAUSED

    # 6. Resume from pause.
    await orchestrator.resume_actor("a1")
    assert (await orchestrator.get_actor("a1")).status == ActorStatus.RUNNING

    # 7. Suspend + delete — terminal.
    await orchestrator.suspend_actor("a1")
    await orchestrator.delete_actor("a1")
    from agentscope_sandbox_ext._orchestration import ActorNotFound

    with pytest.raises(ActorNotFound):
        await orchestrator.get_actor("a1")
