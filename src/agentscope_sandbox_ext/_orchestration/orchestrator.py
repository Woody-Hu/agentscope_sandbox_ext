# -*- coding: utf-8 -*-
"""Orchestrator façade — actor lifecycle on top of workers + checkpoints.

Ties together :class:`ActorStore`, :class:`WorkerPool`,
:class:`CheckpointBridge`, :class:`Router`, a template registry, and
:class:`Singleflight` to expose the actor lifecycle the reference
runtime popularised:

* :meth:`create_actor` — a cheap ``SUSPENDED`` record owning no compute.
* :meth:`resume_actor` — idempotent resume wrapped in singleflight:
  claim a worker, restore (or cold-boot / restore a golden snapshot),
  flip to ``RUNNING``.  Concurrent resumes of the same actor collapse
  onto one provision.
* :meth:`suspend_actor` — durable checkpoint + free the worker.
* :meth:`pause_actor` — node-local checkpoint + free the worker
  (resume prefers the same node).
* :meth:`delete_actor` — terminal.

The resume path is implemented as an idempotent :class:`Workflow`
(:mod:`.lifecycle`) so a crash mid-resume can be re-run without
double-provisioning.  Every actor-state transition is CAS-guarded by
:class:`ActorStore.put(expected_version=...)`.
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
from typing import Any

from agentscope._logging import logger

from .checkpoint import CheckpointBridge, CheckpointError
from .lifecycle import Step, Workflow, WorkflowContext, WorkflowError
from .model import (
    Actor,
    ActorStatus,
    ActorTemplate,
    CheckpointScope,
    Constraints,
    SandboxClass,
    Worker,
    WorkerAssignment,
)
from .router import Router
from .singleflight import Singleflight
from .store import ActorNotFound, ActorStore, VersionConflict, WorkerBusy
from .worker_pool import WorkerPool

#: Resume workflow key prefix for singleflight dedup.
_RESUME_KEY = "resume:{namespace}:{actor_id}"

#: Caps on CAS retry attempts for worker assignment (mirrors the
#: reference runtime's 5-step / 10ms×2 backoff resume workflow).
_MAX_CLAIM_RETRIES = 5
_CLAIM_BACKOFF_BASE = 0.01


class ActorStateError(Exception):
    """Raised when a lifecycle transition is requested from an invalid state."""


class TemplateNotFound(KeyError):
    """Raised when a referenced template is not registered."""


class Orchestrator:
    """Actor lifecycle façade.

    Args:
        actor_store: CAS store for :class:`Actor` records.
        worker_pool: Standby worker registry + scheduler.
        checkpoint: Durable checkpoint bridge.
        router: Active-actor routing table.
        templates: Registered :class:`ActorTemplate` specs, keyed by id.
        singleflight: Optional dedup layer around :meth:`resume_actor`.
            If ``None``, a default :class:`Singleflight` is created.
        golden_snapshots: Optional pre-seeded golden snapshot refs per
            template id (otherwise populated lazily on first boot).
    """

    def __init__(
        self,
        *,
        actor_store: ActorStore,
        worker_pool: WorkerPool,
        checkpoint: CheckpointBridge,
        router: Router,
        templates: dict[str, ActorTemplate],
        singleflight: Singleflight | None = None,
        golden_snapshots: dict[str, str] | None = None,
    ) -> None:
        self._actors = actor_store
        self._workers = worker_pool
        self._checkpoint = checkpoint
        self._router = router
        self._templates = dict(templates)
        self._singleflight = singleflight or Singleflight()
        # Mutable golden-snapshot registry (the template spec is frozen;
        # the golden ref is populated lazily on first materialisation).
        self._golden: dict[str, str] = dict(golden_snapshots or {})

    # ── template registry ───────────────────────────────────────

    def register_template(self, template: ActorTemplate) -> None:
        """Register an :class:`ActorTemplate`."""
        self._templates[template.template_id] = template

    def get_template(self, template_id: str) -> ActorTemplate:
        """Return a registered template or raise :class:`TemplateNotFound`."""
        try:
            return self._templates[template_id]
        except KeyError as exc:
            raise TemplateNotFound(template_id) from exc

    @property
    def golden_snapshots(self) -> dict[str, str]:
        """Current golden snapshot refs per template id (observability)."""
        return dict(self._golden)

    # ── actor CRUD ──────────────────────────────────────────────

    async def create_actor(
        self,
        actor_id: str,
        template_id: str,
        *,
        namespace: str = "default",
        worker_selector: dict[str, str] | None = None,
    ) -> Actor:
        """Create a ``SUSPENDED`` actor owning no compute.

        Raises :class:`ActorStateError` if the actor already exists.
        """
        if template_id not in self._templates:
            raise TemplateNotFound(template_id)
        now = time.time()
        actor = Actor(
            actor_id=actor_id,
            namespace=namespace,
            template_id=template_id,
            status=ActorStatus.SUSPENDED,
            worker_selector=dict(worker_selector or {}),
            created_at=now,
            updated_at=now,
        )
        try:
            return await self._actors.put(actor, expected_version=None)
        except VersionConflict as exc:
            raise ActorStateError(
                f"actor {actor_id!r} already exists"
            ) from exc

    async def get_actor(
        self, actor_id: str, *, namespace: str = "default"
    ) -> Actor:
        """Return the actor record."""
        return await self._actors.get(actor_id, namespace=namespace)

    async def list_actors(self, namespace: str | None = None) -> list[Actor]:
        """List actors, optionally filtered by namespace."""
        return await self._actors.list(namespace)

    async def delete_actor(
        self, actor_id: str, *, namespace: str = "default"
    ) -> None:
        """Delete an actor (only from a quiescent state).

        Raises :class:`ActorStateError` if the actor is still active —
        suspend it first.
        """
        actor = await self._actors.get(actor_id, namespace=namespace)
        if actor.status in (ActorStatus.RUNNING, ActorStatus.RESUMING):
            raise ActorStateError(
                f"cannot delete actor {actor_id!r} in state {actor.status.value}; "
                f"suspend first"
            )
        # CAS flip to DELETING (best-effort; a concurrent resume would
        # conflict here, which is the intended back-pressure).
        deleting = _with_status(actor, ActorStatus.DELETING)
        try:
            await self._actors.put(deleting, expected_version=actor.version)
        except ActorNotFound:
            # Actor vanished between the read above and the CAS — treat
            # as already deleted and continue to the final delete call.
            pass
        except VersionConflict:
            # Someone else mutated it; re-check and refuse if active.
            fresh = await self._actors.get(actor_id, namespace=namespace)
            if fresh.is_active:
                raise ActorStateError(
                    f"actor {actor_id!r} became active during delete"
                )
        await self._router.unbind(actor.ref)
        await self._actors.delete(actor_id, namespace=namespace)

    # ── resume ──────────────────────────────────────────────────

    async def resume_actor(
        self,
        actor_id: str,
        *,
        namespace: str = "default",
        budget: float = 30.0,
    ) -> WorkerAssignment:
        """Resume an actor onto a worker.  Idempotent + singleflighted.

        Concurrent resumes of the same actor collapse onto one
        in-flight provision (the leader's budget is detached from any
        single caller).  If the actor is already ``RUNNING``, returns
        its existing assignment without re-provisioning.
        """
        # Read once to build the singleflight key (namespace-scoped).
        actor = await self._actors.get(actor_id, namespace=namespace)
        key = _RESUME_KEY.format(
            namespace=actor.namespace, actor_id=actor.actor_id
        )
        return await self._singleflight.run(
            key,
            lambda: self._resume_actor_impl(actor_id, namespace=namespace),
            budget=budget,
        )

    async def _resume_actor_impl(
        self, actor_id: str, *, namespace: str = "default"
    ) -> WorkerAssignment:
        """The actual resume workflow, run under singleflight."""
        actor = await self._actors.get(actor_id, namespace=namespace)
        # Idempotent short-circuit: if the actor is already RUNNING with
        # a live assignment, return it without re-running the workflow.
        # The workflow's ``_LoadActorStep.is_complete`` is meant to do
        # this, but the workflow engine still evaluates subsequent steps
        # (``_ClaimWorkerStep`` checks ``ctx.data["worker"]``, which the
        # short-circuit path never sets) — so an early return here is
        # both cheaper and correct.
        if actor.status == ActorStatus.RUNNING and actor.worker_assignment:
            return actor.worker_assignment
        template = self.get_template(actor.template_id)

        ctx = WorkflowContext(actor_id=actor_id, namespace=namespace)
        workflow = Workflow(
            "resume",
            [
                _LoadActorStep(self),
                _ClaimWorkerStep(self, template),
                _RestoreOrBootStep(self, template),
                _FinalizeRunningStep(self, template),
            ],
        )
        await workflow.run(ctx)
        assignment = ctx.data.get("assignment")
        if assignment is None:  # pragma: no cover - defensive
            raise WorkflowError(
                f"resume workflow for {namespace}/{actor_id!r} did not "
                f"produce an assignment"
            )
        return assignment

    # ── suspend / pause ─────────────────────────────────────────

    async def suspend_actor(
        self,
        actor_id: str,
        *,
        namespace: str = "default",
        scope: CheckpointScope | None = None,
    ) -> str:
        """Checkpoint the actor and free its worker.

        Returns the durable ``snapshot_ref``.  The worker returns to
        the warm-idle registry (or is retired); the actor becomes
        ``SUSPENDED`` and can be resumed later into a different worker.
        """
        actor = await self._actors.get(actor_id, namespace=namespace)
        if actor.status != ActorStatus.RUNNING:
            raise ActorStateError(
                f"cannot suspend actor {actor_id!r} in state {actor.status.value}"
            )
        template = self.get_template(actor.template_id)
        eff_scope = scope or template.snapshot_scope_on_suspend
        assignment = actor.worker_assignment
        if assignment is None:  # pragma: no cover - invariant
            raise ActorStateError(
                f"running actor {actor_id!r} has no worker assignment"
            )

        # CAS flip to SUSPENDING so a concurrent resume sees the state.
        suspending = _with_status(actor, ActorStatus.SUSPENDING)
        try:
            actor = await self._actors.put(
                suspending, expected_version=actor.version
            )
        except VersionConflict as exc:
            raise ActorStateError(
                f"actor {actor_id!r} changed concurrently; retry suspend"
            ) from exc

        # Checkpoint + free the worker.
        worker = await self._workers._store.get(assignment.worker_id)
        snapshot_ref = await self._checkpoint.checkpoint(
            actor, worker, eff_scope
        )
        await self._router.unbind(actor.ref)
        await self._workers.release_worker(worker)

        # CAS flip to SUSPENDED with the new snapshot ref.
        suspended = _with_status(actor, ActorStatus.SUSPENDED)
        suspended.latest_snapshot_ref = snapshot_ref
        suspended.worker_assignment = None
        try:
            await self._actors.put(suspended, expected_version=actor.version)
        except VersionConflict:
            # Best-effort: the snapshot is durable; the actor record
            # will be reconciled by the next resume.
            logger.warning(
                "suspend_actor %s: CAS conflict on final flip; snapshot %s is durable",
                actor_id,
                snapshot_ref,
            )
        return snapshot_ref

    async def pause_actor(
        self, actor_id: str, *, namespace: str = "default"
    ) -> str:
        """Node-local checkpoint + free the worker.

        Like :meth:`suspend_actor` but the snapshot is ``DATA`` scope
        and the actor records the node so a later resume prefers it
        (locality).  Use pause for short idle periods where you want
        fast same-node resume; use suspend for long idle periods.
        """
        actor = await self._actors.get(actor_id, namespace=namespace)
        if actor.status != ActorStatus.RUNNING:
            raise ActorStateError(
                f"cannot pause actor {actor_id!r} in state {actor.status.value}"
            )
        assignment = actor.worker_assignment
        if assignment is None:  # pragma: no cover - invariant
            raise ActorStateError(
                f"running actor {actor_id!r} has no worker assignment"
            )

        pausing = _with_status(actor, ActorStatus.PAUSING)
        actor = await self._actors.put(
            pausing, expected_version=actor.version
        )
        worker = await self._workers._store.get(assignment.worker_id)
        snapshot_ref = await self._checkpoint.checkpoint(
            actor, worker, CheckpointScope.DATA
        )
        await self._router.unbind(actor.ref)
        await self._workers.release_worker(worker)

        paused = _with_status(actor, ActorStatus.PAUSED)
        paused.latest_snapshot_ref = snapshot_ref
        paused.worker_assignment = None
        # Record locality so resume prefers this node.
        paused.worker_selector = dict(paused.worker_selector)
        if assignment.node:
            paused.worker_selector.setdefault("__pause_node__", assignment.node)
        await self._actors.put(paused, expected_version=actor.version)
        return snapshot_ref

    # ── crash recovery ──────────────────────────────────────────

    async def mark_crashed(
        self, actor_id: str, *, namespace: str = "default"
    ) -> None:
        """Mark an actor ``CRASHED`` (e.g. its worker was lost).

        A crashed actor can be resumed (re-binds a fresh worker) but
        not suspended (its worker is already gone).  The durable
        snapshot is preserved for resume.
        """
        actor = await self._actors.get(actor_id, namespace=namespace)
        crashed = _with_status(actor, ActorStatus.CRASHED)
        crashed.worker_assignment = None
        await self._router.unbind(actor.ref)
        try:
            await self._actors.put(crashed, expected_version=actor.version)
        except VersionConflict:
            logger.warning(
                "mark_crashed %s: CAS conflict; state may have changed",
                actor_id,
            )

    # ── metrics ─────────────────────────────────────────────────

    async def metrics(self) -> dict[str, Any]:
        """Return orchestration-wide observability fields."""
        actors = await self._actors.list()
        return {
            "actors_total": len(actors),
            "actors_by_status": _count_by_status(actors),
            "templates_registered": len(self._templates),
            "golden_snapshots": len(self._golden),
            "singleflight_in_flight": list(self._singleflight.in_flight_keys),
            "worker_pool": await self._workers.metrics(),
            "router_bindings": len(await self._router.list_bindings()),
        }


# ── resume workflow steps ───────────────────────────────────────


class _LoadActorStep(Step):
    """Load the actor; if already active, short-circuit the workflow."""

    name = "load-actor"

    def __init__(self, orch: Orchestrator) -> None:
        self._orch = orch

    async def is_complete(self, ctx: WorkflowContext) -> bool:
        actor = await self._orch._actors.get(
            ctx.actor_id, namespace=ctx.namespace
        )
        if actor.status == ActorStatus.RUNNING and actor.worker_assignment:
            ctx.data["assignment"] = actor.worker_assignment
            ctx.data["actor"] = actor
            return True
        return False

    async def execute(self, ctx: WorkflowContext) -> None:
        actor = await self._orch._actors.get(
            ctx.actor_id, namespace=ctx.namespace
        )
        if actor.status not in (
            ActorStatus.SUSPENDED,
            ActorStatus.PAUSED,
            ActorStatus.CRASHED,
            ActorStatus.RESUMING,
        ):
            raise ActorStateError(
                f"cannot resume actor {ctx.namespace}/{ctx.actor_id!r} "
                f"from {actor.status.value}"
            )
        ctx.data["actor"] = actor


class _ClaimWorkerStep(Step):
    """Acquire a worker matching the template's constraints and claim it."""

    name = "claim-worker"

    def __init__(self, orch: Orchestrator, template: ActorTemplate) -> None:
        self._orch = orch
        self._template = template

    async def is_complete(self, ctx: WorkflowContext) -> bool:
        return ctx.data.get("worker") is not None

    async def execute(self, ctx: WorkflowContext) -> None:
        actor: Actor = ctx.data["actor"]
        constraints = self._constraints_for(actor)
        # Flip the actor to RESUMING first so concurrent observers see
        # the in-flight transition (CAS; tolerate a conflict if the
        # workflow is re-running after a crash).
        try:
            resuming = _with_status(actor, ActorStatus.RESUMING)
            actor = await self._orch._actors.put(
                resuming, expected_version=actor.version
            )
            ctx.data["actor"] = actor
        except VersionConflict:
            actor = await self._orch._actors.get(
                ctx.actor_id, namespace=ctx.namespace
            )
            if actor.status != ActorStatus.RESUMING:
                raise ActorStateError(
                    f"actor {ctx.namespace}/{ctx.actor_id!r} state changed "
                    f"during resume"
                )
            ctx.data["actor"] = actor

        # Acquire + claim with retry on CAS contention (the reference
        # runtime retries 5× with 10ms×2 backoff).
        last_err: Exception | None = None
        for attempt in range(_MAX_CLAIM_RETRIES):
            worker = await self._orch._workers.acquire_worker(constraints)
            assignment = WorkerAssignment(
                actor_id=actor.actor_id,
                worker_id=worker.worker_id,
                sandbox_class=worker.sandbox_class,
                node=worker.node,
                activated_at=time.time(),
            )
            try:
                claimed = await self._orch._workers.claim_for_actor(
                    worker, assignment
                )
                ctx.data["worker"] = claimed
                return
            except (VersionConflict, WorkerBusy) as exc:
                last_err = exc
                # The worker was grabbed by another actor; retire it
                # (return its sandbox to the pool) and try a fresh one.
                await self._orch._workers.release_worker(worker, drain=True)
                await asyncio.sleep(
                    _CLAIM_BACKOFF_BASE * (2**attempt)
                )
        raise WorkflowError(
            f"could not claim a worker for {ctx.actor_id!r} after "
            f"{_MAX_CLAIM_RETRIES} attempts: {last_err}"
        )

    def _constraints_for(self, actor: Actor) -> Constraints:
        required_node = actor.worker_selector.get("__pause_node__")
        return Constraints(
            sandbox_class=self._template.sandbox_class,
            template_selector={
                k: v
                for k, v in actor.worker_selector.items()
                if k != "__pause_node__"
            },
            required_node=required_node,
        )


class _RestoreOrBootStep(Step):
    """Restore a snapshot if present; else cold-boot (or restore golden)."""

    name = "restore-or-boot"

    def __init__(self, orch: Orchestrator, template: ActorTemplate) -> None:
        self._orch = orch
        self._template = template

    async def is_complete(self, ctx: WorkflowContext) -> bool:
        return ctx.data.get("restored") is True

    async def execute(self, ctx: WorkflowContext) -> None:
        actor: Actor = ctx.data["actor"]
        worker: Worker = ctx.data["worker"]
        # The sandbox from SandboxPool is already initialised (provisioned
        # + seeded).  "Cold-boot" is therefore just using it as-is.
        # 1. If the actor has its own snapshot, restore it.
        if actor.latest_snapshot_ref:
            await self._orch._checkpoint.restore(
                actor, worker, actor.latest_snapshot_ref
            )
            ctx.data["restored"] = True
            return
        # 2. If the template has a golden snapshot, restore that.
        golden = self._orch._golden.get(self._template.template_id)
        if golden:
            try:
                await self._orch._checkpoint.restore(actor, worker, golden)
                ctx.data["restored"] = True
                return
            except CheckpointError:
                logger.warning(
                    "resume %s: golden snapshot %s missing; cold-booting",
                    ctx.actor_id,
                    golden,
                )
        # 3. Cold-boot: capture a golden snapshot for future actors of
        #    this template (first-materialisation win).
        ctx.data["restored"] = True
        if self._template.template_id not in self._orch._golden:
            await self._capture_golden(actor, worker)

    async def _capture_golden(self, actor: Actor, worker: Worker) -> None:
        try:
            ref = await self._orch._checkpoint.checkpoint(
                actor, worker, CheckpointScope.DATA
            )
            self._orch._golden[self._template.template_id] = ref
            logger.debug(
                "captured golden snapshot for template %s -> %s",
                self._template.template_id,
                ref,
            )
        except Exception:
            logger.exception(
                "failed to capture golden snapshot for template %s",
                self._template.template_id,
            )


class _FinalizeRunningStep(Step):
    """Flip the actor to RUNNING, bind the router, store the assignment."""

    name = "finalize-running"

    def __init__(self, orch: Orchestrator, template: ActorTemplate) -> None:
        self._orch = orch
        self._template = template

    async def is_complete(self, ctx: WorkflowContext) -> bool:
        actor = await self._orch._actors.get(
            ctx.actor_id, namespace=ctx.namespace
        )
        if actor.status == ActorStatus.RUNNING and actor.worker_assignment:
            ctx.data["assignment"] = actor.worker_assignment
            return True
        return False

    async def execute(self, ctx: WorkflowContext) -> None:
        actor: Actor = ctx.data["actor"]
        worker: Worker = ctx.data["worker"]
        assignment = WorkerAssignment(
            actor_id=actor.actor_id,
            worker_id=worker.worker_id,
            sandbox_class=worker.sandbox_class,
            node=worker.node,
            activated_at=time.time(),
        )
        running = _with_status(actor, ActorStatus.RUNNING)
        running.worker_assignment = assignment
        try:
            actor = await self._orch._actors.put(
                running, expected_version=actor.version
            )
        except VersionConflict:
            actor = await self._orch._actors.get(
                ctx.actor_id, namespace=ctx.namespace
            )
            if actor.status != ActorStatus.RUNNING:
                raise ActorStateError(
                    f"actor {ctx.namespace}/{ctx.actor_id!r} state changed "
                    f"during finalize"
                )
        await self._orch._router.bind(actor.ref, assignment)
        ctx.data["assignment"] = assignment


# ── helpers ─────────────────────────────────────────────────────


def _with_status(actor: Actor, status: ActorStatus) -> Actor:
    """Return a copy of *actor* with *status* set (preserving version)."""
    return dataclasses.replace(actor, status=status)


def _count_by_status(actors: list[Actor]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for a in actors:
        counts[a.status.value] = counts.get(a.status.value, 0) + 1
    return counts


__all__ = [
    "Orchestrator",
    "ActorStateError",
    "TemplateNotFound",
]
