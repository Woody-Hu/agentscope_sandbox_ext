# -*- coding: utf-8 -*-
"""Agent orchestration runtime — modular actor/worker layer.

An additive orchestration layer on top of the existing per-workspace
sandbox backends.  It borrows five patterns from large-scale agent
runtimes (actor/worker isolation, singleflight, templates + golden
snapshots, standby worker pool, suspend/resume checkpointing) and
exposes them as composable, opt-in modules:

* :class:`Singleflight` — dedup concurrent identical calls.
* :class:`Actor` / :class:`Worker` / :class:`ActorTemplate` — the
  cheap data records the layer operates on.
* :class:`InMemoryActorStore` / :class:`InMemoryWorkerStore` —
  optimistic-CAS state stores (pluggable to Redis).
* :class:`WorkerPool` + :class:`Scheduler` — standby workers with
  at-most-one-actor-per-worker assignment.
* :class:`CheckpointBridge` — durable suspend/resume via the existing
  :class:`~agentscope_sandbox_ext._runtime.SnapshotStore`.
* :class:`Orchestrator` — the façade tying it all together.

Nothing here changes the existing ``SandboxedWorkspaceExtBase`` /
``SandboxExtManagerBase`` / ``SandboxPool`` surface.  See
``docs/ORCHESTRATION.md`` for the full design.

Quickstart
----------

.. code-block:: python

    import asyncio
    from agentscope_sandbox_ext import AgentFSWorkspace, SandboxPool
    from agentscope_sandbox_ext._orchestration import (
        Orchestrator, InMemoryActorStore, InMemoryWorkerStore,
        WorkerPool, Scheduler, CheckpointBridge, InMemoryRouter,
        Singleflight, ActorTemplate, SandboxClass, CheckpointScope,
    )
    from agentscope_sandbox_ext._runtime import LocalSnapshotStore

    async def main():
        pool = SandboxPool(factory=_make_agentfs, max_size=4, min_warm=1)
        await pool.start()
        wp = WorkerPool(InMemoryWorkerStore(), pool, Scheduler())
        await wp.start()
        orch = Orchestrator(
            actor_store=InMemoryActorStore(),
            worker_pool=wp,
            checkpoint=CheckpointBridge(LocalSnapshotStore("/tmp/snaps")),
            router=InMemoryRouter(),
            templates={"bash": ActorTemplate(
                template_id="bash", sandbox_class=SandboxClass.VFS,
                backend_kind="agentfs",
            )},
        )
        await orch.create_actor("a1", "bash")
        assignment = await orch.resume_actor("a1")
        # ... use the live sandbox via the worker ...
        await orch.suspend_actor("a1")

    asyncio.run(main())
"""

from .checkpoint import CheckpointBridge, CheckpointError
from .lifecycle import Step, Workflow, WorkflowContext, WorkflowError
from .model import (
    Actor,
    ActorRef,
    ActorStatus,
    ActorTemplate,
    CheckpointScope,
    Constraints,
    SandboxClass,
    Worker,
    WorkerAssignment,
    WorkerState,
)
from .orchestrator import ActorStateError, Orchestrator, TemplateNotFound
from .router import InMemoryRouter, Router
from .singleflight import BudgetExhausted, Singleflight, SingleflightError
from .store import (
    ActorNotFound,
    ActorStore,
    InMemoryActorStore,
    InMemoryWorkerStore,
    VersionConflict,
    WorkerBusy,
    WorkerNotFound,
    WorkerStore,
)
from .worker_pool import Scheduler, WorkerPool, infer_sandbox_class

__all__ = [
    # singleflight
    "Singleflight",
    "SingleflightError",
    "BudgetExhausted",
    # model
    "Actor",
    "ActorRef",
    "ActorStatus",
    "ActorTemplate",
    "CheckpointScope",
    "Constraints",
    "SandboxClass",
    "Worker",
    "WorkerAssignment",
    "WorkerState",
    # stores
    "ActorStore",
    "InMemoryActorStore",
    "WorkerStore",
    "InMemoryWorkerStore",
    "VersionConflict",
    "ActorNotFound",
    "WorkerNotFound",
    "WorkerBusy",
    # worker pool + scheduler
    "WorkerPool",
    "Scheduler",
    "infer_sandbox_class",
    # checkpoint
    "CheckpointBridge",
    "CheckpointError",
    # router
    "Router",
    "InMemoryRouter",
    # lifecycle
    "Workflow",
    "Step",
    "WorkflowContext",
    "WorkflowError",
    # orchestrator
    "Orchestrator",
    "ActorStateError",
    "TemplateNotFound",
]
