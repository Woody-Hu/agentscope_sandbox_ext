# -*- coding: utf-8 -*-
"""Worker pool + scheduler — standby workers with CAS assignment.

Composes the existing :class:`SandboxPool` (the standby-sandbox
mechanism) with the actor/worker assignment contract from the
reference runtime: a :class:`Worker` record wraps a live
:class:`SandboxedWorkspaceExtBase`, the :class:`Scheduler` picks an
idle worker matching :class:`Constraints`, and the
:class:`WorkerStore.claim` CAS guarantees at-most-one-actor-per-worker.

Two acquisition paths:

1. **Warm-idle reuse** — a recently-released worker whose sandbox is
   still alive sits in the :class:`WorkerStore` as idle.  The scheduler
   picks it (random among eligible), the orchestrator claims it via
   CAS.  This is the fast path — no :class:`SandboxPool` acquire.
2. **Fresh materialisation** — no warm-idle worker matches, so a fresh
   sandbox is acquired from :class:`SandboxPool` and registered as a
   new worker.

On release the worker either returns to the warm-idle registry (fast
re-acquire for the next actor) or, if the registry is full or the
worker is being drained, its sandbox is returned to
:class:`SandboxPool` and the worker record is deleted.  A background
sweeper retires warm-idle workers past ``idle_ttl`` so memory is not
held forever — the analogue of the reference runtime's HPA scale-down.
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from typing import Any

from agentscope._logging import logger

from .._base import SandboxedWorkspaceExtBase
from .._pool import SandboxPool
from .model import (
    Constraints,
    SandboxClass,
    Worker,
    WorkerAssignment,
    WorkerState,
)
from .store import WorkerBusy, WorkerNotFound, WorkerStore

#: Default cap on warm-idle workers kept between activations.
DEFAULT_MAX_WARM_IDLE = 4

#: Default idle TTL (seconds) before a warm-idle worker is retired.
DEFAULT_WARM_IDLE_TTL = 300.0

#: Default sweep interval (seconds) for the warm-idle retire loop.
DEFAULT_SWEEP_INTERVAL = 60.0


# ── sandbox-class inference ─────────────────────────────────────

#: Maps a backend's ``sandbox_kind`` to the orchestration
#: :class:`SandboxClass`.  Snapshots are not portable across classes,
#: so this mapping is what makes the scheduler's class constraint work.
_KIND_TO_CLASS: dict[str, SandboxClass] = {
    "firecracker": SandboxClass.MICROVM,
    "gvisor": SandboxClass.CONTAINER,
    "kata": SandboxClass.CONTAINER,
    "sysbox": SandboxClass.CONTAINER,
    "docker": SandboxClass.CONTAINER,
    "agentfs": SandboxClass.VFS,
    "vfs": SandboxClass.VFS,
}


def infer_sandbox_class(sandbox: SandboxedWorkspaceExtBase) -> SandboxClass:
    """Infer the :class:`SandboxClass` from a sandbox's ``sandbox_kind``."""
    kind = getattr(sandbox, "sandbox_kind", "vfs")
    return _KIND_TO_CLASS.get(kind, SandboxClass.VFS)


# ── scheduler ───────────────────────────────────────────────────


class Scheduler:
    """Pick an idle worker for a set of constraints.

    Mirrors the reference runtime's deliberate simplicity: filter the
    candidate list by :class:`Constraints`, then pick one **uniformly
    at random**.  No bin-packing, no load balancing — the contention
    is resolved by the CAS claim, not by the scheduler being clever.
    """

    def __init__(self, *, rng: random.Random | None = None) -> None:
        self._rng = rng or random.Random()

    async def pick(
        self,
        workers: list[Worker],
        constraints: Constraints,
    ) -> Worker | None:
        """Return a random eligible worker, or ``None`` if none match."""
        eligible = [w for w in workers if _satisfies(w, constraints)]
        if not eligible:
            return None
        return self._rng.choice(eligible)


def _satisfies(worker: Worker, constraints: Constraints) -> bool:
    """True if *worker* can serve *constraints* (all fields AND'd)."""
    if worker.sandbox_class != constraints.sandbox_class:
        return False
    if not worker.is_idle:
        return False
    if constraints.required_node is not None:
        if worker.node != constraints.required_node:
            return False
    for k, v in constraints.template_selector.items():
        if worker.labels.get(k) != v:
            return False
    for k, v in constraints.actor_selector.items():
        if worker.labels.get(k) != v:
            return False
    return True


# ── worker pool ─────────────────────────────────────────────────


class WorkerPool:
    """Standby worker registry composing :class:`SandboxPool`.

    The pool keeps a small set of *warm-idle* workers — recently
    released workers whose sandboxes are still alive — so a follow-up
    activation of any actor reuses a warm sandbox instead of paying the
    :class:`SandboxPool` acquire cost.  Warm-idle workers past
    ``idle_ttl`` are retired (their sandboxes returned to
    :class:`SandboxPool`) by a background sweeper.

    Args:
        store (`WorkerStore`):
            The worker-record store (CAS claim/release).
        sandbox_pool (`SandboxPool`):
            The standby-sandbox pool.  Used to materialise fresh
            workers and to absorb retired workers' sandboxes.
        scheduler (`Scheduler`):
            Picks among warm-idle workers.
        max_warm_idle (`int`, defaults to :data:`DEFAULT_MAX_WARM_IDLE`):
            Cap on warm-idle workers kept between activations.
        idle_ttl (`float`, defaults to :data:`DEFAULT_WARM_IDLE_TTL`):
            Seconds a warm-idle worker may sit before the sweeper
            retires it.
        sweep_interval (`float`, defaults to :data:`DEFAULT_SWEEP_INTERVAL`):
            How often the retire sweeper wakes up.
        labels (`dict[str, str]`, optional):
            Labels stamped onto every worker created by this pool (used
            for template/actor selector matching).
        node (`str | None`, optional):
            Node identity stamped onto every worker (for locality).
    """

    def __init__(
        self,
        store: WorkerStore,
        sandbox_pool: SandboxPool,
        scheduler: Scheduler,
        *,
        max_warm_idle: int = DEFAULT_MAX_WARM_IDLE,
        idle_ttl: float = DEFAULT_WARM_IDLE_TTL,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL,
        labels: dict[str, str] | None = None,
        node: str | None = None,
    ) -> None:
        self._store = store
        self._sandbox_pool = sandbox_pool
        self._scheduler = scheduler
        self._max_warm_idle = max_warm_idle
        self._idle_ttl = idle_ttl
        self._sweep_interval = sweep_interval
        self._labels = dict(labels or {})
        self._node = node
        # worker_id -> last_return_monotonic, for the retire sweeper.
        self._warm_idle_since: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._sweep_task: asyncio.Task | None = None
        self._closed = False

    # ── lifecycle ───────────────────────────────────────────────

    async def start(self) -> None:
        """Start the warm-idle retire sweeper.  Idempotent."""
        if self._sweep_task is None and self._sweep_interval > 0:
            self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def aclose(self) -> None:
        """Stop the sweeper and retire every warm-idle worker."""
        if self._closed:
            return
        self._closed = True
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except (asyncio.CancelledError, Exception):
                pass
            self._sweep_task = None
        # Retire every warm-idle worker so sandboxes return to the pool.
        async with self._lock:
            warm_ids = list(self._warm_idle_since.keys())
            self._warm_idle_since.clear()
        for wid in warm_ids:
            try:
                worker = await self._store.get(wid)
                await self._retire(worker)
            except WorkerNotFound:
                pass
            except Exception:
                logger.exception("WorkerPool.aclose: retire %s failed", wid)

    # ── public API ──────────────────────────────────────────────

    async def acquire_worker(self, constraints: Constraints) -> Worker:
        """Return an idle worker matching *constraints* (unassigned).

        Tries the warm-idle registry first (scheduler picks among
        eligible idle workers); on miss, materialises a fresh worker
        from :class:`SandboxPool`.  The returned worker is idle — the
        caller binds an actor via :meth:`WorkerStore.claim`.

        Raises:
            asyncio.TimeoutError: If :class:`SandboxPool.acquire` times
                out (pool saturated).
        """
        # Fast path: warm-idle worker matching constraints.
        idle = await self._store.list_idle(constraints)
        if idle:
            picked = await self._scheduler.pick(idle, constraints)
            if picked is not None:
                async with self._lock:
                    self._warm_idle_since.pop(picked.worker_id, None)
                return picked

        # Materialise a fresh worker from the standby sandbox pool.
        sandbox = await self._sandbox_pool.acquire()
        sandbox_class = infer_sandbox_class(sandbox)
        worker = Worker(
            worker_id=f"w-{uuid.uuid4().hex[:12]}",
            state=WorkerState.ACTIVE,
            sandbox_class=sandbox_class,
            labels=dict(self._labels),
            node=self._node,
            sandbox=sandbox,
        )
        registered = await self._store.register(worker)
        logger.debug(
            "WorkerPool: materialised worker %s (class=%s)",
            registered.worker_id,
            sandbox_class.value,
        )
        return registered

    async def claim_for_actor(
        self,
        worker: Worker,
        assignment: WorkerAssignment,
    ) -> Worker:
        """CAS-claim *worker* for *assignment*.

        Thin wrapper around :meth:`WorkerStore.claim` so callers do not
        need to track the expected version themselves.  On
        :class:`VersionConflict` / :class:`WorkerBusy` the caller is
        expected to re-acquire a different worker.
        """
        return await self._store.claim(
            worker.worker_id,
            assignment,
            expected_version=worker.version,
        )

    async def release_worker(self, worker: Worker, *, drain: bool = False) -> None:
        """Release the actor binding on *worker*.

        If *drain* is True (or the sandbox is dead), the worker is
        retired: its sandbox is returned to :class:`SandboxPool` and the
        worker record is deleted.  Otherwise the worker returns to the
        warm-idle registry (subject to the cap) for fast re-acquire.
        """
        try:
            released = await self._store.release(
                worker.worker_id, expected_version=worker.version
            )
        except WorkerNotFound:
            return

        if drain or not _sandbox_alive(released):
            await self._retire(released)
            return

        async with self._lock:
            warm_count = len(self._warm_idle_since)
            if warm_count >= self._max_warm_idle:
                retire = True
            else:
                retire = False
                self._warm_idle_since[released.worker_id] = time.monotonic()

        if retire:
            await self._retire(released)
        else:
            logger.debug(
                "WorkerPool: worker %s returned to warm-idle",
                released.worker_id,
            )

    async def warm_idle_count(self) -> int:
        """Number of workers currently in the warm-idle registry."""
        async with self._lock:
            return len(self._warm_idle_since)

    async def metrics(self) -> dict[str, Any]:
        async with self._lock:
            warm = len(self._warm_idle_since)
        idle = await self._store.list_idle(
            Constraints(sandbox_class=SandboxClass.VFS)
        )
        return {
            "warm_idle": warm,
            "max_warm_idle": self._max_warm_idle,
            "idle_ttl": self._idle_ttl,
            "store_idle_total": len(idle) if idle else 0,
            "closed": self._closed,
        }

    # ── internals ───────────────────────────────────────────────

    async def _retire(self, worker: Worker) -> None:
        """Return the worker's sandbox to the pool and delete the record."""
        async with self._lock:
            self._warm_idle_since.pop(worker.worker_id, None)
        sandbox = worker.sandbox
        try:
            await self._store.delete(worker.worker_id)
        except Exception:
            logger.exception(
                "WorkerPool: failed to delete worker %s", worker.worker_id
            )
        if sandbox is not None:
            try:
                await self._sandbox_pool.release(sandbox)
            except Exception:
                logger.exception(
                    "WorkerPool: failed to return sandbox %s to pool",
                    getattr(sandbox, "workspace_id", "?"),
                )

    async def _sweep_loop(self) -> None:
        """Retire warm-idle workers past ``idle_ttl``."""
        while not self._closed:
            try:
                await asyncio.sleep(self._sweep_interval)
            except asyncio.CancelledError:
                return
            try:
                await self._sweep_once()
            except Exception:
                logger.exception("WorkerPool sweeper tick failed")

    async def _sweep_once(self) -> None:
        now = time.monotonic()
        async with self._lock:
            expired_ids = [
                wid
                for wid, since in self._warm_idle_since.items()
                if now - since > self._idle_ttl
            ]
            for wid in expired_ids:
                self._warm_idle_since.pop(wid, None)
        for wid in expired_ids:
            try:
                worker = await self._store.get(wid)
                await self._retire(worker)
                logger.debug("WorkerPool: retired idle worker %s", wid)
            except WorkerNotFound:
                pass
            except Exception:
                logger.exception(
                    "WorkerPool: retire of idle worker %s failed", wid
                )


def _sandbox_alive(worker: Worker) -> bool:
    """True if the worker's sandbox is still usable."""
    sandbox = worker.sandbox
    if sandbox is None:
        return False
    return bool(getattr(sandbox, "is_alive", False))


__all__ = [
    "Scheduler",
    "WorkerPool",
    "infer_sandbox_class",
    "DEFAULT_MAX_WARM_IDLE",
    "DEFAULT_WARM_IDLE_TTL",
    "DEFAULT_SWEEP_INTERVAL",
]
