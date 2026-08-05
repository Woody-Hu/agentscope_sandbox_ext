# -*- coding: utf-8 -*-
"""Worker pool: pre-warmed execution units with constraint-based scheduling.

This is the actor–worker isolation layer.  A small pool of pre-warmed
:class:`Worker` instances (each wrapping a :class:`SandboxRuntime`) is
time-multiplexed across a larger set of actors: an actor *resumes* by
borrowing an idle worker that satisfies its :class:`Constraints`, runs,
then *suspends* (or *pauses*) and returns the worker to the pool for the
next actor.  The oversubscription ratio (actors : workers) is the density
win — workers are the scarce resource, actors are not.

The pool reuses the proven patterns from :class:`SandboxPool`
(pre-warm loop, idle eviction, capacity cap, condition-based wait,
``max_concurrent_provisions`` thundering-herd cap) but operates on
:class:`Worker` objects and delegates placement to a :class:`Scheduler`.

Concurrency model
-----------------
* A single :class:`asyncio.Lock` guards the free list / in-use set /
  pending counter.  The factory call runs *outside* the lock.
* ``acquire`` blocks on an :class:`asyncio.Condition` when no idle
  worker matches and the pool is at capacity — the intended
  back-pressure path.
* ``max_concurrent_provisions`` caps simultaneous factory calls so N
  simultaneous misses cannot thrash the host; it composes with (does
  not replace) :class:`Singleflight`-based dedup at the factory level.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Callable, Awaitable

from agentscope._logging import logger

from .._actor._scheduler import NoCapacityError, Scheduler
from .._actor._types import (
    WORKER_ACTIVE,
    WORKER_BUSY,
    ActorRef,
    Constraints,
)
from .._pool import (
    DEFAULT_ACQUIRE_TIMEOUT,
    DEFAULT_IDLE_TTL,
    DEFAULT_MAX_SIZE,
    DEFAULT_MIN_WARM,
    DEFAULT_SWEEP_INTERVAL,
    _PREWARM_BACKOFF,
)
from ._types import Worker, WorkerFactory


class WorkerPool:
    """Async pool of pre-warmed :class:`Worker` instances.

    Args:
        factory: Produces a fresh, alive :class:`Worker` (runtime already
            provisioned).  Called to grow the warm pool.
        scheduler: Selects an idle worker for an actor's constraints.
        max_size: Hard cap on live + pending workers.
        min_warm: Target idle workers maintained by the pre-warmer.
        idle_ttl: Seconds an idle worker may sit before eviction.
        sweep_interval: Idle-eviction loop period.
        acquire_timeout: Max seconds ``acquire`` waits at capacity.
        enable_prewarm: Whether to run the pre-warm background task.
        max_concurrent_provisions: Cap on simultaneous factory calls.
    """

    def __init__(
        self,
        factory: WorkerFactory | Callable[[], Awaitable[Worker]],
        *,
        scheduler: Scheduler | None = None,
        max_size: int = DEFAULT_MAX_SIZE,
        min_warm: int = DEFAULT_MIN_WARM,
        idle_ttl: float = DEFAULT_IDLE_TTL,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL,
        acquire_timeout: float = DEFAULT_ACQUIRE_TIMEOUT,
        enable_prewarm: bool = True,
        max_concurrent_provisions: int = 0,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if min_warm < 0 or min_warm > max_size:
            raise ValueError("min_warm must be in [0, max_size]")
        self._factory = factory
        self._scheduler = scheduler or Scheduler()
        self._max_size = max_size
        self._min_warm = min_warm
        self._idle_ttl = idle_ttl
        self._sweep_interval = sweep_interval
        self._acquire_timeout = acquire_timeout
        self._enable_prewarm = enable_prewarm and min_warm > 0
        self._provision_sem: asyncio.Semaphore | None = (
            asyncio.Semaphore(max_concurrent_provisions)
            if max_concurrent_provisions > 0
            else None
        )

        self._free: deque[tuple[Worker, float]] = deque()
        self._in_use: set[Worker] = set()
        self._pending: int = 0
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(self._lock)
        self._sweep_task: asyncio.Task | None = None
        self._prewarm_task: asyncio.Task | None = None
        self._closed = False

    # ── properties ───────────────────────────────────────────────

    @property
    def max_size(self) -> int:
        return self._max_size

    @property
    def warm_count(self) -> int:
        return len(self._free)

    @property
    def in_use_count(self) -> int:
        return len(self._in_use)

    @property
    def pending_count(self) -> int:
        return self._pending

    @property
    def total_count(self) -> int:
        return self.warm_count + self.in_use_count + self._pending

    # ── lifecycle ────────────────────────────────────────────────

    async def start(self) -> None:
        if self._sweep_task is None:
            self._sweep_task = asyncio.create_task(self._sweep_loop())
        if self._enable_prewarm and self._prewarm_task is None:
            self._prewarm_task = asyncio.create_task(self._prewarm_loop())

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        for task in (self._sweep_task, self._prewarm_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._sweep_task = None
        self._prewarm_task = None
        async with self._lock:
            free = list(self._free)
            self._free.clear()
            in_use = list(self._in_use)
            self._in_use.clear()
            self._cond.notify_all()
        for w, _ in free:
            await self._safe_close(w)
        for w in in_use:
            await self._safe_close(w)

    # ── acquire / release ────────────────────────────────────────

    async def acquire(
        self,
        constraints: Constraints,
        actor: ActorRef,
        *,
        prefer_node: str | None = None,
        timeout: float | None = None,
    ) -> Worker:
        """Reserve an idle worker for *actor*, growing the pool if there is room.

        Picks a free worker via the scheduler (honouring *prefer_node* for
        pause locality).  If none matches and the pool is below
        ``max_size``, provisions a new one.  Otherwise blocks up to
        *timeout* (default :attr:`_acquire_timeout`) for a worker to be
        returned.

        The returned worker is marked ``BUSY`` with
        ``assigned_actor = actor``.  Callers MUST :meth:`release` it.

        Raises:
            asyncio.TimeoutError: No worker became available in time.
            RuntimeError: The pool is closed.
        """
        if self._closed:
            raise RuntimeError("WorkerPool is closed")
        deadline: float | None = None
        timeout = self._acquire_timeout if timeout is None else timeout

        while True:
            if self._closed:
                raise RuntimeError("WorkerPool is closed")
            async with self._cond:
                candidates = [w for w, _ in self._free]
                chosen: Worker | None = None
                try:
                    chosen = self._scheduler.pick(
                        candidates, constraints, prefer_node=prefer_node,
                    )
                except NoCapacityError:
                    chosen = None

                if chosen is not None:
                    self._remove_free(chosen)
                    chosen.status = WORKER_BUSY
                    chosen.assigned_actor = actor
                    self._in_use.add(chosen)
                    return chosen

                if self.total_count < self._max_size:
                    self._pending += 1
                    try:
                        w = await self._provision_locked()
                    finally:
                        self._pending -= 1
                    w.status = WORKER_BUSY
                    w.assigned_actor = actor
                    self._in_use.add(w)
                    return w

                # Wait path.
                if deadline is None:
                    deadline = time.monotonic() + timeout
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError()
                try:
                    await asyncio.wait_for(
                        self._cond.wait_for(lambda: len(self._free) > 0),
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    raise
                continue

    async def release(self, worker: Worker, *, broken: bool = False) -> None:
        """Return a worker to the pool, or tear it down if *broken*."""
        async with self._cond:
            if worker not in self._in_use:
                return
            self._in_use.discard(worker)
            worker.assigned_actor = None
            if broken or self._closed or self.warm_count >= self._max_size:
                to_close = worker
            else:
                worker.status = WORKER_ACTIVE
                self._free.append((worker, time.monotonic()))
                self._cond.notify_all()
                return
        await self._safe_close(to_close)

    async def workers(self) -> list[Worker]:
        """Snapshot of all live workers (free + in-use)."""
        async with self._lock:
            return [w for w, _ in self._free] + list(self._in_use)

    async def metrics(self) -> dict:
        async with self._lock:
            return {
                "warm": self.warm_count,
                "in_use": self.in_use_count,
                "pending": self._pending,
                "total": self.total_count,
                "max_size": self._max_size,
                "min_warm": self._min_warm,
                "closed": self._closed,
            }

    # ── internals ────────────────────────────────────────────────

    def _remove_free(self, worker: Worker) -> None:
        for i, (w, _ts) in enumerate(self._free):
            if w is worker:
                del self._free[i]
                return

    async def _provision_locked(self) -> Worker:
        """Run the factory *outside* the pool lock (caller holds it).

        Temporarily releases :attr:`_lock` so a slow factory does not
        block other callers, then re-acquires it before returning.
        """
        self._lock.release()
        try:
            if self._provision_sem is not None:
                async with self._provision_sem:
                    w = await self._factory()  # type: ignore[misc]
            else:
                w = await self._factory()  # type: ignore[misc]
        finally:
            await self._lock.acquire()
        return w

    async def _provision_unlocked(self) -> Worker:
        """Run the factory when the pool lock is *not* held (prewarm path).

        Applies the provision semaphore if configured, but does not
        touch the pool lock — the caller manages locking around the
        bookkeeping (``_pending``, ``_free``) separately.
        """
        if self._provision_sem is not None:
            async with self._provision_sem:
                w = await self._factory()  # type: ignore[misc]
        else:
            w = await self._factory()  # type: ignore[misc]
        return w

    async def _sweep_loop(self) -> None:
        while True:
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
            survivors: deque[tuple[Worker, float]] = deque()
            evicted: list[Worker] = []
            while self._free:
                w, ts = self._free.popleft()
                if now - ts > self._idle_ttl:
                    evicted.append(w)
                else:
                    survivors.append((w, ts))
            self._free = survivors
        for w in evicted:
            await self._safe_close(w)

    async def _prewarm_loop(self) -> None:
        backoff_idx = 0
        while True:
            try:
                await asyncio.sleep(0.5)
                await self._prewarm_once()
                backoff_idx = 0
            except asyncio.CancelledError:
                return
            except Exception:
                delay = _PREWARM_BACKOFF[
                    min(backoff_idx, len(_PREWARM_BACKOFF) - 1)
                ]
                logger.warning(
                    "WorkerPool prewarm tick failed; backing off %.1fs",
                    delay,
                    exc_info=True,
                )
                backoff_idx += 1
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return

    async def _prewarm_once(self) -> None:
        if self._min_warm == 0:
            return
        async with self._lock:
            deficit = self._min_warm - self.warm_count
            if deficit <= 0:
                return
            deficit = min(deficit, self._max_size - self.total_count)
            if deficit <= 0:
                return
        for _ in range(deficit):
            async with self._lock:
                if self._closed or self.total_count >= self._max_size:
                    return
                self._pending += 1
            try:
                w = await self._provision_unlocked()
            except Exception:
                logger.exception("WorkerPool prewarm provision failed")
                async with self._lock:
                    self._pending -= 1
                return
            surplus: bool
            async with self._lock:
                self._pending -= 1
                if self._closed or self.warm_count >= self._max_size:
                    surplus = True
                else:
                    surplus = False
                    w.status = WORKER_ACTIVE
                    self._free.append((w, time.monotonic()))
                    self._cond.notify_all()
            if surplus:
                await self._safe_close(w)

    @staticmethod
    async def _safe_close(w: Worker) -> None:
        try:
            await w.runtime.close()
        except Exception:
            logger.exception(
                "WorkerPool: failed to close worker %s", w.worker_id,
            )


__all__ = ["WorkerPool"]
