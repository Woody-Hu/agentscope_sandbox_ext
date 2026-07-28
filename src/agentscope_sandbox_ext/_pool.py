# -*- coding: utf-8 -*-
"""Async sandbox pool with optional pre-warming.

The pool is generic over a *factory* callable that produces a fresh
:class:`SandboxedWorkspaceExtBase` instance and runs its full
``initialize``.  Two eviction paths keep the pool bounded:

* **Idle eviction** — a background sweeper closes any pooled sandbox
  whose last-return timestamp is older than ``idle_ttl``.
* **Capacity cap** — ``max_size`` bounds the warm pool; excess returns
  are torn down immediately rather than queued.

The pool does *not* itself implement the
:class:`WorkspaceManagerBase` interface — it is a building block
managers compose with their own cache.  See ``FirecrackerWorkspaceManager``
for an example of a manager that wires a pool in front of its cache.

Design notes
------------

* Every public method is a coroutine and safe under concurrency; an
  ``asyncio.Lock`` guards mutation of the free list.
* The pre-warm task is best-effort: it tries to maintain
  ``min_warm`` ready sandboxes in the background, retrying with
  exponential back-off.  Failures are logged and swallowed so a
  transient provision error never tears the pool down.
* ``acquire`` blocks (with a timeout) when no warm sandbox is
  available and the warm pool is already at ``max_size``; this is the
  intended back-pressure path for a loaded manager.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any, Awaitable, Callable

from agentscope._logging import logger

from ._base import SandboxedWorkspaceExtBase


#: Default maximum number of warm sandboxes kept in the pool.
DEFAULT_MAX_SIZE = 4

#: Default minimum number of warm sandboxes the pre-warmer tries to
#: maintain.  Zero disables pre-warming entirely.
DEFAULT_MIN_WARM = 0

#: Default idle TTL (seconds) before an unused pooled sandbox is
#: evicted by the sweeper.
DEFAULT_IDLE_TTL = 1800.0

#: Default sweep interval (seconds) for the idle-eviction loop.
DEFAULT_SWEEP_INTERVAL = 60.0

#: Default acquire timeout (seconds) when no warm sandbox is
#: available and the pool is full.
DEFAULT_ACQUIRE_TIMEOUT = 60.0

#: Back-off schedule (seconds) used by the pre-warmer between failed
#: provision attempts.  After exhausting the schedule the pre-warmer
#: waits ``_PREWARM_BACKOFF[-1]`` before retrying.
_PREWARM_BACKOFF = (1.0, 2.0, 5.0, 10.0)


Factory = Callable[[], Awaitable[SandboxedWorkspaceExtBase]]


class SandboxPool:
    """Async pool of pre-warmed :class:`SandboxedWorkspaceExtBase`.

    Args:
        factory (`Factory`):
            Callable that produces a fresh, fully-initialised
            :class:`SandboxedWorkspaceExtBase` instance.  The pool
            calls it whenever it needs to grow the warm pool.
        max_size (`int`, defaults to :data:`DEFAULT_MAX_SIZE`):
            Hard cap on warm sandboxes.  Returns that would push the
            pool above this cap tear the sandbox down immediately.
        min_warm (`int`, defaults to :data:`DEFAULT_MIN_WARM`):
            Target number of warm sandboxes the pre-warmer tries to
            maintain.  ``0`` disables pre-warming.
        idle_ttl (`float`, defaults to :data:`DEFAULT_IDLE_TTL`):
            Seconds a sandbox may sit idle in the pool before the
            sweeper evicts it.
        sweep_interval (`float`, defaults to :data:`DEFAULT_SWEEP_INTERVAL`):
            How often the idle-eviction sweeper wakes up.
        acquire_timeout (`float`, defaults to :data:`DEFAULT_ACQUIRE_TIMEOUT`):
            Maximum seconds ``acquire`` waits for a sandbox when the
            pool is empty and full.  Raises :class:`asyncio.TimeoutError`
            on expiry.
        enable_prewarm (`bool`, defaults to ``True``):
            Whether to start the pre-warm background task.  Disabled
            when ``min_warm`` is ``0``.
    """

    def __init__(
        self,
        factory: Factory,
        *,
        max_size: int = DEFAULT_MAX_SIZE,
        min_warm: int = DEFAULT_MIN_WARM,
        idle_ttl: float = DEFAULT_IDLE_TTL,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL,
        acquire_timeout: float = DEFAULT_ACQUIRE_TIMEOUT,
        enable_prewarm: bool = True,
    ) -> None:
        """Bind pool configuration and create the empty free list."""
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if min_warm < 0:
            raise ValueError("min_warm must be >= 0")
        if min_warm > max_size:
            raise ValueError("min_warm cannot exceed max_size")

        self._factory = factory
        self._max_size = max_size
        self._min_warm = min_warm
        self._idle_ttl = idle_ttl
        self._sweep_interval = sweep_interval
        self._acquire_timeout = acquire_timeout
        self._enable_prewarm = enable_prewarm and min_warm > 0

        #: Free list — (sandbox, last_returned_monotonic).
        self._free: deque[tuple[SandboxedWorkspaceExtBase, float]] = deque()
        #: Live sandboxes currently handed out via ``acquire``.
        self._in_use: set[SandboxedWorkspaceExtBase] = set()
        self._lock = asyncio.Lock()
        #: Condition signalled whenever a sandbox returns to the free
        #: list, so blocked ``acquire`` callers can wake up.
        self._cond = asyncio.Condition(self._lock)
        self._sweep_task: asyncio.Task | None = None
        self._prewarm_task: asyncio.Task | None = None
        self._closed = False

    # ── public API ───────────────────────────────────────────────

    @property
    def max_size(self) -> int:
        """Hard cap on warm sandboxes."""
        return self._max_size

    @property
    def min_warm(self) -> int:
        """Target warm pool size maintained by the pre-warmer."""
        return self._min_warm

    @property
    def warm_count(self) -> int:
        """Number of sandboxes currently sitting in the free list."""
        return len(self._free)

    @property
    def in_use_count(self) -> int:
        """Number of sandboxes currently checked out via ``acquire``."""
        return len(self._in_use)

    @property
    def total_count(self) -> int:
        """Total live sandboxes (warm + in use)."""
        return self.warm_count + self.in_use_count

    async def start(self) -> None:
        """Start the background sweeper and (optionally) pre-warmer.

        Safe to call multiple times; subsequent calls are no-ops.
        """
        if self._sweep_task is None:
            self._sweep_task = asyncio.create_task(self._sweep_loop())
        if self._enable_prewarm and self._prewarm_task is None:
            self._prewarm_task = asyncio.create_task(self._prewarm_loop())

    async def aclose(self) -> None:
        """Stop background tasks and close every pooled sandbox.

        Idempotent.  After ``aclose`` the pool must not be reused.
        """
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
            # Wake any blocked ``acquire`` callers so they observe
            # ``self._closed`` and raise rather than waiting forever.
            # Must hold the lock to notify on an asyncio.Condition.
            self._cond.notify_all()

        # Close outside the lock so a slow teardown doesn't block
        # other callers — though at this point the pool is closed and
        # no new acquire will succeed anyway.
        for ws, _ in free:
            await self._safe_close(ws)
        for ws in in_use:
            await self._safe_close(ws)

    async def acquire(self) -> SandboxedWorkspaceExtBase:
        """Return a ready sandbox, growing the pool if there is room.

        When a warm sandbox is available it is popped off the free
        list.  Otherwise, if the live count is below ``max_size``, a
        fresh sandbox is provisioned via the factory.  Otherwise the
        call blocks for up to ``acquire_timeout`` seconds waiting for
        a sandbox to be returned.

        Raises:
            asyncio.TimeoutError: If no sandbox becomes available
                within ``acquire_timeout``.
            RuntimeError: If the pool has been closed.
        """
        if self._closed:
            raise RuntimeError("SandboxPool is closed")

        async with self._cond:
            # Fast path: a warm sandbox is waiting.
            if self._free:
                ws = self._free.popleft()[0]
                self._in_use.add(ws)
                return ws

            # Growth path: still room below max_size — provision now.
            if self.total_count < self._max_size:
                ws = await self._provision_locked()
                self._in_use.add(ws)
                return ws

            # Wait path: at capacity — wait for a return.
            try:
                await asyncio.wait_for(
                    self._cond.wait_for(self._free.__len__.__call__),
                    timeout=self._acquire_timeout,
                )
            except asyncio.TimeoutError:
                raise
            ws = self._free.popleft()[0]
            self._in_use.add(ws)
            return ws

    async def release(
        self,
        ws: SandboxedWorkspaceExtBase,
        *,
        broken: bool = False,
    ) -> None:
        """Return a sandbox to the pool, or tear it down if broken.

        Args:
            ws (`SandboxedWorkspaceExtBase`):
                The sandbox to release.  Must have been previously
                acquired from this pool.
            broken (`bool`, defaults to ``False``):
                If ``True`` the sandbox is torn down instead of
                returned — caller asserts the sandbox is in an
                unusable state (gateway crash, exec failure, ...).
        """
        async with self._cond:
            if ws not in self._in_use:
                # Already released or never acquired — ignore.
                return
            self._in_use.discard(ws)

            if broken or self._closed:
                # Close outside the lock to avoid blocking other
                # callers on a slow teardown.
                pass
            elif self.warm_count >= self._max_size:
                # Pool is full — close this one rather than hold it.
                pass
            else:
                self._free.append((ws, time.monotonic()))
                self._cond.notify_all()
                return

        # Close path: arrived here only if we did NOT return above.
        await self._safe_close(ws)

    async def metrics(self) -> dict[str, Any]:
        """Return a small dict of pool observability fields."""
        async with self._lock:
            return {
                "warm": self.warm_count,
                "in_use": self.in_use_count,
                "total": self.total_count,
                "max_size": self._max_size,
                "min_warm": self._min_warm,
                "closed": self._closed,
            }

    # ── internals ────────────────────────────────────────────────

    async def _provision_locked(self) -> SandboxedWorkspaceExtBase:
        """Call the factory and return the new sandbox.

        The factory is invoked *outside* the lock to avoid holding it
        across the (slow) provision; we re-check capacity after the
        await.  Caller MUST hold ``self._lock`` on entry.

        Returns:
            `SandboxedWorkspaceExtBase`:
                A live, initialised sandbox.
        """
        # Release the lock around the await; re-acquire afterwards.
        self._lock.release()
        try:
            ws = await self._factory()
        finally:
            await self._lock.acquire()
        return ws

    async def _sweep_loop(self) -> None:
        """Idle-eviction loop."""
        while True:
            try:
                await asyncio.sleep(self._sweep_interval)
            except asyncio.CancelledError:
                return
            try:
                await self._sweep_once()
            except Exception:
                logger.exception("SandboxPool sweeper tick failed")

    async def _sweep_once(self) -> None:
        """One sweep tick: evict expired entries from the free list."""
        now = time.monotonic()
        async with self._lock:
            survivors: deque[tuple[SandboxedWorkspaceExtBase, float]] = deque()
            evicted: list[SandboxedWorkspaceExtBase] = []
            while self._free:
                ws, ts = self._free.popleft()
                if now - ts > self._idle_ttl:
                    evicted.append(ws)
                else:
                    survivors.append((ws, ts))
            self._free = survivors
        for ws in evicted:
            await self._safe_close(ws)

    async def _prewarm_loop(self) -> None:
        """Best-effort pre-warm loop targeting ``min_warm`` warm slots."""
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
                    "SandboxPool prewarm tick failed; backing off %.1fs",
                    delay,
                    exc_info=True,
                )
                backoff_idx += 1
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return

    async def _prewarm_once(self) -> None:
        """One prewarm tick: top up to ``min_warm`` warm sandboxes."""
        if self._min_warm == 0:
            return
        async with self._lock:
            deficit = self._min_warm - self.warm_count
            if deficit <= 0:
                return
            # Cap deficit by remaining capacity — don't grow beyond
            # max_size even when min_warm was bumped at runtime.
            deficit = min(deficit, self._max_size - self.total_count)
            if deficit <= 0:
                return

        # Provision outside the lock — see ``_provision_locked``.
        for _ in range(deficit):
            try:
                ws = await self._factory()
            except Exception:
                logger.exception("SandboxPool prewarm provision failed")
                return
            async with self._lock:
                if self._closed:
                    await self._safe_close(ws)
                    return
                if self.warm_count >= self._max_size:
                    # Pool filled up by another path while we were
                    # provisioning — close the surplus.
                    pass
                else:
                    self._free.append((ws, time.monotonic()))
                    self._cond.notify_all()
                    continue
            await self._safe_close(ws)

    @staticmethod
    async def _safe_close(ws: SandboxedWorkspaceExtBase) -> None:
        """Close a sandbox, logging any failure instead of raising."""
        try:
            await ws.close()
        except Exception:
            logger.exception(
                "SandboxPool: failed to close sandbox %s",
                getattr(ws, "workspace_id", "?"),
            )
