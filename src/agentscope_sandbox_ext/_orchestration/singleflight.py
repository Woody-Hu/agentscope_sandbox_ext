# -*- coding: utf-8 -*-
"""Async singleflight — deduplication of concurrent identical calls.

Borrowed from ``golang.org/x/sync/singleflight`` and the reference
runtime's ingress router (which dedups concurrent ``ResumeActor`` calls
for the same actor onto one in-flight control-plane call).  The key
property the reference runtime relies on is that **the leader's budget
is detached from any single caller**: if caller 1 disconnects, callers
2 and 3 still get the result.  Late joiners inherit the *remaining*
budget.

This module reproduces that contract in pure asyncio.  Concurrent
``run(key, fn)`` calls with the same *key* collapse onto one in-flight
``fn`` invocation; every caller receives the same result (or the same
exception).  The leader's cancellation does **not** cancel the
in-flight work — only budget exhaustion or the work itself completing
ends the flight.

This is the missing dedup primitive the existing :class:`SandboxPool`
does not provide.  ``SandboxPool.max_concurrent_provisions`` *bounds*
concurrency (a semaphore); singleflight *eliminates* it for identical
requests.  The two compose: singleflight collapses N identical requests
to 1, and the semaphore bounds how many *distinct* requests run at
once.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Generic, TypeVar

from agentscope._logging import logger

T = TypeVar("T")


class SingleflightError(Exception):
    """Base class for singleflight failures."""


class BudgetExhausted(SingleflightError):
    """Raised by a late joiner when the flight's budget has run out.

    The leader's work may still be in flight — this only signals that the
    late joiner was not willing to wait any longer.  Mirrors the
    reference runtime's per-flight budget timeout.
    """


@dataclass
class _Flight(Generic[T]):
    """In-flight state for one singleflight key.

    Exactly one caller (the *leader*) executes ``fn``; everyone else
    (the *joiners*) awaits :attr:`leader` and shares the outcome.  The
    ``budget_deadline`` is the absolute monotonic time at which joiners
    stop waiting; the leader itself is **not** cancelled by the budget
    — it runs to completion so its result is available to future
    callers.
    """

    leader: asyncio.Task[T]
    started_at: float
    budget_deadline: float
    joiner_count: int = 0
    completed: bool = False
    result: T | None = None
    error: BaseException | None = None


class Singleflight:
    """Deduplicate concurrent calls with the same key onto one flight.

    Example::

        sf = Singleflight()
        # These three concurrent calls execute `load` exactly once:
        a, b, c = await asyncio.gather(
            sf.run("user-42", load),
            sf.run("user-42", load),
            sf.run("user-42", load),
        )
        assert a is b is c

    The leader's budget is detached from any single caller's context —
    if a joiner cancels (via ``asyncio.TimeoutError`` from its own
    ``wait_for``), the leader keeps running so other joiners and future
    callers still get the result.

    Args:
        default_budget (`float`, defaults to ``30.0``):
            Default per-flight budget in seconds.  ``None`` means no
            budget (joiners wait indefinitely for the leader).  The
            budget applies to *joiners* — the leader runs to completion
            regardless, so its result is cached for any caller that
            arrives after the budget but before the leader finishes.
    """

    def __init__(self, *, default_budget: float | None = 30.0) -> None:
        self._default_budget = default_budget
        self._flights: dict[str, _Flight] = {}
        self._lock = asyncio.Lock()

    async def run(
        self,
        key: str,
        fn: Callable[[], Awaitable[T]],
        *,
        budget: float | None = None,
    ) -> T:
        """Run *fn* under singleflight dedup keyed by *key*.

        If a flight for *key* is already in flight, this call joins it
        (sharing the leader's eventual result/exception) instead of
        starting a second ``fn``.  Otherwise this call is the leader and
        ``fn`` is invoked immediately.

        Args:
            key (`str`):
                Dedup key.  Concurrent calls with the same key collapse.
            fn (`Callable[[], Awaitable[T]]`):
                The async work to dedup.  Invoked at most once per
                in-flight key.
            budget (`float | None`, optional):
                Per-flight budget in seconds for *this* flight.  Only
                meaningful for the leader (establishes the flight's
                deadline).  Joiners inherit the *remaining* budget of
                the existing flight.  ``None`` = wait indefinitely.

        Returns:
            The leader's result, shared with every joiner.

        Raises:
            BudgetExhausted: If a joiner's remaining budget runs out
                before the leader completes.  The leader is **not**
                cancelled — it keeps running and its result is cached.
            Exception: Whatever ``fn`` raises, propagated to every
                joiner that has not yet timed out.
        """
        eff_budget = budget if budget is not None else self._default_budget
        return await self._run(key, fn, eff_budget)

    async def _run(
        self,
        key: str,
        fn: Callable[[], Awaitable[T]],
        budget: float | None,
    ) -> T:
        now = time.monotonic()
        deadline = (now + budget) if budget is not None else None

        # Decide leader-vs-joiner under the lock, but NEVER await while
        # holding it — otherwise a joiner waiting on the leader would
        # deadlock the leader's _finalize (which needs the lock to pop
        # the flight) and block every other caller.
        role: str
        flight: _Flight[T]
        async with self._lock:
            existing = self._flights.get(key)
            if existing is not None:
                existing.joiner_count += 1
                role = "joiner"
                flight = existing
            else:
                task = asyncio.create_task(self._lead(key, fn))
                flight = _Flight(
                    leader=task,
                    started_at=now,
                    budget_deadline=(
                        deadline if deadline is not None else float("inf")
                    ),
                )
                self._flights[key] = flight
                role = "leader"

        if role == "joiner":
            return await self._join(flight, deadline)
        # Leader path: await our own task, then finalize.
        try:
            return await flight.leader
        finally:
            await self._finalize(key, flight.leader)

    async def _lead(self, key: str, fn: Callable[[], Awaitable[T]]) -> T:
        """Leader body: run ``fn`` to completion, cache the outcome."""
        flight = self._flights[key]
        try:
            result = await fn()
            flight.result = result
            flight.completed = True
            return result
        except BaseException as exc:  # noqa: BLE001 - propagate to joiners
            flight.error = exc
            flight.completed = True
            raise
        finally:
            flight.completed = True

    async def _join(self, flight: _Flight[T], deadline: float | None) -> T:
        """Joiner body: wait for the leader, sharing its outcome."""
        try:
            if deadline is None:
                return await flight.leader
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BudgetExhausted(
                    "singleflight budget exhausted before joining flight"
                )
            try:
                return await asyncio.wait_for(asyncio.shield(flight.leader), remaining)
            except asyncio.TimeoutError as exc:
                raise BudgetExhausted(
                    "singleflight budget exhausted while waiting for leader"
                ) from exc
        finally:
            flight.joiner_count -= 1

    async def _finalize(self, key: str, task: asyncio.Task[T]) -> None:
        """Remove the completed flight, warning if the leader failed unexpectedly."""
        async with self._lock:
            flight = self._flights.pop(key, None)
        if flight is None:
            return
        if not task.cancelled() and task.done() and task.exception() is not None:
            logger.debug(
                "Singleflight flight %r completed with error: %r",
                key,
                task.exception(),
            )

    # ── introspection (for tests / metrics) ──────────────────────

    @property
    def in_flight_keys(self) -> set[str]:
        """Keys with an active (not-yet-finalized) flight."""
        return set(self._flights.keys())

    def flight_stats(self, key: str) -> dict[str, object] | None:
        """Return observability data for *key*'s flight, or ``None``."""
        flight = self._flights.get(key)
        if flight is None:
            return None
        return {
            "started_at": flight.started_at,
            "budget_deadline": flight.budget_deadline,
            "joiner_count": flight.joiner_count,
            "completed": flight.completed,
            "leader_done": flight.leader.done(),
        }


__all__ = ["Singleflight", "SingleflightError", "BudgetExhausted"]
