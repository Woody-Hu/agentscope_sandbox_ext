# -*- coding: utf-8 -*-
"""Per-key in-process call deduplication (singleflight).

Borrowed from the reference runtime's image-layer cache pattern: when
several callers ask for the same expensive, idempotent result at once,
only one of them actually executes; the rest wait and share the outcome
(or the exception).

This is *not* a concurrency cap (that is the job of a semaphore such as
``SandboxPool.max_concurrent_provisions``) and *not* a serialisation
lock (that is the job of the keyed ``LockProvider``).  Singleflight
collapses **identical in-flight work**: same key ⇒ one execution, all
waiters get the same result.  The three concerns compose:

* **singleflight** — N identical requests ⇒ 1 execution (dedup).
* **semaphore cap** — at most C executions in parallel (back-pressure).
* **keyed lock** — contended multi-step workflows run one at a time.

The implementation is a single :class:`asyncio.Lock` guarding a
``{key: Future}`` table.  The first caller for a key creates a future,
releases the table lock, runs the factory, and resolves the future; late
arrivers find the existing future and await it.  The entry is removed
once the factory resolves so a later call for the same key re-executes
(lazy, not a cache).

Used by: provision (``provision:<class>:<template>``), checkpoint
(``checkpoint:<actor>``), golden-snapshot materialise
(``golden:<template>``).
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


class Singleflight:
    """Collapse concurrent identical async calls into one execution.

    Thread-safety / concurrency: safe under asyncio concurrency.  All
    access to the in-flight table is guarded by a single lock; the
    factory runs *outside* that lock so a slow factory never blocks
    other keys.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._inflight: dict[str, asyncio.Future] = {}

    async def run(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
    ) -> T:
        """Run *factory* once for *key*; concurrent callers share the result.

        Args:
            key: Dedup key.  Callers with the same key concurrently
                receive the same return value (or the same exception).
            factory: Zero-arg async callable that produces the result.
                Executed at most once per in-flight window.

        Returns:
            The factory's result, shared by all concurrent callers for
            *key*.

        Raises:
            Exception: Whatever the factory raises, broadcast to all
                waiters for *key*.
        """
        # Fast path under the table lock: either register as the leader
        # (create the future) or join an existing in-flight call.
        async with self._lock:
            existing = self._inflight.get(key)
            if existing is not None:
                leader = False
                fut: asyncio.Future = existing
            else:
                leader = True
                fut = asyncio.get_event_loop().create_future()
                self._inflight[key] = fut

        if not leader:
            # Follower: just await the leader's result.
            return await fut  # type: ignore[no-any-return]

        # Leader: run the factory outside the table lock, then resolve
        # the future and remove the entry so the next call re-executes.
        try:
            result = await factory()
        except BaseException as exc:  # noqa: BLE001 - broadcast everything
            async with self._lock:
                self._inflight.pop(key, None)
            if not fut.done():
                fut.set_exception(exc)
                # If no follower ever awaits, mark the exception as
                # retrieved to suppress the "Future exception was never
                # retrieved" warning on GC.  Followers that *do* await
                # still receive the exception via ``await fut``.
                fut.exception()
            raise
        else:
            async with self._lock:
                self._inflight.pop(key, None)
            if not fut.done():
                fut.set_result(result)
            return result

    @property
    def inflight_count(self) -> int:
        """Number of keys currently being executed (observability)."""
        return len(self._inflight)


__all__ = ["Singleflight"]
