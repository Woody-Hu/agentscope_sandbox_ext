# -*- coding: utf-8 -*-
"""Actor registry + keyed lock provider.

The registry stores :class:`ActorRecord` instances keyed by
:class:`ActorRef`.  Two concurrency primitives govern access:

* **Optimistic version** — every :meth:`ActorRegistry.update` is
  conditional on an ``expected_version``; a stale writer gets
  :class:`VersionConflict`.  This gives lock-free reads (callers just
  ``get`` the record) while preventing lost updates on contended actors.
* **Keyed lock** — :meth:`LockProvider.acquire(key)` returns an async
  context manager that serialises multi-step workflows (resume / suspend
  / pause) on the same actor.  The in-process impl is a ref-counted
  ``{key: asyncio.Lock}`` table; a distributed impl (Redis/Valkey) drops
  in for multi-replica deployments with auto-renew + context-cancellation
  on lock loss.

The two compose: the lock scopes a workflow's *steps*, optimistic
version protects against a stale write *within* a step (or by a writer
that did not take the lock).  This mirrors the reference runtime's
optimistic-version + distributed-lock combination, scaled down to a
single process by default.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import time
from typing import AsyncIterator

from ._types import (
    STATUS_SUSPENDED,
    ActorRecord,
    ActorRef,
    VersionConflict,
)


class LockProvider(abc.ABC):
    """Distributed-style keyed lock, in-process by default."""

    @abc.abstractmethod
    @contextlib.asynccontextmanager
    async def acquire(self, key: str) -> AsyncIterator[None]:
        """Hold an exclusive lock on *key* for the duration of the block."""
        yield  # pragma: no cover


class InProcessLockProvider(LockProvider):
    """Ref-counted ``{key: asyncio.Lock}`` table.

    A lock is created on first acquire for a key and removed once the
    last holder releases it, so the table does not grow unbounded.
    """

    def __init__(self) -> None:
        self._table_lock = asyncio.Lock()
        self._locks: dict[str, tuple[asyncio.Lock, int]] = {}

    @contextlib.asynccontextmanager
    async def acquire(self, key: str) -> AsyncIterator[None]:
        lock: asyncio.Lock
        async with self._table_lock:
            entry = self._locks.get(key)
            if entry is None:
                lock = asyncio.Lock()
                self._locks[key] = (lock, 1)
            else:
                lock, count = entry
                self._locks[key] = (lock, count + 1)
        try:
            async with lock:
                yield
        finally:
            async with self._table_lock:
                entry = self._locks.get(key)
                if entry is not None:
                    lock2, count = entry
                    if count <= 1:
                        self._locks.pop(key, None)
                    else:
                        self._locks[key] = (lock2, count - 1)


class ActorRegistry(abc.ABC):
    """Persistent store of :class:`ActorRecord`, with optimistic versioning."""

    @abc.abstractmethod
    async def create(self, record: ActorRecord) -> ActorRecord:
        """Insert a new record; raise :class:`KeyError` if it already exists."""

    @abc.abstractmethod
    async def get(self, ref: ActorRef) -> ActorRecord:
        """Return the record for *ref*; raise :class:`KeyError` if missing."""

    @abc.abstractmethod
    async def update(
        self,
        record: ActorRecord,
        *,
        expected_version: int,
    ) -> ActorRecord:
        """Replace the record iff its current version is *expected_version*.

        On success bumps ``version`` by 1 and stamps ``updated_at``.
        Raises :class:`VersionConflict` if the stored version differs.
        """

    @abc.abstractmethod
    async def delete(self, ref: ActorRef) -> None:
        """Delete the record; raise :class:`RuntimeError` unless SUSPENDED."""

    @abc.abstractmethod
    async def list(self, namespace: str) -> list[ActorRecord]:
        """List all actors in *namespace*."""


class InProcessActorRegistry(ActorRegistry):
    """In-memory registry suitable for single-process deployments.

    All access is guarded by a single :class:`asyncio.Lock`; the
    optimistic-version check is performed under that lock so two
    concurrent updates to the same actor cannot both succeed.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._records: dict[str, ActorRecord] = {}

    async def create(self, record: ActorRecord) -> ActorRecord:
        async with self._lock:
            if record.ref.key in self._records:
                raise KeyError(f"actor already exists: {record.ref.key}")
            now = time.time()
            record.created_at = now
            record.updated_at = now
            self._records[record.ref.key] = record
            return record

    async def get(self, ref: ActorRef) -> ActorRecord:
        async with self._lock:
            rec = self._records.get(ref.key)
            if rec is None:
                raise KeyError(f"no such actor: {ref.key}")
            return rec

    async def update(
        self,
        record: ActorRecord,
        *,
        expected_version: int,
    ) -> ActorRecord:
        async with self._lock:
            existing = self._records.get(record.ref.key)
            if existing is None:
                raise KeyError(f"no such actor: {record.ref.key}")
            if existing.version != expected_version:
                raise VersionConflict(
                    f"actor {record.ref.key}: expected version "
                    f"{expected_version}, got {existing.version}",
                )
            record.version = expected_version + 1
            record.created_at = existing.created_at
            record.updated_at = time.time()
            self._records[record.ref.key] = record
            return record

    async def delete(self, ref: ActorRef) -> None:
        async with self._lock:
            existing = self._records.get(ref.key)
            if existing is None:
                raise KeyError(f"no such actor: {ref.key}")
            if existing.status != STATUS_SUSPENDED:
                raise RuntimeError(
                    f"actor {ref.key} is {existing.status}; only "
                    f"{STATUS_SUSPENDED} actors can be deleted",
                )
            self._records.pop(ref.key, None)

    async def list(self, namespace: str) -> list[ActorRecord]:
        async with self._lock:
            return [
                rec
                for rec in self._records.values()
                if rec.ref.namespace == namespace
            ]


__all__ = [
    "LockProvider",
    "InProcessLockProvider",
    "ActorRegistry",
    "InProcessActorRegistry",
]
