# -*- coding: utf-8 -*-
"""Traffic routing for active actors.

Borrowed from the reference runtime's "uniform DNS mesh + ingress
router" pattern, where every actor is reachable at a stable name and
the router lazily resumes + tunnels on demand.  In-process we do not
need Envoy or mTLS tunnels; we only need the *contract*: given an
:class:`ActorRef`, resolve it to the :class:`WorkerAssignment` of the
worker currently hosting it (so a caller can reach the live sandbox).

The :class:`Router` ABC is the pluggability seam — a real deployment
can back it with a DNS server, a service mesh, or a Redis pub/sub
fanout.  :class:`InMemoryRouter` is the default and is sufficient for
single-node use.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from .model import ActorRef, WorkerAssignment


class Router(ABC):
    """Resolve an :class:`ActorRef` to its active :class:`WorkerAssignment`.

    A binding exists only while the actor is ``RUNNING`` / ``RESUMING``;
    :meth:`unbind` is called when the actor suspends or its worker is
    lost.  :meth:`resolve` returning ``None`` means "no active
    assignment" — the caller should resume the actor before routing.
    """

    @abstractmethod
    async def resolve(self, actor: ActorRef) -> WorkerAssignment | None:
        """Return the live assignment for *actor*, or ``None`` if idle."""

    @abstractmethod
    async def bind(self, actor: ActorRef, assignment: WorkerAssignment) -> None:
        """Record that *actor* is now hosted on *assignment*'s worker."""

    @abstractmethod
    async def unbind(self, actor: ActorRef) -> None:
        """Drop the binding (actor suspended / worker lost)."""

    @abstractmethod
    async def list_bindings(self) -> dict[str, WorkerAssignment]:
        """Return a snapshot of all current bindings (for metrics)."""


class InMemoryRouter(Router):
    """Process-local :class:`Router` backed by a dict + asyncio lock."""

    def __init__(self) -> None:
        self._bindings: dict[str, WorkerAssignment] = {}
        self._lock = asyncio.Lock()

    async def resolve(self, actor: ActorRef) -> WorkerAssignment | None:
        async with self._lock:
            return self._bindings.get(str(actor))

    async def bind(self, actor: ActorRef, assignment: WorkerAssignment) -> None:
        async with self._lock:
            self._bindings[str(actor)] = assignment

    async def unbind(self, actor: ActorRef) -> None:
        async with self._lock:
            self._bindings.pop(str(actor), None)

    async def list_bindings(self) -> dict[str, WorkerAssignment]:
        async with self._lock:
            return dict(self._bindings)


__all__ = ["Router", "InMemoryRouter"]
