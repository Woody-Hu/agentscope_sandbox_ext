# -*- coding: utf-8 -*-
"""Unified extension base classes for sandbox workspaces.

This module defines a single ``SandboxedWorkspaceExtBase`` that every
extension backend (Firecracker microVM, gVisor, Kata Containers, ...)
inherits from, plus a ``SandboxExtManagerBase`` that every manager
inherits from.  Both reuse agentscope's own abstractions verbatim:

* :class:`agentscope.workspace._sandboxed_base.SandboxedWorkspaceBase`
  — the in-sandbox MCP-gateway template-method lifecycle.
* :class:`agentscope.app.workspace_manager._base.WorkspaceManagerBase`
  — the manager interface that ``agentscope.app`` calls into.

The two extension base classes only add:

1. A ``sandbox_kind`` discriminator string, so a management plane can
   route metrics / dashboard cards by backend without instanceof
   checks.
2. A ``metrics()`` hook returning a small dict of backend-specific
   observability fields (vCPU / mem / pool size / boot time).
3. A ``verify_runtime_available`` classmethod that every concrete
   backend implements so the manager can surface a clean error before
   attempting to provision a sandbox whose runtime is not installed.

Nothing here overrides or patches agentscope native code; everything
is composed by inheritance.  See ``docs/ARCHITECTURE.md``.
"""

from __future__ import annotations

import abc
from typing import Any

# Native agentscope abstractions we extend.  These are *imported*, not
# modified — SandboxedWorkspaceBase lives at a private path because
# agentscope's public surface only re-exports concrete backends, but
# the class itself is part of the documented subclassing contract (the
# native applecontainer / e2b / docker backends all subclass it).
from agentscope.workspace._sandboxed_base import SandboxedWorkspaceBase
from agentscope.app.workspace_manager._base import (
    IsolationPolicy,
    WorkspaceManagerBase,
)


class SandboxedWorkspaceExtBase(SandboxedWorkspaceBase):
    """Common base class for every extension sandbox backend.

    Concrete subclasses (``FirecrackerWorkspace``, ``GVisorWorkspace``,
    ``KataWorkspace``) inherit from this so a management plane has a
    single ``isinstance`` discriminator and a uniform ``metrics()``
    hook, while still enjoying the full sandboxed-workspace lifecycle
    (gateway bootstrap, MCP persistence, skill seeding, offload)
    inherited from :class:`SandboxedWorkspaceBase`.

    Subclasses MUST set :attr:`sandbox_kind` to a short, stable,
    lower-snake-case identifier (``"firecracker"``, ``"gvisor"``,
    ``"kata"``).  The discriminator is intended for telemetry and
    dashboard routing — it must not be used for behavioural branching
    inside the framework.
    """

    #: Backend discriminator, overridden by each concrete subclass.
    sandbox_kind: str = "ext"

    @classmethod
    @abc.abstractmethod
    async def verify_runtime_available(cls) -> None:
        """Raise :class:`RuntimeError` if the host cannot run this backend.

        Concrete subclasses probe for the runtime binary / kernel module
        / device node they need (e.g. ``firecracker --version``,
        ``which runsc``, ``test -c /dev/kvm``) and raise a descriptive
        :class:`RuntimeError` on miss.  This is a probe, not a
        guarantee — actual provisioning can still fail later if the
        host state changed between probe and provision.

        Raises:
            RuntimeError: If the runtime is not installed / not usable.
        """

    async def metrics(self) -> dict[str, Any]:
        """Return a small dict of backend-specific observability fields.

        The default implementation returns the discriminator and the
        live / dead flag; subclasses extend it with backend-specific
        counters (boot time, vCPU, memory, pool slot id, ...).

        Returns:
            `dict[str, Any]`:
                Backend-specific metrics.  Always includes
                ``sandbox_kind`` and ``is_alive``.
        """
        return {
            "sandbox_kind": self.sandbox_kind,
            "is_alive": self.is_alive,
            "workspace_id": self.workspace_id,
        }


class SandboxExtManagerBase(WorkspaceManagerBase):
    """Common base class for every extension workspace manager.

    Adds a ``backend_kind`` discriminator (mirroring
    :attr:`SandboxedWorkspaceExtBase.sandbox_kind`) and a
    ``manager_metrics`` hook.  Otherwise it inherits the full manager
    interface (``get_workspace`` / ``close`` / ``close_all`` /
    isolation policies / async-context-manager semantics) from
    :class:`WorkspaceManagerBase` unchanged.

    Concrete managers typically compose a :class:`SandboxPool` for
    pooling optimisation rather than managing a flat cache directly —
    see :mod:`agentscope_sandbox_ext._pool`.
    """

    #: Backend discriminator, set by each concrete manager.
    backend_kind: str = "ext"

    def __init__(self, *, isolation: IsolationPolicy) -> None:
        """Bind the isolation policy via the parent constructor.

        Args:
            isolation (`IsolationPolicy`):
                Isolation grain forwarded to
                :class:`WorkspaceManagerBase`.
        """
        super().__init__(isolation=isolation)

    async def manager_metrics(self) -> dict[str, Any]:
        """Return a small dict of manager-level observability fields.

        Returns:
            `dict[str, Any]`:
                Manager metrics.  Always includes ``backend_kind``
                and the live cache size.
        """
        # ``_cache`` is the convention shared by the native managers;
        # we read it defensively because some pooling managers may use
        # a different internal structure.
        cache = getattr(self, "_cache", None)
        cache_size = len(cache) if isinstance(cache, dict) else 0
        return {
            "backend_kind": self.backend_kind,
            "cache_size": cache_size,
        }
