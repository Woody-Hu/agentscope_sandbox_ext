# -*- coding: utf-8 -*-
"""agentscope-sandbox-ext

Extension sandbox backends for the agentScope framework.

This package adds three new sandboxed-workspace backends without
modifying any agentscope native code — everything is composed by
inheritance from agentscope's public (and documented-subclassing)
private abstractions:

* :class:`FirecrackerWorkspace` — a Firecracker microVM per
  workspace, driven through the Firecracker REST API over a
  Unix-domain socket and bridged to the host via a tiny stdlib-only
  guest agent reachable over virtio-vsock.

* :class:`GVisorWorkspace` — a Docker container forced onto the
  ``runsc`` (gVisor) runtime, inheriting the entire
  :class:`agentscope.workspace.DockerWorkspace` flow and only
  overriding ``HostConfig.Runtime``.

* :class:`KataWorkspace` — a Docker container forced onto a Kata
  Containers runtime, combining VM-grade isolation with container
  ergonomics.

Each workspace inherits from a single
:class:`SandboxedWorkspaceExtBase` so a management plane has a
uniform ``isinstance`` discriminator and ``metrics()`` hook while
keeping the full sandboxed-workspace lifecycle (gateway bootstrap,
MCP persistence, skill seeding, offload) inherited from
:class:`agentscope.workspace._sandboxed_base.SandboxedWorkspaceBase`.

Each backend ships with a matching manager (``FirecrackerWorkspaceManager``,
``GVisorWorkspaceManager``, ``KataWorkspaceManager``) that owns a
cache + TTL sweeper and an optional :class:`SandboxPool` pre-warm
tier to smooth out cold-boot latency.

Quickstart
----------

.. code-block:: python

    import asyncio
    from agentscope_sandbox_ext import (
        GVisorWorkspaceManager,
        IsolationPolicy,
    )

    async def main():
        async with GVisorWorkspaceManager(
            basedir="/tmp/as-workspaces",
            isolation=IsolationPolicy.PER_AGENT,
        ) as mgr:
            ws = await mgr.get_workspace(
                user_id="alice",
                agent_id="bash",
                session_id="s1",
            )
            result = await ws.get_backend().exec_shell(
                ["sh", "-c", "echo hello from gVisor"],
            )
            print(result.stdout.decode())

    asyncio.run(main())
"""

from ._base import SandboxedWorkspaceExtBase, SandboxExtManagerBase
from ._firecracker import (
    FirecrackerApi,
    FirecrackerApiError,
    FirecrackerBackend,
    FirecrackerWorkspace,
    FirecrackerWorkspaceManager,
    GuestAgentClient,
    GuestAgentError,
)
from ._gvisor import GVisorWorkspace, GVisorWorkspaceManager
from ._kata import KataWorkspace, KataWorkspaceManager
from ._sysbox import SysboxWorkspace, SysboxWorkspaceManager
from ._pool import SandboxPool

# Re-export IsolationPolicy so callers do not need to reach into
# agentscope.app.workspace_manager._base for the common case.
from agentscope.app.workspace_manager._base import IsolationPolicy

__version__ = "0.1.0"

__all__ = [
    # Base classes
    "SandboxedWorkspaceExtBase",
    "SandboxExtManagerBase",
    "SandboxPool",
    "IsolationPolicy",
    # Firecracker
    "FirecrackerWorkspace",
    "FirecrackerWorkspaceManager",
    "FirecrackerBackend",
    "FirecrackerApi",
    "FirecrackerApiError",
    "GuestAgentClient",
    "GuestAgentError",
    # gVisor
    "GVisorWorkspace",
    "GVisorWorkspaceManager",
    # Kata
    "KataWorkspace",
    "KataWorkspaceManager",
    # Sysbox
    "SysboxWorkspace",
    "SysboxWorkspaceManager",
    "__version__",
]
