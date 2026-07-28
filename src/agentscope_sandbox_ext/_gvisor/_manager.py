# -*- coding: utf-8 -*-
"""GVisorWorkspaceManager — lifecycle manager for
:class:`GVisorWorkspace`.

Thin specialisation of
:class:`DockerRuntimeWorkspaceManagerBase` that constructs
:class:`GVisorWorkspace` instances and tags cache entries with the
gVisor backend discriminator.
"""

from __future__ import annotations

from agentscope.app.workspace_manager._base import IsolationPolicy
from agentscope.workspace._docker._make_dockerfile import (
    DEFAULT_BASE_IMAGE,
    DEFAULT_GATEWAY_PORT,
)

from .._docker_runtime._base_manager import (
    DEFAULT_SWEEP_INTERVAL,
    DockerRuntimeWorkspaceManagerBase,
)
from .._gvisor._constants import GVISOR_RUNTIME_NAME
from .._gvisor._workspace import GVisorWorkspace


class GVisorWorkspaceManager(DockerRuntimeWorkspaceManagerBase):
    """Manages :class:`GVisorWorkspace` instances with TTL-based
    caching and optional pre-warm pooling.

    Use the manager as an ``async with`` context manager: entering it
    starts the TTL sweeper (and pool pre-warmer when enabled),
    exiting it stops both and then closes every cached workspace via
    :meth:`close_all`.
    """

    backend_kind = "gvisor"

    def __init__(
        self,
        basedir: str,
        *,
        isolation: IsolationPolicy = IsolationPolicy.PER_AGENT,
        base_image: str = DEFAULT_BASE_IMAGE,
        node_version: str | None = "20",
        extra_pip: list[str] | None = None,
        gateway_port: int = DEFAULT_GATEWAY_PORT,
        env: dict[str, str] | None = None,
        default_mcps=None,
        skill_paths: list[str] | None = None,
        ttl: float = 3600.0,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL,
        enable_pool: bool = False,
        pool_max_size: int = 4,
        pool_min_warm: int = 0,
        pool_idle_ttl: float = 1800.0,
        runtime: str = GVISOR_RUNTIME_NAME,
    ) -> None:
        """Initialize the gVisor workspace manager.

        Args:
            basedir (`str`):
                Host root under which per-user/per-agent workdirs are
                created.
            isolation (`IsolationPolicy`, defaults to `PER_AGENT`):
                Isolation grain for :meth:`assign_workspace_id`.
            base_image (`str`, defaults to `DEFAULT_BASE_IMAGE`):
                Base Docker image; must provide ``python3``.
            node_version (`str | None`, defaults to `"20"`):
                Major Node.js version baked into the image.
            extra_pip (`list[str] | None`, optional):
                Extra Python packages installed at image-build time.
            gateway_port (`int`, defaults to `DEFAULT_GATEWAY_PORT`):
                TCP port the gateway listens on inside the container.
            env (`dict[str, str] | None`, optional):
                Environment variables set inside every container.
            default_mcps (`list[MCPClient] | None`, optional):
                MCP clients seeded into brand-new workspaces.
            skill_paths (`list[str] | None`, optional):
                Skill directories seeded into brand-new workspaces.
            ttl (`float`, defaults to `3600.0`):
                Seconds before an idle cached workspace is evicted.
            sweep_interval (`float`, defaults to
            `DEFAULT_SWEEP_INTERVAL`):
                How often the background sweeper wakes up.
            enable_pool (`bool`, defaults to `False`):
                When ``True``, start a :class:`SandboxPool` pre-warm
                tier in front of the cache.
            pool_max_size (`int`, defaults to `4`):
                Hard cap on warm sandboxes in the pool.
            pool_min_warm (`int`, defaults to `0`):
                Target number of warm sandboxes the pre-warmer
                maintains.
            pool_idle_ttl (`float`, defaults to `1800.0`):
                Idle TTL for pooled sandboxes.
            runtime (`str`, defaults to :data:`GVISOR_RUNTIME_NAME`):
                Docker runtime name.  Override only if your daemon
                registers ``runsc`` under a different key.
        """
        super().__init__(
            basedir,
            isolation=isolation,
            base_image=base_image,
            node_version=node_version,
            extra_pip=extra_pip,
            gateway_port=gateway_port,
            env=env,
            default_mcps=default_mcps,
            skill_paths=skill_paths,
            ttl=ttl,
            sweep_interval=sweep_interval,
            enable_pool=enable_pool,
            pool_max_size=pool_max_size,
            pool_min_warm=pool_min_warm,
            pool_idle_ttl=pool_idle_ttl,
        )
        self._runtime = runtime

    def _make_workspace(
        self,
        workspace_id: str,
        *,
        user_id: str,
        agent_id: str,
    ) -> GVisorWorkspace:
        """Construct a :class:`GVisorWorkspace` from manager config."""
        host_workdir = self._workdir_for(user_id=user_id, agent_id=agent_id)
        return GVisorWorkspace(
            workspace_id=workspace_id,
            base_image=self._base_image,
            host_workdir=host_workdir,
            node_version=self._node_version,
            extra_pip=self._extra_pip,
            gateway_port=self._gateway_port,
            env=self._env,
            default_mcps=self._default_mcps,
            skill_paths=self._skill_paths,
            runtime=self._runtime,
        )
