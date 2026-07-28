# -*- coding: utf-8 -*-
"""Shared manager for Docker-runtime-backed workspaces (gVisor, Kata).

Both gVisor and Kata are *Docker runtimes* — they reuse the entire
:class:`agentscope.workspace.DockerWorkspace` image-build /
bind-mount / gateway-bootstrap flow and only differ in the
``HostConfig.Runtime`` name.  Their managers are therefore
structurally identical; this module factors the shared cache +
TTL-sweeper + optional pool logic into a single base class.

The public surface mirrors :class:`agentscope.app.workspace_manager.
DockerWorkspaceManager` (``get_workspace`` / ``close`` /
``close_all`` / async-context-manager semantics) so callers do not
branch on backend.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Self, TypeVar

from agentscope._logging import logger
from agentscope.app.workspace_manager._base import IsolationPolicy
from agentscope.mcp import MCPClient
from agentscope.workspace import DockerWorkspace
from agentscope.workspace._docker._make_dockerfile import (
    DEFAULT_BASE_IMAGE,
    DEFAULT_GATEWAY_PORT,
)

from .._base import SandboxedWorkspaceExtBase, SandboxExtManagerBase
from .._pool import SandboxPool

DEFAULT_SWEEP_INTERVAL = 300.0

#: Type variable bound to a :class:`DockerWorkspace` subclass.
W = TypeVar("W", bound=DockerWorkspace)


class DockerRuntimeWorkspaceManagerBase(SandboxExtManagerBase):
    """Shared cache + TTL sweeper + optional pool for Docker-runtime
    workspaces.

    Concrete subclasses (gVisor, Kata) only need to set
    :attr:`backend_kind` and override :meth:`_make_workspace` to
    construct their specific workspace subclass.
    """

    backend_kind = "docker-runtime"

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
        default_mcps: list[MCPClient] | None = None,
        skill_paths: list[str] | None = None,
        ttl: float = 3600.0,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL,
        enable_pool: bool = False,
        pool_max_size: int = 4,
        pool_min_warm: int = 0,
        pool_idle_ttl: float = 1800.0,
    ) -> None:
        """Initialize the shared manager state.

        Args:
            basedir (`str`):
                Host root under which per-user/per-agent workdirs are
                created (``<basedir>/<user_id>/<agent_id>``).
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
        """
        super().__init__(isolation=isolation)
        self._basedir = os.path.abspath(basedir)
        self._base_image = base_image
        self._node_version = node_version
        self._extra_pip = list(extra_pip or [])
        self._gateway_port = gateway_port
        self._env = dict(env or {})
        self._default_mcps = list(default_mcps or [])
        self._skill_paths = list(skill_paths or [])
        self._ttl = ttl
        self._sweep_interval = sweep_interval

        self._cache: dict[str, tuple[DockerWorkspace, float]] = {}
        self._lock = asyncio.Lock()
        self._sweep_task: asyncio.Task | None = None

        self._enable_pool = enable_pool
        self._pool: SandboxPool | None = None
        self._pool_max_size = pool_max_size
        self._pool_min_warm = pool_min_warm
        self._pool_idle_ttl = pool_idle_ttl

    # ── workspace construction (subclass hook) ───────────────────

    def _make_workspace(
        self,
        workspace_id: str,
        *,
        user_id: str,
        agent_id: str,
    ) -> DockerWorkspace:
        """Construct the concrete workspace subclass.

        Subclasses MUST override this to instantiate their own
        :class:`DockerWorkspace` subclass with the runtime they
        want.  The default implementation raises
        :class:`NotImplementedError` to make the contract explicit.
        """
        raise NotImplementedError(
            "DockerRuntimeWorkspaceManagerBase._make_workspace must be "
            "overridden by the concrete manager",
        )

    async def _build_and_start(
        self,
        workspace_id: str,
        *,
        user_id: str,
        agent_id: str,
    ) -> DockerWorkspace:
        """Construct and initialise a workspace."""
        ws = self._make_workspace(
            workspace_id,
            user_id=user_id,
            agent_id=agent_id,
        )
        await ws.initialize()
        return ws

    async def _factory(self) -> SandboxedWorkspaceExtBase:
        """Pool factory: build a fresh workspace with a generated id."""
        workspace_id = self.assign_workspace_id(
            user_id="pool",
            agent_id="prewarm",
            session_id="",
        )
        return await self._build_and_start(  # type: ignore[return-value]
            workspace_id,
            user_id="pool",
            agent_id="prewarm",
        )

    # ── public API ────────────────────────────────────────────────

    async def get_workspace(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        workspace_id: str | None = None,
    ) -> DockerWorkspace:
        """Return an initialised workspace, building one on cache miss."""
        del session_id  # accepted for interface parity; not used here

        if workspace_id is None:
            workspace_id = self.assign_workspace_id(
                user_id=user_id,
                agent_id=agent_id,
                session_id="",
            )

        async with self._lock:
            cached = self._cache.get(workspace_id)
            if cached is not None:
                ws, _ = cached
                self._cache[workspace_id] = (ws, time.monotonic())
                return ws

        if self._pool is not None:
            try:
                ws = await self._pool.acquire()
                async with self._lock:
                    self._cache[workspace_id] = (ws, time.monotonic())  # type: ignore[arg-type]
                return ws  # type: ignore[return-value]
            except asyncio.TimeoutError:
                logger.warning(
                    "%s: pool acquire timed out, falling back to "
                    "direct provision",
                    type(self).__name__,
                )

        async with self._lock:
            cached = self._cache.get(workspace_id)
            if cached is not None:
                ws, _ = cached
                self._cache[workspace_id] = (ws, time.monotonic())
                return ws

            ws = await self._build_and_start(
                workspace_id,
                user_id=user_id,
                agent_id=agent_id,
            )
            self._cache[workspace_id] = (ws, time.monotonic())
            return ws

    async def close(self, workspace_id: str) -> None:
        """Close and evict a single workspace from the cache."""
        async with self._lock:
            entry = self._cache.pop(workspace_id, None)
        if entry is None:
            return
        ws, _ = entry
        if self._pool is not None and ws.is_alive:
            await self._pool.release(ws)  # type: ignore[arg-type]
        else:
            await self._safe_close(ws)

    async def close_all(self) -> None:
        """Close every cached workspace in parallel."""
        async with self._lock:
            entries = list(self._cache.values())
            self._cache.clear()
        if not entries:
            return
        await asyncio.gather(
            *(self._safe_close(ws) for ws, _ in entries),
            return_exceptions=True,
        )

    # ── async context manager ─────────────────────────────────────

    async def __aenter__(self) -> Self:
        """Start the TTL sweeper and (optionally) the pool pre-warmer."""
        if self._enable_pool and self._pool is None:
            self._pool = SandboxPool(
                factory=self._factory,
                max_size=self._pool_max_size,
                min_warm=self._pool_min_warm,
                idle_ttl=self._pool_idle_ttl,
            )
            await self._pool.start()
        if self._sweep_task is None:
            self._sweep_task = asyncio.create_task(self._sweep_loop())
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Stop background tasks, then close every cached workspace."""
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except (asyncio.CancelledError, Exception):
                pass
            self._sweep_task = None
        if self._pool is not None:
            await self._pool.aclose()
            self._pool = None
        await self.close_all()

    # ── background sweeper ───────────────────────────────────────

    async def _sweep_loop(self) -> None:
        """Periodically evict idle workspaces."""
        while True:
            try:
                await asyncio.sleep(self._sweep_interval)
            except asyncio.CancelledError:
                return
            try:
                await self._sweep_once()
            except Exception:
                logger.exception(
                    "%s sweeper tick failed",
                    type(self).__name__,
                )

    async def _sweep_once(self) -> None:
        """One sweeper tick: evict expired entries and close them."""
        now = time.monotonic()
        async with self._lock:
            expired_ids = [
                wid
                for wid, (_, ts) in self._cache.items()
                if now - ts > self._ttl
            ]
            evicted = [self._cache.pop(wid)[0] for wid in expired_ids]
        if not evicted:
            return
        await asyncio.gather(
            *(self._safe_close(ws) for ws in evicted),
            return_exceptions=True,
        )

    @staticmethod
    async def _safe_close(ws: DockerWorkspace) -> None:
        """Close a workspace, logging any failure instead of raising."""
        try:
            await ws.close()
        except Exception:
            logger.exception(
                "Failed to close workspace %s",
                ws.workspace_id,
            )

    # ── metrics ───────────────────────────────────────────────────

    async def manager_metrics(self) -> dict[str, Any]:
        """Return manager-level metrics, including pool stats."""
        base = await super().manager_metrics()
        if self._pool is not None:
            base["pool"] = await self._pool.metrics()
        return base

    # ── helpers ──────────────────────────────────────────────────

    def _workdir_for(self, user_id: str, agent_id: str) -> str:
        """Resolve the host workdir for ``(user_id, agent_id)``."""
        return os.path.join(self._basedir, user_id, agent_id)
