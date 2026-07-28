# -*- coding: utf-8 -*-
"""FirecrackerWorkspaceManager — lifecycle manager for
:class:`FirecrackerWorkspace`, with an optional
:class:`SandboxPool` pre-warm tier in front of the cache.

Public surface mirrors :class:`agentscope.app.workspace_manager.
DockerWorkspaceManager` (``get_workspace`` / ``close`` /
``close_all`` / async-context-manager semantics) so callers do not
branch on backend.

Pooling design
--------------
Unlike the Docker manager (which evicts idle workspaces only via a
TTL sweeper), the Firecracker manager may compose a
:class:`SandboxPool` to keep ``min_warm`` microVMs hot and ready
for immediate acquisition.  ``get_workspace`` first asks the pool;
on a hit the sandbox is handed straight to the caller without
paying the microVM cold-boot cost.  On ``close`` the sandbox is
returned to the pool (if it is still alive and below the cap) or
torn down.

When ``enable_pool=False`` (the default for tests / single-user
setups) the manager degrades to the same flat-cache behaviour as
the Docker manager.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Self

from agentscope._logging import logger
from agentscope.mcp import MCPClient

from agentscope.app.workspace_manager._base import IsolationPolicy

from .._base import SandboxedWorkspaceExtBase, SandboxExtManagerBase
from .._pool import SandboxPool
from ._constants import (
    DEFAULT_FIRECRACKER_BIN,
    DEFAULT_GATEWAY_PORT,
    DEFAULT_GUEST_AGENT_PORT,
    DEFAULT_GUEST_CID,
    DEFAULT_KERNEL_PATH,
    DEFAULT_MEM_SIZE_MIB,
    DEFAULT_POOL_IDLE_TTL,
    DEFAULT_POOL_MAX_SIZE,
    DEFAULT_POOL_MIN_WARM,
    DEFAULT_ROOTFS_PATH,
    DEFAULT_RUN_DIR,
    DEFAULT_VCPU_COUNT,
)
from ._workspace import FirecrackerWorkspace

DEFAULT_SWEEP_INTERVAL = 300.0


class FirecrackerWorkspaceManager(SandboxExtManagerBase):
    """Manages :class:`FirecrackerWorkspace` instances with optional
    pre-warm pooling and TTL-based idle eviction.

    Use the manager as an ``async with`` context manager: entering it
    starts the TTL sweeper (and pool pre-warmer when enabled), exiting
    it stops both and then closes every cached workspace via
    :meth:`close_all`.
    """

    backend_kind = "firecracker"

    def __init__(
        self,
        *,
        isolation: IsolationPolicy = IsolationPolicy.PER_AGENT,
        bin: str = DEFAULT_FIRECRACKER_BIN,
        kernel_path: str = DEFAULT_KERNEL_PATH,
        rootfs_path: str = DEFAULT_ROOTFS_PATH,
        vcpu_count: int = DEFAULT_VCPU_COUNT,
        mem_size_mib: int = DEFAULT_MEM_SIZE_MIB,
        guest_cid: int = DEFAULT_GUEST_CID,
        guest_agent_port: int = DEFAULT_GUEST_AGENT_PORT,
        gateway_port: int = DEFAULT_GATEWAY_PORT,
        run_dir: str = DEFAULT_RUN_DIR,
        env: dict[str, str] | None = None,
        extra_pip: list[str] | None = None,
        default_mcps: list[MCPClient] | None = None,
        skill_paths: list[str] | None = None,
        ttl: float = 3600.0,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL,
        enable_pool: bool = False,
        pool_max_size: int = DEFAULT_POOL_MAX_SIZE,
        pool_min_warm: int = DEFAULT_POOL_MIN_WARM,
        pool_idle_ttl: float = DEFAULT_POOL_IDLE_TTL,
    ) -> None:
        """Initialize the Firecracker workspace manager.

        Args:
            isolation (`IsolationPolicy`, defaults to `PER_AGENT`):
                Isolation grain for :meth:`assign_workspace_id`.
            bin (`str`, defaults to :data:`DEFAULT_FIRECRACKER_BIN`):
                Firecracker binary path.
            kernel_path (`str`, defaults to :data:`DEFAULT_KERNEL_PATH`):
                Host-side path to the kernel image.
            rootfs_path (`str`, defaults to :data:`DEFAULT_ROOTFS_PATH`):
                Host-side path to the ext4 rootfs image.
            vcpu_count (`int`, defaults to :data:`DEFAULT_VCPU_COUNT`):
                vCPUs per microVM.
            mem_size_mib (`int`, defaults to :data:`DEFAULT_MEM_SIZE_MIB`):
                Guest memory in MiB.
            guest_cid (`int`, defaults to :data:`DEFAULT_GUEST_CID`):
                Guest Context Identifier for virtio-vsock.
            guest_agent_port (`int`, defaults to
            :data:`DEFAULT_GUEST_AGENT_PORT`):
                Port the in-VM guest agent listens on.
            gateway_port (`int`, defaults to :data:`DEFAULT_GATEWAY_PORT`):
                TCP port the in-VM gateway listens on.
            run_dir (`str`, defaults to :data:`DEFAULT_RUN_DIR`):
                Host-side directory for per-VM sockets / logs.
            env (`dict[str, str] | None`, optional):
                Environment variables set inside every VM.
            extra_pip (`list[str] | None`, optional):
                Extra Python packages installed into the gateway venv.
            default_mcps (`list[MCPClient] | None`, optional):
                MCP clients seeded into brand-new workspaces.
            skill_paths (`list[str] | None`, optional):
                Skill directories seeded into brand-new workspaces.
            ttl (`float`, defaults to `3600.0`):
                Seconds before an idle cached workspace is evicted.
            sweep_interval (`float`, defaults to
            `DEFAULT_SWEEP_INTERVAL`):
                How often the idle-eviction sweeper wakes up.
            enable_pool (`bool`, defaults to `False`):
                When ``True``, start a :class:`SandboxPool` pre-warm
                tier in front of the cache.
            pool_max_size (`int`, defaults to
            :data:`DEFAULT_POOL_MAX_SIZE`):
                Hard cap on warm sandboxes in the pool.
            pool_min_warm (`int`, defaults to
            :data:`DEFAULT_POOL_MIN_WARM`):
                Target number of warm sandboxes the pre-warmer
                maintains.  ``0`` disables pre-warming.
            pool_idle_ttl (`float`, defaults to
            :data:`DEFAULT_POOL_IDLE_TTL`):
                Idle TTL for pooled sandboxes.
        """
        super().__init__(isolation=isolation)
        self._bin = bin
        self._kernel_path = kernel_path
        self._rootfs_path = rootfs_path
        self._vcpu_count = vcpu_count
        self._mem_size_mib = mem_size_mib
        self._guest_cid = guest_cid
        self._guest_agent_port = guest_agent_port
        self._gateway_port = gateway_port
        self._run_dir = run_dir
        self._env = dict(env or {})
        self._extra_pip = list(extra_pip or [])
        self._default_mcps = list(default_mcps or [])
        self._skill_paths = list(skill_paths or [])
        self._ttl = ttl
        self._sweep_interval = sweep_interval

        # workspace_id → (workspace, last_access_monotonic)
        self._cache: dict[str, tuple[FirecrackerWorkspace, float]] = {}
        self._lock = asyncio.Lock()
        self._sweep_task: asyncio.Task | None = None

        # Optional pre-warm pool.  Lazily started in __aenter__.
        self._enable_pool = enable_pool
        self._pool: SandboxPool | None = None
        self._pool_max_size = pool_max_size
        self._pool_min_warm = pool_min_warm
        self._pool_idle_ttl = pool_idle_ttl

    # ── workspace construction ────────────────────────────────────

    def _make_workspace(self, workspace_id: str) -> FirecrackerWorkspace:
        """Construct a :class:`FirecrackerWorkspace` from manager config.

        Does NOT call ``initialize`` — the caller decides whether to
        run the full provision flow now or defer it to a pool worker.
        """
        return FirecrackerWorkspace(
            workspace_id=workspace_id,
            bin=self._bin,
            kernel_path=self._kernel_path,
            rootfs_path=self._rootfs_path,
            vcpu_count=self._vcpu_count,
            mem_size_mib=self._mem_size_mib,
            guest_cid=self._guest_cid,
            guest_agent_port=self._guest_agent_port,
            gateway_port=self._gateway_port,
            run_dir=self._run_dir,
            env=self._env,
            extra_pip=self._extra_pip,
            default_mcps=self._default_mcps,
            skill_paths=self._skill_paths,
        )

    async def _build_and_start(
        self,
        *,
        workspace_id: str,
    ) -> FirecrackerWorkspace:
        """Construct and initialise a workspace."""
        ws = self._make_workspace(workspace_id)
        await ws.initialize()
        return ws

    async def _factory(self) -> SandboxedWorkspaceExtBase:
        """Pool factory: build a fresh workspace with a generated id."""
        workspace_id = self.assign_workspace_id(
            user_id="pool",
            agent_id="prewarm",
            session_id="",
        )
        try:
            return await self._build_and_start(workspace_id=workspace_id)
        except Exception:
            logger.exception(
                "FirecrackerWorkspaceManager: pool factory failed for %s",
                workspace_id,
            )
            raise

    # ── public API ────────────────────────────────────────────────

    async def get_workspace(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        workspace_id: str | None = None,
    ) -> FirecrackerWorkspace:
        """Return an initialised workspace, building one on cache miss.

        When the pool is enabled, a warm sandbox from the pool is
        preferred over provisioning a fresh one.
        """
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

        # Pool path: try to acquire a pre-warmed sandbox.
        if self._pool is not None:
            try:
                ws = await self._pool.acquire()
                ws.workspace_id  # type: ignore[statement]
                async with self._lock:
                    self._cache[workspace_id] = (
                        ws,  # type: ignore[arg-type]
                        time.monotonic(),
                    )
                return ws  # type: ignore[return-value]
            except asyncio.TimeoutError:
                logger.warning(
                    "FirecrackerWorkspaceManager: pool acquire "
                    "timed out, falling back to direct provision",
                )

        # Cache miss + no pool: build under the lock.
        async with self._lock:
            cached = self._cache.get(workspace_id)
            if cached is not None:
                ws, _ = cached
                self._cache[workspace_id] = (ws, time.monotonic())
                return ws

            ws = await self._build_and_start(workspace_id=workspace_id)
            self._cache[workspace_id] = (ws, time.monotonic())
            return ws

    async def close(self, workspace_id: str) -> None:
        """Close and evict a single workspace from the cache.

        When the pool is enabled and the sandbox is still alive it is
        returned to the pool instead of torn down (subject to the
        pool's capacity cap).
        """
        async with self._lock:
            entry = self._cache.pop(workspace_id, None)
        if entry is None:
            return
        ws, _ = entry
        if self._pool is not None and ws.is_alive:
            await self._pool.release(ws)
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
                    "Firecracker workspace sweeper tick failed",
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
    async def _safe_close(ws: FirecrackerWorkspace) -> None:
        """Close a workspace, logging any failure instead of raising."""
        try:
            await ws.close()
        except Exception:
            logger.exception(
                "Failed to close FirecrackerWorkspace %s",
                ws.workspace_id,
            )

    # ── metrics ───────────────────────────────────────────────────

    async def manager_metrics(self) -> dict[str, Any]:
        """Return manager-level metrics, including pool stats."""
        base = await super().manager_metrics()
        if self._pool is not None:
            base["pool"] = await self._pool.metrics()
        return base
