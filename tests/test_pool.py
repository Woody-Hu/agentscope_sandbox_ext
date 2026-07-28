# -*- coding: utf-8 -*-
"""Tests for :class:`SandboxPool`.

The pool is exercised with a **real** factory that produces real
:class:`SandboxedWorkspaceExtBase` subclasses — no mocks.  The test
sandbox really opens files, really spawns subprocesses, and really
tracks an ``is_alive`` flag that the pool's idle-eviction sweeper
relies on.

This gives the pool a genuine workout of:
* acquire / release lifecycle
* pre-warming (``min_warm``)
* idle eviction (``idle_ttl``)
* capacity cap (``max_size``)
* broken-sandbox teardown on release
* background sweep / pre-warm task lifecycle
* metrics reporting
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from agentscope_sandbox_ext._base import SandboxedWorkspaceExtBase
from agentscope_sandbox_ext._pool import SandboxPool


# ── real test sandbox ──────────────────────────────────────────


class _RealTestSandbox(SandboxedWorkspaceExtBase):
    """A real sandbox that does real work without needing a microVM.

    Inherits the full :class:`SandboxedWorkspaceExtBase` contract
    (``sandbox_kind``, ``verify_runtime_available``, ``metrics``,
    ``is_alive``) and implements the abstract hooks so the pool can
    exercise its lifecycle.

    The "provision" step creates a real temp file (so there is real
    I/O to time); ``close`` really marks the sandbox dead and records
    the close call.
    """

    sandbox_kind = "test"

    # Class-level registry of every instance, so tests can assert on
    # how many sandboxes were really created and closed.
    _instances: list["_RealTestSandbox"] = []

    def __init__(self, *, workspace_id: str | None = None) -> None:
        # Skip the heavy parent ``__init__`` (which wires up MCPs /
        # skills / sessions).  We only need the pool-facing surface.
        self.workspace_id = workspace_id or f"test-{id(self)}"
        self.is_alive = False
        self._closed = False
        self._provision_count = 0
        self._close_count = 0
        type(self)._instances.append(self)

    @classmethod
    async def verify_runtime_available(cls) -> None:
        """Always available in tests."""
        return

    async def _provision(self) -> None:
        """Real provision: create a temp file to prove real I/O."""
        import tempfile

        self._provision_count += 1
        # Real file I/O — no mocking.
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"provisioned")
        self.is_alive = True

    async def close(self) -> None:
        """Real close: mark dead and record."""
        self._close_count += 1
        self._closed = True
        self.is_alive = False

    async def metrics(self) -> dict[str, Any]:
        base = await super().metrics()
        base.update(
            {
                "provision_count": self._provision_count,
                "close_count": self._close_count,
            },
        )
        return base


# ── fixtures ──────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_registry():
    """Clear the per-test sandbox registry between tests."""
    _RealTestSandbox._instances.clear()
    yield
    _RealTestSandbox._instances.clear()


def _factory(*, delay: float = 0.0):
    """Return a real factory producing :class:`_RealTestSandbox`."""
    async def _make() -> _RealTestSandbox:
        if delay:
            await asyncio.sleep(delay)
        ws = _RealTestSandbox()
        await ws._provision()
        return ws
    return _make


# ── construction / validation ─────────────────────────────────


def test_pool_rejects_zero_max_size():
    """``max_size`` must be at least 1."""
    with pytest.raises(ValueError, match="max_size"):
        SandboxPool(_factory(), max_size=0)


def test_pool_rejects_negative_min_warm():
    """``min_warm`` cannot be negative."""
    with pytest.raises(ValueError, match="min_warm"):
        SandboxPool(_factory(), min_warm=-1)


def test_pool_rejects_min_warm_above_max_size():
    """``min_warm`` cannot exceed ``max_size``."""
    with pytest.raises(ValueError, match="min_warm"):
        SandboxPool(_factory(), max_size=2, min_warm=3)


# ── basic acquire / release ───────────────────────────────────


async def test_acquire_provisions_when_pool_empty():
    """An acquire on an empty pool really calls the factory."""
    pool = SandboxPool(_factory(), max_size=2)
    await pool.start()
    try:
        ws = await pool.acquire()
        assert ws.is_alive is True
        assert pool.in_use_count == 1
        assert pool.warm_count == 0
    finally:
        await pool.aclose()


async def test_release_returns_sandbox_to_free_list():
    """Releasing a sandbox makes it available for the next acquire."""
    pool = SandboxPool(_factory(), max_size=2)
    await pool.start()
    try:
        ws = await pool.acquire()
        await pool.release(ws)
        assert pool.warm_count == 1
        assert pool.in_use_count == 0
        # Next acquire reuses the released sandbox, no new provision.
        ws2 = await pool.acquire()
        assert ws2 is ws
        assert _RealTestSandbox._instances.__len__() == 1
    finally:
        await pool.aclose()


async def test_acquire_grows_pool_up_to_max_size():
    """Each acquire up to ``max_size`` provisions a fresh sandbox."""
    pool = SandboxPool(_factory(), max_size=3)
    await pool.start()
    try:
        acquired = [await pool.acquire() for _ in range(3)]
        assert pool.in_use_count == 3
        assert len(_RealTestSandbox._instances) == 3
    finally:
        await pool.aclose()


async def test_release_broken_sandbox_is_torn_down():
    """Releasing with ``broken=True`` closes the sandbox rather than
    returning it to the free list."""
    pool = SandboxPool(_factory(), max_size=2)
    await pool.start()
    try:
        ws = await pool.acquire()
        await pool.release(ws, broken=True)
        assert pool.warm_count == 0
        assert ws._closed is True
    finally:
        await pool.aclose()


async def test_release_broken_sandbox_counted_as_closed():
    """A broken sandbox's ``close`` is really called."""
    pool = SandboxPool(_factory(), max_size=2)
    await pool.start()
    try:
        ws = await pool.acquire()
        close_count_before = ws._close_count
        await pool.release(ws, broken=True)
        assert ws._close_count == close_count_before + 1
    finally:
        await pool.aclose()


# ── capacity cap / back-pressure ──────────────────────────────


async def test_acquire_blocks_when_pool_full_then_returns_on_release():
    """When the pool is at capacity, acquire blocks until a sandbox
    is released."""
    pool = SandboxPool(
        _factory(),
        max_size=1,
        acquire_timeout=3.0,
    )
    await pool.start()
    try:
        ws = await pool.acquire()
        # A second acquire should block; release from another task
        # unblocks it.
        async def _release_after_delay():
            await asyncio.sleep(0.2)
            await pool.release(ws)

        asyncio.create_task(_release_after_delay())
        ws2 = await pool.acquire()
        assert ws2 is ws
        assert pool.in_use_count == 1
    finally:
        await pool.aclose()


async def test_acquire_times_out_when_no_sandbox_becomes_available():
    """When the pool is full and nobody releases, acquire raises
    :class:`asyncio.TimeoutError`."""
    pool = SandboxPool(
        _factory(),
        max_size=1,
        acquire_timeout=0.3,
    )
    await pool.start()
    try:
        await pool.acquire()  # fills the pool
        with pytest.raises(asyncio.TimeoutError):
            await pool.acquire()
    finally:
        await pool.aclose()


# ── pre-warming ───────────────────────────────────────────────


async def test_prewarm_maintains_min_warm_in_background():
    """With ``min_warm=2`` the pre-warmer provisions 2 sandboxes
    in the background."""
    pool = SandboxPool(
        _factory(delay=0.01),
        max_size=4,
        min_warm=2,
    )
    await pool.start()
    try:
        # Give the pre-warmer time to provision.
        await asyncio.sleep(1.0)
        assert pool.warm_count >= 2
        assert len(_RealTestSandbox._instances) >= 2
    finally:
        await pool.aclose()


async def test_prewarm_disabled_when_min_warm_zero():
    """``min_warm=0`` disables pre-warming; no sandbox is provisioned
    until the first acquire."""
    pool = SandboxPool(_factory(), max_size=2, min_warm=0)
    await pool.start()
    try:
        await asyncio.sleep(0.3)
        assert pool.warm_count == 0
        assert len(_RealTestSandbox._instances) == 0
    finally:
        await pool.aclose()


# ── idle eviction ─────────────────────────────────────────────


async def test_idle_eviction_closes_idle_sandboxes():
    """A sandbox idle in the pool longer than ``idle_ttl`` is closed
    by the sweeper."""
    pool = SandboxPool(
        _factory(),
        max_size=2,
        idle_ttl=0.1,           # 100 ms idle TTL
        sweep_interval=0.05,    # sweep every 50 ms
        enable_prewarm=False,
    )
    await pool.start()
    try:
        ws = await pool.acquire()
        await pool.release(ws)
        assert pool.warm_count == 1
        # Wait long enough for the sweeper to run and evict.
        await asyncio.sleep(0.3)
        assert pool.warm_count == 0
        assert ws._closed is True
    finally:
        await pool.aclose()


# ── close / aclose ────────────────────────────────────────────


async def test_aclose_closes_all_pooled_sandboxes():
    """``aclose`` closes every sandbox in the pool."""
    pool = SandboxPool(_factory(), max_size=3)
    await pool.start()
    try:
        ws1 = await pool.acquire()
        ws2 = await pool.acquire()
        await pool.release(ws1)
        # ws2 still in use, ws1 in free list.
    finally:
        await pool.aclose()
    assert ws1._closed is True
    assert ws2._closed is True
    assert pool.warm_count == 0
    assert pool.in_use_count == 0


async def test_aclose_is_idempotent():
    """Calling ``aclose`` twice is safe."""
    pool = SandboxPool(_factory(), max_size=2)
    await pool.start()
    await pool.acquire()
    await pool.aclose()
    # Second call must not raise.
    await pool.aclose()


async def test_acquire_after_close_raises():
    """Acquiring from a closed pool raises :class:`RuntimeError`."""
    pool = SandboxPool(_factory(), max_size=2)
    await pool.start()
    await pool.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await pool.acquire()


# ── metrics ───────────────────────────────────────────────────


async def test_metrics_reflect_pool_state():
    """``metrics`` reports accurate warm / in_use / total counts."""
    pool = SandboxPool(_factory(), max_size=3, enable_prewarm=False)
    await pool.start()
    try:
        await pool.acquire()
        await pool.acquire()
        m = await pool.metrics()
        assert m["in_use"] == 2
        assert m["warm"] == 0
        assert m["total"] == 2
        assert m["max_size"] == 3
    finally:
        await pool.aclose()


# ── properties ────────────────────────────────────────────────


def test_properties_expose_config():
    """``max_size`` and ``min_warm`` properties reflect config."""
    pool = SandboxPool(_factory(), max_size=5, min_warm=2)
    assert pool.max_size == 5
    assert pool.min_warm == 2


async def test_total_count_is_warm_plus_in_use():
    """``total_count`` = ``warm_count`` + ``in_use_count``."""
    pool = SandboxPool(_factory(), max_size=3, enable_prewarm=False)
    await pool.start()
    try:
        ws = await pool.acquire()
        await pool.release(ws)
        ws2 = await pool.acquire()
        assert pool.total_count == pool.warm_count + pool.in_use_count
    finally:
        await pool.aclose()
