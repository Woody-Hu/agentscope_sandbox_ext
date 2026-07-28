# -*- coding: utf-8 -*-
"""Tests for the new pool performance knobs added in this branch:

* ``acquire_strategy`` — ``"fifo"`` / ``"lifo"`` ordering
* ``health_check`` — liveness probe on acquire, evict-and-retry on failure
* ``max_concurrent_provisions`` — semaphore-bounded factory concurrency

Plus regression coverage for the live + pending capacity accounting
that prevents oversubscription under concurrent ``acquire`` calls.

All tests use a **real** :class:`_RealTestSandbox` subclass of
:class:`SandboxedWorkspaceExtBase` — the same factory used by
``test_pool.py``.  No mocks: the sandbox really flips ``is_alive``,
really records provision / close counts, and really does file I/O in
``_provision``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from agentscope_sandbox_ext._base import SandboxedWorkspaceExtBase
from agentscope_sandbox_ext._pool import ACQUIRE_STRATEGIES, SandboxPool


# ── real test sandbox ──────────────────────────────────────────


class _RealTestSandbox(SandboxedWorkspaceExtBase):
    """A real sandbox that does real work without needing a microVM.

    Mirrors the test sandbox in ``test_pool.py`` but adds a
    ``kill_after_provision`` flag the health-check tests use to
    simulate a sandbox that dies while pooled.  Every instance really
    creates a temp file in ``_provision`` and really records
    provision / close counts so tests can assert on real side effects.
    """

    sandbox_kind = "test"

    # Class-level registry so tests can count real provisions.
    _instances: list["_RealTestSandbox"] = []

    def __init__(
        self,
        *,
        workspace_id: str | None = None,
        provision_delay: float = 0.0,
    ) -> None:
        # Skip the heavy parent ``__init__`` (which wires up MCPs /
        # skills / sessions).  We only need the pool-facing surface.
        self.workspace_id = workspace_id or f"test-{id(self)}"
        self.is_alive = False
        self._closed = False
        self._provision_count = 0
        self._close_count = 0
        self._provision_delay = provision_delay
        self._health_check_calls = 0
        type(self)._instances.append(self)

    @classmethod
    async def verify_runtime_available(cls) -> None:
        """Always available in tests."""
        return

    async def _provision(self) -> None:
        """Real provision: create a temp file to prove real I/O."""
        import tempfile

        if self._provision_delay:
            await asyncio.sleep(self._provision_delay)
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
        ws = _RealTestSandbox(provision_delay=delay)
        await ws._provision()
        return ws
    return _make


# ── construction / validation ─────────────────────────────────


def test_pool_rejects_invalid_acquire_strategy():
    """An unknown ``acquire_strategy`` raises :class:`ValueError`."""
    with pytest.raises(ValueError, match="acquire_strategy"):
        SandboxPool(_factory(), acquire_strategy="random")


def test_pool_rejects_negative_concurrent_provisions():
    """A negative ``max_concurrent_provisions`` raises ValueError."""
    with pytest.raises(ValueError, match="max_concurrent_provisions"):
        SandboxPool(_factory(), max_concurrent_provisions=-1)


def test_acquire_strategies_constant_lists_valid_values():
    """``ACQUIRE_STRATEGIES`` advertises the accepted values."""
    assert set(ACQUIRE_STRATEGIES) == {"fifo", "lifo"}


def test_default_acquire_strategy_is_fifo():
    """Default strategy is ``"fifo"`` (preserves old behaviour)."""
    pool = SandboxPool(_factory())
    assert pool.acquire_strategy == "fifo"


# ── LIFO acquisition strategy ────────────────────────────────


async def test_lifo_returns_most_recently_returned_first():
    """Under LIFO the most recently returned sandbox is acquired next.

    Provisions two sandboxes A then B, releases them in order
    A → B, then acquires twice.  LIFO returns B first (newest), then
    A (oldest).  FIFO would return A first.
    """
    pool = SandboxPool(
        _factory(),
        max_size=4,
        acquire_strategy="lifo",
        enable_prewarm=False,
    )
    await pool.start()
    try:
        a = await pool.acquire()
        b = await pool.acquire()
        assert a is not b
        # Return A first, then B — B is the newest.
        await pool.release(a)
        await pool.release(b)
        # LIFO: pop B (most recent) first.
        first = await pool.acquire()
        second = await pool.acquire()
        assert first is b
        assert second is a
    finally:
        await pool.aclose()


async def test_fifo_returns_oldest_returned_first():
    """Under FIFO the oldest pooled sandbox is acquired first.

    This is the historical behaviour; assert it is preserved when
    ``acquire_strategy="fifo"`` is explicit.
    """
    pool = SandboxPool(
        _factory(),
        max_size=4,
        acquire_strategy="fifo",
        enable_prewarm=False,
    )
    await pool.start()
    try:
        a = await pool.acquire()
        b = await pool.acquire()
        await pool.release(a)
        await pool.release(b)
        # FIFO: pop A (oldest) first.
        first = await pool.acquire()
        second = await pool.acquire()
        assert first is a
        assert second is b
    finally:
        await pool.aclose()


async def test_lifo_strategy_property_round_trip():
    """The ``acquire_strategy`` property reflects the constructor arg."""
    pool = SandboxPool(_factory(), acquire_strategy="lifo")
    assert pool.acquire_strategy == "lifo"


# ── health check ─────────────────────────────────────────────


async def test_health_check_skipped_when_none():
    """When no health_check is configured, acquire trusts is_alive.

    Regression: the fast path must not call any probe — just return.
    """
    pool = SandboxPool(_factory(), max_size=2, enable_prewarm=False)
    await pool.start()
    try:
        ws = await pool.acquire()
        # The test sandbox records nothing on acquire; the only
        # observable proof is that we got *a* sandbox and it is alive.
        assert ws.is_alive is True
    finally:
        await pool.aclose()


async def test_health_check_invoked_on_warm_candidate():
    """A configured health_check is called on the warm candidate.

    Uses a probe that records the candidate it sees and returns True
    (sandbox is healthy).  Asserts the probe was called exactly once
    and saw the pooled sandbox.
    """
    probed: list[_RealTestSandbox] = []

    async def _probe(ws: _RealTestSandbox) -> bool:
        probed.append(ws)
        return True

    pool = SandboxPool(
        _factory(),
        max_size=2,
        acquire_strategy="fifo",
        health_check=_probe,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        ws = await pool.acquire()
        await pool.release(ws)
        # Second acquire reuses the warm sandbox — health check fires.
        ws2 = await pool.acquire()
        assert ws2 is ws
        assert len(probed) == 1
        assert probed[0] is ws
    finally:
        await pool.aclose()


async def test_health_check_failure_evicts_and_retries():
    """When the health check returns False the candidate is torn down
    and the pool tries the next one.

    Puts two sandboxes in the pool, marks the first as failing via the
    probe, then acquires.  Assert the caller receives the second
    (healthy) sandbox and the first was really closed.
    """
    # Manually push two sandboxes so we control ordering under FIFO.
    a = _RealTestSandbox()
    await a._provision()
    b = _RealTestSandbox()
    await b._provision()

    fail_first = {"value": True}

    async def _probe(ws: _RealTestSandbox) -> bool:
        if ws is a and fail_first["value"]:
            return False
        return True

    pool = SandboxPool(
        _factory(),
        max_size=4,
        acquire_strategy="fifo",
        health_check=_probe,
        enable_prewarm=False,
    )
    # Bypass the factory: push a then b into the free list directly.
    pool._free.append((a, time.monotonic()))
    pool._free.append((b, time.monotonic()))
    await pool.start()
    try:
        ws = await pool.acquire()
        # First candidate (a) failed the probe; second (b) is returned.
        assert ws is b
        # ``a`` was really torn down.
        assert a._closed is True
        assert a._close_count == 1
        # ``b`` is now in_use and is alive.
        assert b.is_alive is True
    finally:
        fail_first["value"] = False
        await pool.aclose()


async def test_health_check_exception_treated_as_failure():
    """If the health_check callable raises, the candidate is torn down
    and the pool retries.  This protects callers from a buggy probe."""
    a = _RealTestSandbox()
    await a._provision()
    b = _RealTestSandbox()
    await b._provision()

    call_count = {"value": 0}

    async def _probe(ws: _RealTestSandbox) -> bool:
        call_count["value"] += 1
        if ws is a:
            raise RuntimeError("probe exploded")
        return True

    pool = SandboxPool(
        _factory(),
        max_size=4,
        acquire_strategy="fifo",
        health_check=_probe,
        enable_prewarm=False,
    )
    pool._free.append((a, time.monotonic()))
    pool._free.append((b, time.monotonic()))
    await pool.start()
    try:
        ws = await pool.acquire()
        assert ws is b
        assert a._closed is True
        assert call_count["value"] == 2  # a (raised) + b (ok)
    finally:
        await pool.aclose()


async def test_health_check_not_called_on_fresh_provision():
    """A freshly provisioned sandbox skips the health check — the
    factory already verified it is alive, so calling the probe would
    be redundant work."""
    probe_calls = {"value": 0}

    async def _probe(ws: _RealTestSandbox) -> bool:
        probe_calls["value"] += 1
        return True

    pool = SandboxPool(
        _factory(),
        max_size=2,
        health_check=_probe,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        ws = await pool.acquire()
        assert ws.is_alive is True
        # Fresh provision path — no probe call.
        assert probe_calls["value"] == 0
    finally:
        await pool.aclose()


# ── concurrent provision limiter ────────────────────────────


async def test_max_concurrent_provisions_serialises_factory_calls():
    """With ``max_concurrent_provisions=1`` only one factory call runs
    at a time, even when N acquires miss the pool simultaneously.

    The factory sleeps 50 ms per provision; with the limiter set to 1
    four concurrent acquires must take at least ~200 ms.  Without the
    limiter they would run in parallel and finish in ~50 ms.
    """
    in_flight = {"value": 0, "max": 0}

    async def _slow_factory() -> _RealTestSandbox:
        in_flight["value"] += 1
        in_flight["max"] = max(in_flight["max"], in_flight["value"])
        try:
            ws = _RealTestSandbox(provision_delay=0.05)
            await ws._provision()
            return ws
        finally:
            in_flight["value"] -= 1

    pool = SandboxPool(
        _slow_factory,
        max_size=4,
        max_concurrent_provisions=1,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        t0 = time.monotonic()
        results = await asyncio.gather(*(pool.acquire() for _ in range(4)))
        elapsed = time.monotonic() - t0
        # Max in-flight must be 1 — the limiter really serialises.
        assert in_flight["max"] == 1
        # Four 50 ms provisions in series → ≥ 0.18 s with margin.
        assert elapsed >= 0.18, f"elapsed={elapsed:.3f}s"
        assert len(results) == 4
    finally:
        await pool.aclose()


async def test_unlimited_provisions_run_in_parallel():
    """With ``max_concurrent_provisions=0`` (default) provisions run
    in parallel.  Regression test for the old behaviour."""
    in_flight = {"value": 0, "max": 0}

    async def _slow_factory() -> _RealTestSandbox:
        in_flight["value"] += 1
        in_flight["max"] = max(in_flight["max"], in_flight["value"])
        try:
            ws = _RealTestSandbox(provision_delay=0.05)
            await ws._provision()
            return ws
        finally:
            in_flight["value"] -= 1

    pool = SandboxPool(
        _slow_factory,
        max_size=4,
        max_concurrent_provisions=0,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        await asyncio.gather(*(pool.acquire() for _ in range(4)))
        # No limiter → at least 2 provisions overlapped.
        assert in_flight["max"] >= 2
    finally:
        await pool.aclose()


async def test_max_concurrent_provisions_property():
    """The ``max_concurrent_provisions`` property reflects the config."""
    pool = SandboxPool(_factory(), max_concurrent_provisions=3)
    assert pool.max_concurrent_provisions == 3
    pool2 = SandboxPool(_factory())
    assert pool2.max_concurrent_provisions == 0


# ── live + pending capacity accounting ─────────────────────


async def test_concurrent_acquires_cannot_oversubscribe_max_size():
    """N concurrent acquires on an empty pool never grow the live +
    pending count above ``max_size``.

    Before the pending-count fix this test would fail because every
    concurrent acquire saw ``total_count < max_size`` simultaneously
    and provisioned N sandboxes even when N > max_size.
    """
    in_flight = {"value": 0, "max": 0}
    provisioned = {"value": 0}

    async def _slow_factory() -> _RealTestSandbox:
        in_flight["value"] += 1
        in_flight["max"] = max(in_flight["max"], in_flight["value"])
        try:
            ws = _RealTestSandbox(provision_delay=0.05)
            await ws._provision()
            provisioned["value"] += 1
            return ws
        finally:
            in_flight["value"] -= 1

    pool = SandboxPool(
        _slow_factory,
        max_size=2,
        enable_prewarm=False,
        acquire_timeout=5.0,
    )
    await pool.start()
    try:
        # Launch 5 concurrent acquires; only 2 can run concurrently.
        # The other 3 must wait for a return.
        async def _acquire_then_release(idx: int):
            ws = await pool.acquire()
            # Hold for a bit so concurrent acquires actually contend.
            await asyncio.sleep(0.05)
            await pool.release(ws)
            return idx

        await asyncio.gather(*(_acquire_then_release(i) for i in range(5)))
        # Even though 5 acquires fired simultaneously, at no point did
        # the in-flight provision count exceed max_size.
        assert in_flight["max"] <= 2
    finally:
        await pool.aclose()


async def test_pending_count_returns_to_zero_after_provision():
    """After all provisions finish ``pending_count`` is back to 0."""
    pool = SandboxPool(
        _factory(delay=0.02),
        max_size=3,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        await asyncio.gather(*(pool.acquire() for _ in range(3)))
        assert pool.pending_count == 0
    finally:
        await pool.aclose()


async def test_pending_count_tracks_in_flight_provisions():
    """While a provision is in flight, ``pending_count`` is ≥ 1."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def _blocked_factory() -> _RealTestSandbox:
        started.set()
        await release.wait()
        ws = _RealTestSandbox()
        await ws._provision()
        return ws

    pool = SandboxPool(
        _blocked_factory,
        max_size=2,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        task = asyncio.create_task(pool.acquire())
        await started.wait()
        # While the factory is blocked, the pending slot is held.
        assert pool.pending_count == 1
        release.set()
        await task
        # After the provision finishes pending drops back to 0.
        assert pool.pending_count == 0
    finally:
        release.set()
        await pool.aclose()


# ── metrics ─────────────────────────────────────────────────


async def test_metrics_include_new_fields():
    """``metrics()`` reports ``pending``, ``acquire_strategy`` and
    ``max_concurrent_provisions``."""
    pool = SandboxPool(
        _factory(),
        max_size=2,
        acquire_strategy="lifo",
        max_concurrent_provisions=2,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        m = await pool.metrics()
        assert m["acquire_strategy"] == "lifo"
        assert m["max_concurrent_provisions"] == 2
        assert m["health_check_enabled"] is False
        assert m["pending"] == 0
    finally:
        await pool.aclose()


async def test_metrics_report_health_check_enabled():
    """When a health_check is configured ``metrics`` says so."""
    async def _probe(_ws: SandboxedWorkspaceExtBase) -> bool:
        return True

    pool = SandboxPool(
        _factory(),
        max_size=2,
        health_check=_probe,
        enable_prewarm=False,
    )
    await pool.start()
    try:
        m = await pool.metrics()
        assert m["health_check_enabled"] is True
    finally:
        await pool.aclose()


# ── combined scenario ───────────────────────────────────────


async def test_lifo_with_health_check_under_load():
    """End-to-end: LIFO + health check + concurrent acquires.

    Prewarms 2 sandboxes, runs 4 concurrent acquires, releases all,
    acquires again — the LIFO order should hold and the health check
    should be called on each warm candidate."""
    probed: list[_RealTestSandbox] = []

    async def _probe(ws: _RealTestSandbox) -> bool:
        probed.append(ws)
        return True

    pool = SandboxPool(
        _factory(),
        max_size=3,
        min_warm=2,
        acquire_strategy="lifo",
        health_check=_probe,
        acquire_timeout=5.0,
    )
    await pool.start()
    try:
        # Wait for the prewarmer.
        await asyncio.sleep(1.0)
        assert pool.warm_count >= 2

        # Acquire all warm sandboxes (drives health-check calls).
        acquired = await asyncio.gather(
            *(asyncio.create_task(pool.acquire()) for _ in range(2))
        )
        assert all(ws.is_alive for ws in acquired)
        assert len(probed) == 2

        # Release in a specific order; LIFO pops the last returned.
        for ws in acquired:
            await pool.release(ws)

        # Acquire one — should be the most recently returned.
        ws = await pool.acquire()
        assert ws is acquired[-1]
        assert len(probed) == 3
    finally:
        await pool.aclose()
