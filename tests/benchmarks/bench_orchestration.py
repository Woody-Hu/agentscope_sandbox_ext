# -*- coding: utf-8 -*-
"""Benchmark suite: orchestration-layer resume strategies.

Measures the per-activation cost of the actor resume path under four
strategies, using the zero-cost ``agentfs`` backend so the numbers
reflect the *orchestration* overhead (workflow, CAS, worker pool,
checkpoint bridge) — not container boot cost.

Strategies compared
-------------------

* ``direct``             — raw sandbox provision + exec + close, no
  orchestration.  The baseline: zero actor/workflow overhead.
* ``cold-resume``        — ``resume_actor`` with no golden snapshot and
  no warm-idle worker (``max_warm_idle=0``).  Every activation
  materialises a fresh worker from :class:`SandboxPool` and cold-boots
  (capturing the golden).  This is the "scale-up from zero" path.
* ``warm-idle-resume``   — ``resume_actor`` reusing a warm-idle worker
  (returned by the previous suspend) and restoring the template's
  golden snapshot.  The delta vs. cold-resume is the
  :class:`SandboxPool` acquire cost saved by warm-idle reuse.
* ``snapshot-restore``   — ``resume_actor`` restoring the actor's *own*
  durable snapshot (suspend → resume round-trip), reusing a warm-idle
  worker.  The delta vs. warm-idle-resume is the checkpoint + restore
  cost of carrying actor-private state across suspend.

Each orchestration strategy measures the **resume** latency in isolation
(suspend runs outside the timed region, between iterations, to free the
worker for the next resume).  All strategies run **serially**
(concurrency=1) so the per-activation percentiles are directly
comparable across strategies — the pool-level throughput-under-load
numbers live in ``bench_pool.py``.  ``steady_ops_per_s`` is the serial
resume+suspend cycle rate.

Running
-------

::

    python -m tests.benchmarks.bench_orchestration
    python -m tests.benchmarks.bench_orchestration --json out.json
    python -m tests.benchmarks.bench_orchestration --iters 500
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable

from agentscope_sandbox_ext import AgentFSWorkspace, SandboxPool
from agentscope_sandbox_ext._orchestration import (
    ActorTemplate,
    CheckpointBridge,
    InMemoryActorStore,
    InMemoryRouter,
    InMemoryWorkerStore,
    Orchestrator,
    SandboxClass,
    Scheduler,
    WorkerPool,
)
from agentscope_sandbox_ext._runtime import LocalSnapshotStore


# ── result container ────────────────────────────────────────────


@dataclass
class StrategyResult:
    name: str
    resume_ms: float
    suspend_ms: float
    steady_ops_per_s: float
    p50_resume_ms: float
    p95_resume_ms: float
    p99_resume_ms: float
    resume_latencies_ms: list[float] = field(
        default_factory=list, repr=False,
    )


# ── shared wiring ───────────────────────────────────────────────


def make_factory() -> Callable[[], Awaitable[AgentFSWorkspace]]:
    """Async factory producing a provisioned agentfs workspace."""

    async def _factory() -> AgentFSWorkspace:
        workdir = tempfile.mkdtemp(prefix="as_bench_orch_")
        ws = AgentFSWorkspace(host_workdir=workdir)
        await ws._provision_backend()
        await ws.initialize()
        return ws

    return _factory


def _template(template_id: str = "bash") -> ActorTemplate:
    return ActorTemplate(
        template_id=template_id,
        sandbox_class=SandboxClass.VFS,
        backend_kind="agentfs",
    )


async def _build_orchestrator(
    tmpdir: str,
    *,
    max_warm_idle: int,
    pool_size: int = 8,
) -> tuple[Orchestrator, SandboxPool, WorkerPool, str]:
    """Wire up a fully-real Orchestrator; return (orch, pool, wp, snaps_dir)."""
    sandbox_pool = SandboxPool(
        make_factory(), max_size=pool_size, enable_prewarm=False
    )
    await sandbox_pool.start()
    worker_pool = WorkerPool(
        InMemoryWorkerStore(),
        sandbox_pool,
        Scheduler(),
        max_warm_idle=max_warm_idle,
        node="bench-node",
        idle_ttl=3600.0,
        sweep_interval=3600.0,
    )
    await worker_pool.start()
    snaps_dir = os.path.join(tmpdir, f"snaps-{uuid.uuid4().hex[:8]}")
    os.makedirs(snaps_dir, exist_ok=True)
    checkpoint = CheckpointBridge(LocalSnapshotStore(snaps_dir))
    orch = Orchestrator(
        actor_store=InMemoryActorStore(),
        worker_pool=worker_pool,
        checkpoint=checkpoint,
        router=InMemoryRouter(),
        templates={"bash": _template("bash")},
    )
    return orch, sandbox_pool, worker_pool, snaps_dir


# ── timing helpers ──────────────────────────────────────────────


async def _time_ms(coro: Awaitable[Any]) -> float:
    t0 = time.perf_counter()
    await coro
    return (time.perf_counter() - t0) * 1000.0


def _percentiles(samples: list[float]) -> tuple[float, float, float]:
    if not samples:
        return (0.0, 0.0, 0.0)
    s = sorted(samples)
    n = len(s)

    def _p(p: float) -> float:
        if n == 1:
            return s[0]
        k = (n - 1) * p
        f = int(k)
        c = min(f + 1, n - 1)
        return s[f] + (s[c] - s[f]) * (k - f)

    return (_p(0.50), _p(0.95), _p(0.99))


async def _serial_lats(
    one_req: Callable[[], Awaitable[float]],
    iters: int,
) -> list[float]:
    """Run ``one_req`` ``iters`` times serially, return per-call latencies."""
    lats: list[float] = []
    for _ in range(iters):
        lats.append(await one_req())
    return lats


# ── direct (no orchestration) ──────────────────────────────────


async def bench_direct(
    warmup: int, iters: int, concurrency: int
) -> StrategyResult:
    """Raw sandbox provision + exec + close — the zero-orchestration baseline."""
    factory = make_factory()

    for _ in range(warmup):
        ws = await factory()
        await ws.get_backend().exec_shell(["true"])
        await ws.close()

    cold_ws = await factory()
    resume_ms = await _time_ms(cold_ws.get_backend().exec_shell(["true"]))
    await cold_ws.close()

    async def _one_req() -> float:
        t0 = time.perf_counter()
        w = await factory()
        try:
            await w.get_backend().exec_shell(["true"])
        finally:
            await w.close()
        return (time.perf_counter() - t0) * 1000.0

    lats = await _serial_lats(_one_req, iters)
    p50, p95, p99 = _percentiles(lats)
    total = sum(lats) / 1000.0
    return StrategyResult(
        name="direct",
        resume_ms=resume_ms,
        suspend_ms=0.0,
        steady_ops_per_s=(len(lats) / total) if total else 0.0,
        p50_resume_ms=p50,
        p95_resume_ms=p95,
        p99_resume_ms=p99,
        resume_latencies_ms=lats,
    )


# ── cold-resume (fresh worker, no golden, cold boot) ───────────


async def bench_cold_resume(
    warmup: int, iters: int, concurrency: int
) -> StrategyResult:
    """Resume with no golden + no warm-idle worker (max_warm_idle=0).

    Every activation materialises a fresh worker from the sandbox pool
    and cold-boots (capturing the golden).  Models the "scale-up from
    zero" path where no warm capacity exists.
    """
    tmpdir = tempfile.mkdtemp(prefix="as_bench_cold_")
    orch, sandbox_pool, worker_pool, _ = await _build_orchestrator(
        tmpdir, max_warm_idle=0, pool_size=max(concurrency, 4)
    )
    try:
        async def _activate(actor_id: str) -> tuple[float, float]:
            await orch.create_actor(actor_id, "bash")
            # Clear any captured golden so every resume is a cold boot.
            orch._golden.clear()
            t0 = time.perf_counter()
            await orch.resume_actor(actor_id)
            resume_ms = (time.perf_counter() - t0) * 1000.0
            suspend_ms = await _time_ms(orch.suspend_actor(actor_id))
            return resume_ms, suspend_ms

        # Warmup.
        for i in range(warmup):
            await _activate(f"warmup-{i}")

        # One-shot measurements.
        r_ms, s_ms = await _activate("measure")
        resume_latencies: list[float] = [r_ms]
        suspend_ms = s_ms

        async def _one_req() -> float:
            aid = f"cold-{uuid.uuid4().hex[:8]}"
            r, _ = await _activate(aid)
            return r

        lats = await _serial_lats(_one_req, iters)
        p50, p95, p99 = _percentiles(lats)
        total = sum(lats) / 1000.0
        return StrategyResult(
            name="cold-resume",
            resume_ms=r_ms,
            suspend_ms=suspend_ms,
            steady_ops_per_s=(len(lats) / total) if total else 0.0,
            p50_resume_ms=p50,
            p95_resume_ms=p95,
            p99_resume_ms=p99,
            resume_latencies_ms=lats,
        )
    finally:
        await worker_pool.aclose()
        await sandbox_pool.aclose()


# ── warm-idle-resume (warm worker + golden restore) ────────────


async def bench_warm_idle_resume(
    warmup: int, iters: int, concurrency: int
) -> StrategyResult:
    """Resume reusing a warm-idle worker + restoring the golden snapshot.

    Setup boots one actor (capturing the golden) and suspends it (worker
    returns to warm-idle).  Each iteration creates a new actor of the
    same template and resumes it — the warm-idle worker is reused (no
    :class:`SandboxPool` acquire) and the golden is restored.  The
    delta vs. cold-resume is the sandbox-pool acquire cost saved.
    """
    tmpdir = tempfile.mkdtemp(prefix="as_bench_warm_")
    orch, sandbox_pool, worker_pool, _ = await _build_orchestrator(
        tmpdir, max_warm_idle=max(concurrency, 4), pool_size=max(concurrency, 4)
    )
    try:
        # Bootstrap the golden snapshot with one cold boot.
        await orch.create_actor("golden-bootstrap", "bash")
        await orch.resume_actor("golden-bootstrap")
        await orch.suspend_actor("golden-bootstrap")
        assert "bash" in orch.golden_snapshots
        # The bootstrap worker is now warm-idle.

        async def _activate(actor_id: str) -> tuple[float, float]:
            await orch.create_actor(actor_id, "bash")
            t0 = time.perf_counter()
            await orch.resume_actor(actor_id)
            resume_ms = (time.perf_counter() - t0) * 1000.0
            suspend_ms = await _time_ms(orch.suspend_actor(actor_id))
            return resume_ms, suspend_ms

        for _ in range(warmup):
            await _activate(f"warmup-{uuid.uuid4().hex[:8]}")

        r_ms, s_ms = await _activate("measure")

        async def _one_req() -> float:
            aid = f"warm-{uuid.uuid4().hex[:8]}"
            r, _ = await _activate(aid)
            return r

        lats = await _serial_lats(_one_req, iters)
        p50, p95, p99 = _percentiles(lats)
        total = sum(lats) / 1000.0
        return StrategyResult(
            name="warm-idle-resume",
            resume_ms=r_ms,
            suspend_ms=s_ms,
            steady_ops_per_s=(len(lats) / total) if total else 0.0,
            p50_resume_ms=p50,
            p95_resume_ms=p95,
            p99_resume_ms=p99,
            resume_latencies_ms=lats,
        )
    finally:
        await worker_pool.aclose()
        await sandbox_pool.aclose()


# ── snapshot-restore (own snapshot + warm worker) ──────────────


async def bench_snapshot_restore(
    warmup: int, iters: int, concurrency: int
) -> StrategyResult:
    """Suspend/resume round-trip restoring the actor's own snapshot.

    One actor is resumed (cold boot), mutated with a small state file,
    and suspended (capturing its own durable snapshot).  Each iteration
    resumes (restoring the own snapshot into a warm-idle worker) and
    suspends (re-checkpointing).  The delta vs. warm-idle-resume is the
    checkpoint + restore cost of carrying actor-private state.
    """
    tmpdir = tempfile.mkdtemp(prefix="as_bench_snap_")
    orch, sandbox_pool, worker_pool, _ = await _build_orchestrator(
        tmpdir, max_warm_idle=max(concurrency, 4), pool_size=max(concurrency, 4)
    )
    try:
        # Bootstrap: cold-boot + mutate + suspend → actor has own snapshot.
        await orch.create_actor("snap-actor", "bash")
        await orch.resume_actor("snap-actor")
        actor = await orch.get_actor("snap-actor")
        worker = await orch._workers._store.get(
            actor.worker_assignment.worker_id
        )
        ws = worker.sandbox
        root = ws._host_workdir
        with open(os.path.join(root, "state.db"), "wb") as f:
            f.write(b"snapshot-restore-benchmark-state-v1")
        await orch.suspend_actor("snap-actor")
        assert (await orch.get_actor("snap-actor")).latest_snapshot_ref

        async def _cycle() -> tuple[float, float]:
            t0 = time.perf_counter()
            await orch.resume_actor("snap-actor")
            resume_ms = (time.perf_counter() - t0) * 1000.0
            suspend_ms = await _time_ms(orch.suspend_actor("snap-actor"))
            return resume_ms, suspend_ms

        for _ in range(warmup):
            await _cycle()

        r_ms, s_ms = await _cycle()

        # Serial loop: the suspend/resume cycle is a strict sequence on
        # a single actor, so percentiles are directly comparable to the
        # other strategies' serial measurements.
        lats: list[float] = []
        for _ in range(iters):
            r, _ = await _cycle()
            lats.append(r)
        p50, p95, p99 = _percentiles(lats)
        total = sum(lats) / 1000.0
        return StrategyResult(
            name="snapshot-restore",
            resume_ms=r_ms,
            suspend_ms=s_ms,
            steady_ops_per_s=(len(lats) / total) if total else 0.0,
            p50_resume_ms=p50,
            p95_resume_ms=p95,
            p99_resume_ms=p99,
            resume_latencies_ms=lats,
        )
    finally:
        await worker_pool.aclose()
        await sandbox_pool.aclose()


# ── runner ─────────────────────────────────────────────────────


async def run_all(
    *,
    warmup: int = 5,
    iters: int = 150,
    concurrency: int = 4,
) -> list[StrategyResult]:
    results: list[StrategyResult] = []
    results.append(await bench_direct(warmup, iters, concurrency))
    results.append(await bench_cold_resume(warmup, iters, concurrency))
    results.append(await bench_warm_idle_resume(warmup, iters, concurrency))
    results.append(await bench_snapshot_restore(warmup, iters, concurrency))
    return results


def format_table(results: list[StrategyResult]) -> str:
    headers = [
        "strategy",
        "resume ms",
        "suspend ms",
        "ops/s",
        "p50 ms",
        "p95 ms",
        "p99 ms",
    ]
    rows = [
        [
            r.name,
            f"{r.resume_ms:.3f}",
            f"{r.suspend_ms:.3f}",
            f"{r.steady_ops_per_s:.1f}",
            f"{r.p50_resume_ms:.3f}",
            f"{r.p95_resume_ms:.3f}",
            f"{r.p99_resume_ms:.3f}",
        ]
        for r in results
    ]
    widths = [
        max(len(str(x)) for x in col)
        for col in zip(*([headers] + rows))
    ]
    sep = "+".join("-" * (w + 2) for w in widths)
    lines = [
        "| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |",
        sep,
    ]
    for row in rows:
        lines.append(
            "| " + " | ".join(str(c).ljust(w) for c, w in zip(row, widths)) + " |",
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--iters", type=int, default=150)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument(
        "--json", type=str, default=None,
        help="write JSON results to this path",
    )
    args = parser.parse_args(argv)

    results = asyncio.run(
        run_all(
            warmup=args.warmup,
            iters=args.iters,
            concurrency=args.concurrency,
        )
    )

    print(format_table(results))

    payload = {
        "config": {
            "warmup": args.warmup,
            "iters": args.iters,
            "concurrency": args.concurrency,
        },
        "results": [
            {k: v for k, v in asdict(r).items() if k != "resume_latencies_ms"}
            for r in results
        ],
    }
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nWrote JSON to {args.json}")
    else:
        out = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "docs",
            "orchestration-benchmark-results.json",
        )
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nWrote snapshot to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
