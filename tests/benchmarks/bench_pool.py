# -*- coding: utf-8 -*-
"""Benchmark suite: VFS backend + pool optimisation strategies.

This suite measures **real** acquire / exec / release latency and
steady-state throughput across the optimisation strategies the package
supports, using the zero-cost ``agentfs`` backend so the numbers reflect
the *pool* and *scheduling* overhead — not container boot cost.

Strategies compared
-------------------

* ``direct``            — no pool; every request provisions + tears down.
* ``pool-fifo``         — :class:`SandboxPool`, ``acquire_strategy="fifo"``.
* ``pool-lifo``         — :class:`SandboxPool`, ``acquire_strategy="lifo"``.
* ``prewarm``           — pool with ``min_warm=N`` (warm pool under idle load).
* ``prewarm+ratelimit`` — pool with ``min_warm=N`` AND
  ``max_concurrent_provisions=1`` (thundering-herd guard).
* ``snapshot-restore``  — VFS ``snapshot()`` once, then iterate
  ``mutate → restore()`` to roll back.  Models the iterative agent
  workflow (try a change → test → roll back) and is the bench that
  justifies the snapshot/restore feature shipped on
  :class:`VFSWorkspaceBase`.  See ``docs/SNAPSHOT.md``.

Metrics
-------

* ``cold_acquire_ms``  — first acquire from an empty pool (provision time).
* ``hot_acquire_ms``   — acquire when a sandbox is already warm in the pool.
* ``exec_ms``          — one ``exec_shell(["true"])`` round trip.
* ``release_ms``       — one ``release`` (return to pool).
* ``steady_ops_per_s`` — sustained acquire→exec→release rate at the
  given concurrency.
* ``p50/p95/p99_acquire_ms`` — acquire tail latency during steady state.

For ``snapshot-restore`` the same percentile columns are populated from
the per-iteration restore latency, so the row is directly comparable to
the pool rows even though the workflow is different.

Running
-------

::

    python -m tests.benchmarks.bench_pool                 # print table
    python -m tests.benchmarks.bench_pool --json out.json # machine-readable
    python -m tests.benchmarks.bench_pool --plot out.png   # bar chart

A snapshot is always written to ``docs/benchmark-results.json`` so the
docs can reference a reproducible result.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable

from agentscope_sandbox_ext import (
    AgentFSWorkspace,
    SandboxPool,
    SandboxedWorkspaceExtBase,
)


# ── result containers ───────────────────────────────────────────


@dataclass
class StrategyResult:
    name: str
    cold_acquire_ms: float
    hot_acquire_ms: float
    exec_ms: float
    release_ms: float
    steady_ops_per_s: float
    p50_acquire_ms: float
    p95_acquire_ms: float
    p99_acquire_ms: float
    acquire_latencies_ms: list[float] = field(
        default_factory=list, repr=False,
    )


# ── workspace factory ──────────────────────────────────────────


def make_factory() -> Callable[[], Awaitable[SandboxedWorkspaceExtBase]]:
    """Return an async factory that provisions a fresh agentfs workspace.

    Each workspace gets its own host tempdir so the bench never sees
    cross-workspace I/O contention from a shared directory.
    """

    async def _factory() -> SandboxedWorkspaceExtBase:
        workdir = tempfile.mkdtemp(prefix="as_bench_")
        ws = AgentFSWorkspace(host_workdir=workdir)
        # ``SandboxPool`` expects the factory to hand back a
        # fully-initialised workspace (so acquire latency reflects the
        # real provision cost).  We call the provision + initialize
        # template methods directly — they are the cheap part of the
        # VFS lifecycle, which is exactly the point: the bench isolates
        # pool / scheduling overhead, not container boot.
        await ws._provision_backend()
        await ws.initialize()
        return ws

    return _factory


# ── timing helpers ─────────────────────────────────────────────


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


async def _steady_state(
    one_req: Callable[[], Awaitable[float]],
    iters: int,
    concurrency: int,
) -> list[float]:
    """Run ``iters`` requests at ``concurrency`` parallelism, return latencies."""
    sem = asyncio.Semaphore(concurrency)
    lats: list[float] = [0.0] * iters

    async def _runner(i: int) -> None:
        async with sem:
            lats[i] = await one_req()

    await asyncio.gather(*(_runner(i) for i in range(iters)))
    return lats


# ── per-strategy benches ───────────────────────────────────────


async def bench_direct(
    warmup: int,
    iters: int,
    concurrency: int,
) -> StrategyResult:
    """No pool — every request provisions + tears down a workspace."""
    factory = make_factory()

    # Warmup (excluded from timings).
    for _ in range(warmup):
        ws = await factory()
        await ws.get_backend().exec_shell(["true"])
        await ws.close()

    # Cold acquire == any acquire (no pool to warm).
    cold = await _time_ms(factory())

    # Measure exec_ms on a fresh workspace.
    ws_exec = await factory()
    exec_ms = await _time_ms(ws_exec.get_backend().exec_shell(["true"]))

    async def _one_req() -> float:
        t0 = time.perf_counter()
        w = await factory()
        try:
            await w.get_backend().exec_shell(["true"])
        finally:
            await w.close()
        return (time.perf_counter() - t0) * 1000.0

    lats = await _steady_state(_one_req, iters, concurrency)
    p50, p95, p99 = _percentiles(lats)
    await ws_exec.close()
    return StrategyResult(
        name="direct",
        cold_acquire_ms=cold,
        hot_acquire_ms=cold,  # no hot path
        exec_ms=exec_ms,
        release_ms=0.0,  # close, not release
        steady_ops_per_s=(len(lats) / (sum(lats) / 1000.0)) if lats else 0.0,
        p50_acquire_ms=p50,
        p95_acquire_ms=p95,
        p99_acquire_ms=p99,
        acquire_latencies_ms=lats,
    )


# ── snapshot / restore bench ────────────────────────────────────


def _seed_rollback_workspace(ws: SandboxedWorkspaceExtBase) -> None:
    """Populate *ws* with a realistic iterative-agent starting state.

    The seeded tree mirrors what a real agent session has on disk after
    a few setup steps: a couple of skills, a sessions/ dir with a
    session log, and a handful of data files.  This makes the
    snapshot/reprovision costs non-trivial — a bare workdir would
    artificially flatten the comparison.
    """
    # We use the synchronous ``open`` rather than the backend's
    # ``write_file`` because the workspace's host_workdir is exposed
    # via ``ws._host_workdir`` and we want deterministic bytes on disk
    # for both the snapshot copy and the reseed path.
    root = ws._host_workdir
    for rel, body in [
        ("skills/search/SKILL.md", b"# search skill\n"),
        ("skills/search/helper.py", b"def run(q):\n    return [q]\n"),
        ("skills/edit/SKILL.md", b"# edit skill\n"),
        ("skills/edit/patch.py", b"def patch(*a, **kw):\n    pass\n"),
        ("sessions/s1/log.json", b'{"events": []}\n'),
        ("data/notes.md", b"# scratch notes\n- alpha\n- beta\n"),
        ("data/blob.bin", bytes(range(256)) * 8),
        (".mcp.json", b"[]\n"),
    ]:
        full = os.path.join(root, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(body)


def _mutate(ws: SandboxedWorkspaceExtBase) -> None:
    """Apply a small, deterministic mutation to *ws*'s workdir.

    Models the "agent tried a change" step of the iterative loop.
    """
    root = ws._host_workdir
    with open(os.path.join(root, "data", "notes.md"), "ab") as f:
        f.write(b"- mutated\n")
    with open(os.path.join(root, "scratch.tmp"), "wb") as f:
        f.write(b"throwaway")


async def bench_reprovision_rollback(
    warmup: int,
    iters: int,
    concurrency: int,
    *,
    seed_tree: bool = True,
) -> StrategyResult:
    """Baseline rollback path: close + reprovision + reseed each iter.

    Models the iterative agent workflow *without* snapshot/restore: to
    roll back to a known-good state the workspace is closed, a fresh
    one is provisioned, the layout is re-ensured, and the skill tree
    is re-seeded.  On a real container/microVM backend this is the
    cold-boot cost; on ``agentfs`` it isolates the provision + reseed
    cost, which is the apples-to-apples comparison for the
    snapshot/restore feature.
    """
    factory = make_factory()

    # Build one workspace to measure the "cold" cost of a single
    # provision + layout + seed cycle.
    async def _full_provision() -> SandboxedWorkspaceExtBase:
        ws = await factory()
        if seed_tree:
            # Re-seed the skill/data tree every iteration — this is
            # the cost snapshot/restore avoids.
            _seed_rollback_workspace(ws)
        return ws

    # Warmup (excluded from timings).
    for _ in range(warmup):
        ws = await _full_provision()
        await ws.get_backend().exec_shell(["true"])
        await ws.close()

    cold_ws = await _full_provision()
    cold = await _time_ms(_full_provision())
    exec_ms = await _time_ms(cold_ws.get_backend().exec_shell(["true"]))
    await cold_ws.close()

    async def _one_req() -> float:
        t0 = time.perf_counter()
        # Mutate, then throw the workspace away and provision + reseed
        # a fresh one — that is the rollback.
        w = await _full_provision()
        try:
            _mutate(w)
            await w.get_backend().exec_shell(["true"])
        finally:
            await w.close()
        return (time.perf_counter() - t0) * 1000.0

    lats = await _steady_state(_one_req, iters, concurrency)
    p50, p95, p99 = _percentiles(lats)
    return StrategyResult(
        name="reprovision-rollback",
        cold_acquire_ms=cold,
        hot_acquire_ms=cold,  # no hot path
        exec_ms=exec_ms,
        release_ms=0.0,  # close, not release
        steady_ops_per_s=(len(lats) / (sum(lats) / 1000.0)) if lats else 0.0,
        p50_acquire_ms=p50,
        p95_acquire_ms=p95,
        p99_acquire_ms=p99,
        acquire_latencies_ms=lats,
    )


async def bench_snapshot_restore(
    warmup: int,
    iters: int,
    concurrency: int,
    *,
    seed_tree: bool = True,
) -> StrategyResult:
    """Snapshot/restore rollback path: snapshot once, restore per iter.

    Models the iterative agent workflow *with* snapshot/restore: a
    single ``snapshot("rollback")`` captures the seeded state, then
    every iteration is ``mutate → restore("rollback")``.  The workspace
    stays alive — no re-provision, no re-seed — so the per-iteration
    cost is just the deep-copy restore.

    This is the bench that justifies the snapshot/restore feature on
    :class:`VFSWorkspaceBase`: the per-iteration speedup vs the
    reprovision baseline is the feature's quantified value.
    """
    factory = make_factory()

    # Each concurrent worker needs its own workspace+snapshot (the
    # snapshot lives under a per-workspace snapshots_root and a restore
    # wipes the workdir).  We pre-build a pool of ``concurrency``
    # workspaces, each with its own seeded snapshot, and have the
    # steady-state runners borrow / return them.
    ws_pool: list[SandboxedWorkspaceExtBase] = []
    snap_tag = "rollback"

    async def _make_snapped() -> SandboxedWorkspaceExtBase:
        ws = await factory()
        if seed_tree:
            _seed_rollback_workspace(ws)
        await ws.snapshot(snap_tag)
        return ws

    # Warmup: build + snapshot + tear down ``warmup`` workspaces.
    for _ in range(warmup):
        ws = await _make_snapped()
        await ws.get_backend().exec_shell(["true"])
        await ws.close()

    # Cold acquire = cost of one ``snapshot()`` call (the one-time
    # setup cost the feature adds).  Measure it on a fresh workspace.
    cold_ws = await factory()
    if seed_tree:
        _seed_rollback_workspace(cold_ws)
    cold = await _time_ms(cold_ws.snapshot(snap_tag))
    exec_ms = await _time_ms(cold_ws.get_backend().exec_shell(["true"]))
    # cold_ws becomes the first worker's workspace.

    ws_pool.append(cold_ws)
    for _ in range(max(0, concurrency - 1)):
        ws_pool.append(await _make_snapped())

    # Hot acquire = one ``restore()`` latency (the per-iteration cost
    # the feature promises to keep cheap).
    hot_ws = ws_pool[-1]
    hot = await _time_ms(hot_ws.restore(snap_tag))

    # Steady-state: each runner borrows a workspace, mutates, restores,
    # returns.  We use a simple asyncio.Lock per workspace to enforce
    # "one in-flight op per workspace" — restore is not concurrency-safe
    # against itself on the same workdir.
    ws_locks = [asyncio.Lock() for _ in ws_pool]
    ws_iter = itertools.count()

    async def _one_req() -> float:
        idx = next(ws_iter) % len(ws_pool)
        ws = ws_pool[idx]
        async with ws_locks[idx]:
            t0 = time.perf_counter()
            _mutate(ws)
            await ws.restore(snap_tag)
            return (time.perf_counter() - t0) * 1000.0

    lats = await _steady_state(_one_req, iters, concurrency)
    p50, p95, p99 = _percentiles(lats)

    # Release_ms: time to close one of the warm workspaces (teardown
    # is the only "release" path for a VFS workspace).
    release_t0 = time.perf_counter()
    await ws_pool[0].close()
    release_ms = (time.perf_counter() - release_t0) * 1000.0
    # Tear down the rest.
    for ws in ws_pool[1:]:
        await ws.close()

    return StrategyResult(
        name="snapshot-restore",
        cold_acquire_ms=cold,
        hot_acquire_ms=hot,
        exec_ms=exec_ms,
        release_ms=release_ms,
        steady_ops_per_s=(len(lats) / (sum(lats) / 1000.0)) if lats else 0.0,
        p50_acquire_ms=p50,
        p95_acquire_ms=p95,
        p99_acquire_ms=p99,
        acquire_latencies_ms=lats,
    )


async def _measure_pool(
    *,
    name: str,
    warmup: int,
    iters: int,
    concurrency: int,
    acquire_strategy: str = "fifo",
    min_warm: int = 0,
    max_concurrent_provisions: int = 0,
) -> StrategyResult:
    """Pool-backed strategy with configurable knobs."""
    factory = make_factory()
    pool = SandboxPool(
        factory,
        max_size=max(concurrency, 4),
        min_warm=min_warm,
        idle_ttl=3600.0,  # disable eviction during bench
        sweep_interval=3600.0,
        acquire_strategy=acquire_strategy,
        max_concurrent_provisions=max_concurrent_provisions,
        enable_prewarm=min_warm > 0,
    )
    await pool.start()
    try:
        # Warmup.
        for _ in range(warmup):
            ws = await pool.acquire()
            await ws.get_backend().exec_shell(["true"])
            await pool.release(ws)

        # Cold acquire: fresh pool, no warm sandbox yet. Build a
        # separate pool so we don't disturb the main one's warm state.
        cold_pool = SandboxPool(
            factory,
            max_size=max(concurrency, 4),
            min_warm=0,
            idle_ttl=3600.0,
            sweep_interval=3600.0,
            acquire_strategy=acquire_strategy,
            enable_prewarm=False,
        )
        await cold_pool.start()
        try:
            cold = await _time_ms(cold_pool.acquire())
            # Use the cold-provisioned ws for exec_ms, then release.
            cold_ws = next(iter(cold_pool._in_use))
            exec_ms = await _time_ms(
                cold_ws.get_backend().exec_shell(["true"]),
            )
            await cold_pool.release(cold_ws)
        finally:
            await cold_pool.aclose()

        # Hot acquire: pool has warm sandboxes from warmup releases.
        # Release one to make it warm, then time the acquire.
        warm = await pool.acquire()
        await pool.release(warm)
        hot = await _time_ms(pool.acquire())

        # exec_ms on a hot workspace (reuse the just-acquired one).
        exec_ws = next(iter(pool._in_use))
        release_t0 = time.perf_counter()
        await pool.release(exec_ws)
        release_ms = (time.perf_counter() - release_t0) * 1000.0

        async def _one_req() -> float:
            t0 = time.perf_counter()
            ws = await pool.acquire()
            try:
                await ws.get_backend().exec_shell(["true"])
            finally:
                await pool.release(ws)
            return (time.perf_counter() - t0) * 1000.0

        lats = await _steady_state(_one_req, iters, concurrency)
        p50, p95, p99 = _percentiles(lats)
        # Use the cold-pool exec_ms as the representative exec cost.
        return StrategyResult(
            name=name,
            cold_acquire_ms=cold,
            hot_acquire_ms=hot,
            exec_ms=exec_ms,
            release_ms=release_ms,
            steady_ops_per_s=(len(lats) / (sum(lats) / 1000.0)) if lats else 0.0,
            p50_acquire_ms=p50,
            p95_acquire_ms=p95,
            p99_acquire_ms=p99,
            acquire_latencies_ms=lats,
        )
    finally:
        await pool.aclose()


# ── orchestration ─────────────────────────────────────────────


async def run_all(
    *,
    warmup: int = 5,
    iters: int = 200,
    concurrency: int = 8,
) -> list[StrategyResult]:
    """Run every strategy and return their results."""
    results: list[StrategyResult] = []
    results.append(await bench_direct(warmup, iters, concurrency))
    results.append(
        await _measure_pool(
            name="pool-fifo",
            warmup=warmup,
            iters=iters,
            concurrency=concurrency,
            acquire_strategy="fifo",
        ),
    )
    results.append(
        await _measure_pool(
            name="pool-lifo",
            warmup=warmup,
            iters=iters,
            concurrency=concurrency,
            acquire_strategy="lifo",
        ),
    )
    results.append(
        await _measure_pool(
            name="prewarm",
            warmup=warmup,
            iters=iters,
            concurrency=concurrency,
            acquire_strategy="lifo",
            min_warm=max(concurrency, 4),
        ),
    )
    results.append(
        await _measure_pool(
            name="prewarm+ratelimit",
            warmup=warmup,
            iters=iters,
            concurrency=concurrency,
            acquire_strategy="lifo",
            min_warm=max(concurrency, 4),
            max_concurrent_provisions=1,
        ),
    )
    # Snapshot/restore rollback pair — appended after the pool rows so
    # the pool-vs-direct comparison above stays in its existing order
    # (downstream docs index rows by position).
    results.append(
        await bench_reprovision_rollback(
            warmup=warmup, iters=iters, concurrency=concurrency,
        ),
    )
    results.append(
        await bench_snapshot_restore(
            warmup=warmup, iters=iters, concurrency=concurrency,
        ),
    )
    return results


def format_table(results: list[StrategyResult]) -> str:
    headers = [
        "strategy",
        "cold ms",
        "hot ms",
        "exec ms",
        "release ms",
        "ops/s",
        "p50 ms",
        "p95 ms",
        "p99 ms",
    ]
    rows = [
        [
            r.name,
            f"{r.cold_acquire_ms:.3f}",
            f"{r.hot_acquire_ms:.3f}",
            f"{r.exec_ms:.3f}",
            f"{r.release_ms:.3f}",
            f"{r.steady_ops_per_s:.1f}",
            f"{r.p50_acquire_ms:.3f}",
            f"{r.p95_acquire_ms:.3f}",
            f"{r.p99_acquire_ms:.3f}",
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
    parser.add_argument("--iters", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument(
        "--json", type=str, default=None,
        help="write JSON results to this path",
    )
    parser.add_argument(
        "--plot", type=str, default=None,
        help="write a PNG bar chart to this path",
    )
    args = parser.parse_args(argv)

    results = asyncio.run(
        run_all(
            warmup=args.warmup,
            iters=args.iters,
            concurrency=args.concurrency,
        ),
    )

    print(format_table(results))

    payload = {
        "config": {
            "warmup": args.warmup,
            "iters": args.iters,
            "concurrency": args.concurrency,
        },
        "results": [
            {k: v for k, v in asdict(r).items() if k != "acquire_latencies_ms"}
            for r in results
        ],
    }
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nWrote JSON to {args.json}")
    else:
        # Always persist a snapshot next to docs for reproducibility.
        out = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "docs",
            "benchmark-results.json",
        )
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nWrote snapshot to {out}")

    if args.plot:
        _plot(results, args.plot)
        print(f"Wrote chart to {args.plot}")

    return 0


def _plot(results: list[StrategyResult], path: str) -> None:
    """Render a grouped bar chart of ops/s and p99 latency."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print(
            "matplotlib not installed; skipping plot. "
            "(pip install matplotlib)",
            file=sys.stderr,
        )
        return

    names = [r.name for r in results]
    ops = [r.steady_ops_per_s for r in results]
    p99 = [r.p99_acquire_ms for r in results]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    ax1.bar(names, ops, color="steelblue")
    ax1.set_title("Steady-state throughput (ops/s)")
    ax1.set_ylabel("ops / s")
    ax1.tick_params(axis="x", rotation=20)

    ax2.bar(names, p99, color="firebrick")
    ax2.set_title("p99 acquire latency (ms)")
    ax2.set_ylabel("ms")
    ax2.tick_params(axis="x", rotation=20)

    fig.tight_layout()
    fig.savefig(path, dpi=110)


if __name__ == "__main__":
    raise SystemExit(main())
