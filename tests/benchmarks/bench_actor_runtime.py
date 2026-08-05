# -*- coding: utf-8 -*-
"""Benchmark suite: actor / worker / checkpoint runtime.

Measures the three design claims of the modular runtime:

1. **Actor density** — how many actors can a small worker pool sustain?
   Compares direct-provision (1 actor = 1 worker) against the
   actor–worker isolation model (N actors time-multiplexed on M << N
   workers) at varying oversubscription ratios.

2. **Snapshot / restore latency** — pause (node-local) vs suspend
   (durable upload) vs golden clone-restore.  This is the
   cost-vs-durability tradeoff that drives the two-level checkpoint
   design.

3. **Singleflight dedup gain** — how much work does singleflight save
   when N concurrent callers request the same snapshot?  Compares
   dedup-on vs dedup-off under contention.

All benchmarks use the in-process :class:`FakeSandboxRuntime` (real
on-disk I/O, no microVM) so the numbers isolate the *runtime* and
*scheduling* overhead, not container boot cost.

Running
-------

::

    python -m tests.benchmarks.bench_actor_runtime                 # print table
    python -m tests.benchmarks.bench_actor_runtime --json out.json # machine-readable
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable

from agentscope_sandbox_ext._actor._lifecycle import ActorLifecycle
from agentscope_sandbox_ext._actor._registry import (
    InProcessActorRegistry,
    InProcessLockProvider,
)
from agentscope_sandbox_ext._actor._types import (
    ActorRef,
    Constraints,
    SandboxClass,
    TemplateRef,
)
from agentscope_sandbox_ext._checkpoint._manager import CheckpointManager
from agentscope_sandbox_ext._checkpoint._singleflight import Singleflight
from agentscope_sandbox_ext._checkpoint._types import CheckpointConfig
from agentscope_sandbox_ext._runtime.local_snapshot_store import (
    LocalSnapshotStore,
)
from agentscope_sandbox_ext._template._template import (
    ActorTemplateRecord,
    InProcessTemplateRegistry,
    TemplateBaker,
)
from agentscope_sandbox_ext._worker._pool import WorkerPool
from tests._helpers.fake_runtime import make_worker_factory


# ── result containers ───────────────────────────────────────────


@dataclass
class BenchResult:
    name: str
    description: str
    iterations: int
    concurrency: int
    total_ms: float
    ops_per_s: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    extra: dict[str, Any] = field(default_factory=dict)
    latencies_ms: list[float] = field(default_factory=list, repr=False)


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


async def _steady_state(
    one_req: Callable[[], Awaitable[float]],
    iters: int,
    concurrency: int,
) -> list[float]:
    sem = asyncio.Semaphore(concurrency)
    lats: list[float] = [0.0] * iters

    async def _runner(i: int) -> None:
        async with sem:
            lats[i] = await one_req()

    await asyncio.gather(*(_runner(i) for i in range(iters)))
    return lats


def _make_result(
    name: str,
    description: str,
    lats: list[float],
    concurrency: int,
    **extra: Any,
) -> BenchResult:
    p50, p95, p99 = _percentiles(lats)
    total_ms = sum(lats)
    return BenchResult(
        name=name,
        description=description,
        iterations=len(lats),
        concurrency=concurrency,
        total_ms=total_ms,
        ops_per_s=(len(lats) / (total_ms / 1000.0)) if total_ms else 0.0,
        p50_ms=p50,
        p95_ms=p95,
        p99_ms=p99,
        extra=extra,
        latencies_ms=lats,
    )


# ── shared harness ──────────────────────────────────────────────


async def _build_runtime(
    *,
    pool_size: int,
    tmpdir: str,
    warm: bool = False,
):
    """Build a full actor lifecycle stack with real on-disk stores."""
    durable = LocalSnapshotStore(os.path.join(tmpdir, "durable"))
    checkpoint = CheckpointManager(CheckpointConfig(durable_store=durable))
    registry = InProcessActorRegistry()
    templates = InProcessTemplateRegistry()
    locks = InProcessLockProvider()

    pool = WorkerPool(
        make_worker_factory(sandbox_class="vfs"),
        max_size=pool_size,
        enable_prewarm=warm,
        min_warm=pool_size if warm else 0,
        acquire_timeout=30.0,
    )
    await pool.start()

    baker = TemplateBaker(
        factory=make_worker_factory(sandbox_class="vfs"),
        checkpoint=checkpoint,
        registry=templates,
    )
    template_ref = TemplateRef(name="bench", version=1)
    template = ActorTemplateRecord(
        ref=template_ref,
        sandbox_class=SandboxClass.of("vfs"),
        spec={},
    )
    await templates.create(template)
    baked = await baker.bake(template)

    lifecycle = ActorLifecycle(
        registry=registry,
        templates=templates,
        pool=pool,
        checkpoint=checkpoint,
        lock_provider=locks,
    )
    return lifecycle, pool, checkpoint, baked, template_ref


def _constraints() -> Constraints:
    return Constraints(sandbox_class=SandboxClass.of("vfs"))


def _actor(i: int) -> ActorRef:
    return ActorRef(namespace="bench", name=f"a{i}")


# ── bench 1: actor density ──────────────────────────────────────


async def bench_density(
    tmpdir: str,
    *,
    actors: int = 50,
    pool_size: int = 4,
    iters: int = 200,
    concurrency: int = 16,
) -> BenchResult:
    """Actor–worker isolation: N actors time-multiplexed on M workers.

    Each iteration: resume → tiny work → suspend.  The pool has
    ``pool_size`` workers; the benchmark proves that ``actors``
    (>> ``pool_size``) can all be serviced.

    Concurrency is capped at ``actors`` so two concurrent requests never
    target the same actor (which would collide on the per-actor keyed
    lock and produce ``IllegalTransition`` rather than measuring
    density).
    """
    lifecycle, pool, _cp, baked, tref = await _build_runtime(
        pool_size=pool_size, tmpdir=tmpdir,
    )
    eff_concurrency = min(concurrency, actors)
    try:
        # Pre-create all actors.
        for i in range(actors):
            await lifecycle.create_actor(_actor(i), tref, _constraints())

        # Free-actor queue: a request borrows an actor, runs
        # resume→suspend, then returns it.  This guarantees no two
        # concurrent requests target the same actor (which would
        # collide on the per-actor keyed lock and raise
        # ``IllegalTransition`` instead of measuring density).
        free_actors: asyncio.Queue[int] = asyncio.Queue()
        for i in range(actors):
            free_actors.put_nowait(i)

        async def _one_req() -> float:
            idx = await free_actors.get()
            try:
                ref = _actor(idx)
                t0 = time.perf_counter()
                await lifecycle.resume_actor(ref)
                await lifecycle.suspend_actor(ref)
                return (time.perf_counter() - t0) * 1000.0
            finally:
                free_actors.put_nowait(idx)

        lats = await _steady_state(_one_req, iters, eff_concurrency)
        return _make_result(
            name=f"density-{actors}a-{pool_size}w",
            description=(
                f"{actors} actors on {pool_size} workers, "
                f"resume→suspend per iter"
            ),
            lats=lats,
            concurrency=eff_concurrency,
            actors=actors,
            pool_size=pool_size,
            density_ratio=actors / pool_size,
        )
    finally:
        await pool.aclose()


# ── bench 2: snapshot latency ───────────────────────────────────


async def bench_snapshot_latency(
    tmpdir: str,
    *,
    iters: int = 100,
) -> list[BenchResult]:
    """Compare pause (node-local) vs suspend (durable) vs golden restore."""
    lifecycle, pool, checkpoint, baked, tref = await _build_runtime(
        pool_size=4, tmpdir=tmpdir,
    )
    results: list[BenchResult] = []
    try:
        ref = _actor(0)
        await lifecycle.create_actor(ref, tref, _constraints())

        # ── pause latency ────────────────────────────────────────
        pause_lats: list[float] = []
        for _ in range(iters):
            await lifecycle.resume_actor(ref)
            t0 = time.perf_counter()
            await lifecycle.pause_actor(ref)
            pause_lats.append((time.perf_counter() - t0) * 1000.0)
        results.append(_make_result(
            name="pause-latency",
            description="pause (node-local capture, no upload)",
            lats=pause_lats,
            concurrency=1,
        ))

        # ── suspend latency ──────────────────────────────────────
        suspend_lats: list[float] = []
        for _ in range(iters):
            await lifecycle.resume_actor(ref)
            t0 = time.perf_counter()
            await lifecycle.suspend_actor(ref)
            suspend_lats.append((time.perf_counter() - t0) * 1000.0)
        results.append(_make_result(
            name="suspend-latency",
            description="suspend (capture + durable upload)",
            lats=suspend_lats,
            concurrency=1,
        ))

        # ── golden clone-restore latency ─────────────────────────
        # Each iter: create a fresh actor (seeds golden) → resume
        # (clone-restores from golden) → delete.
        golden_lats: list[float] = []
        for i in range(iters):
            fresh = _actor(1000 + i)
            await lifecycle.create_actor(fresh, tref, _constraints())
            t0 = time.perf_counter()
            await lifecycle.resume_actor(fresh)
            golden_lats.append((time.perf_counter() - t0) * 1000.0)
            await lifecycle.suspend_actor(fresh)
            await lifecycle.delete_actor(fresh)
        results.append(_make_result(
            name="golden-restore",
            description="resume from golden snapshot (clone-restore)",
            lats=golden_lats,
            concurrency=1,
        ))
        return results
    finally:
        await pool.aclose()


# ── bench 3: singleflight dedup gain ────────────────────────────


async def bench_singleflight(
    tmpdir: str,
    *,
    callers: int = 10,
    iters: int = 30,
) -> list[BenchResult]:
    """Singleflight collapses N concurrent ``checkpoint.suspend`` calls
    on the same actor+worker to 1 execution.

    The lifecycle's keyed lock serialises per-actor *workflow* calls
    (resume/suspend/pause), so singleflight only fires when callers
    hit the checkpoint manager directly — e.g. concurrent operator
    triggers, or a fan-out from a higher layer that bypasses the
    lifecycle.  This bench exercises that path: N callers invoke
    ``checkpoint.suspend(ref, worker)`` concurrently; singleflight
    dedups them to 1 snapshot+upload.  The baseline runs the inner
    work N times (no dedup).

    Returns two results: ``singleflight-on`` (1 execution per batch)
    and ``singleflight-off`` (N executions per batch).  The headline
    metric is ``executions_per_iter`` in :attr:`BenchResult.extra`.
    """
    results: list[BenchResult] = []
    ref = _actor(0)

    # ── with singleflight (default) ───────────────────────────
    # ``snapshot_delay`` gives followers a yield point to pile up in
    # the singleflight table before the leader's synchronous copytree
    # completes — without it, the leader's sync I/O blocks the event
    # loop and each caller runs solo (no dedup window).
    durable_on = LocalSnapshotStore(os.path.join(tmpdir, "sf_on"))
    sf = Singleflight()
    checkpoint_on = CheckpointManager(
        CheckpointConfig(durable_store=durable_on), singleflight=sf,
    )
    factory_on = make_worker_factory(
        sandbox_class="vfs", snapshot_delay=0.005,
    )
    worker_on = await factory_on()
    runtime_on = worker_on.runtime
    try:
        sf_lats: list[float] = []
        sf_execs = 0
        for _ in range(iters):
            runtime_on.calls.clear()  # type: ignore[attr-defined]
            t0 = time.perf_counter()
            await asyncio.gather(
                *(checkpoint_on.suspend(ref, worker_on)
                  for _ in range(callers)),
            )
            sf_lats.append((time.perf_counter() - t0) * 1000.0)
            sf_execs += sum(
                1 for c in runtime_on.calls  # type: ignore[attr-defined]
                if c[0] == "snapshot"
            )
        results.append(_make_result(
            name=f"singleflight-on-{callers}c",
            description=(
                f"{callers} concurrent checkpoint.suspend calls; "
                f"singleflight dedups to 1 execution"
            ),
            lats=sf_lats,
            concurrency=callers,
            callers=callers,
            executions_per_iter=sf_execs / iters,
            dedup_ratio=callers,
        ))
    finally:
        await worker_on.runtime.close()  # type: ignore[attr-defined]

    # ── baseline: no singleflight (inner work runs N times) ───
    durable_off = LocalSnapshotStore(os.path.join(tmpdir, "sf_off"))
    factory_off = make_worker_factory(
        sandbox_class="vfs", snapshot_delay=0.005,
    )
    worker_off = await factory_off()
    runtime_off = worker_off.runtime
    try:
        base_lats: list[float] = []
        base_execs = 0
        for batch in range(iters):
            runtime_off.calls.clear()  # type: ignore[attr-defined]

            async def _one_direct(slot: int, b: int = batch):
                # Unique tag per call so N concurrent executions don't
                # race on the same snapshot directory — models "N
                # redundant executions" which is exactly what
                # singleflight prevents.
                tag = f"{ref.namespace}_{ref.name}_last_{b}_{slot}"
                local_path = await worker_off.runtime.snapshot(tag)  # type: ignore[attr-defined]
                await durable_off.put_tree(
                    actor_id=ref.key, tag=f"last_{b}_{slot}",
                    source_dir=local_path,
                )

            t0 = time.perf_counter()
            await asyncio.gather(
                *(_one_direct(s) for s in range(callers)),
            )
            base_lats.append((time.perf_counter() - t0) * 1000.0)
            base_execs += sum(
                1 for c in runtime_off.calls  # type: ignore[attr-defined]
                if c[0] == "snapshot"
            )
        results.append(_make_result(
            name=f"singleflight-off-{callers}c",
            description=(
                f"{callers} concurrent direct suspend calls; "
                f"no dedup (N executions)"
            ),
            lats=base_lats,
            concurrency=callers,
            callers=callers,
            executions_per_iter=base_execs / iters,
            dedup_ratio=1,
        ))
    finally:
        await worker_off.runtime.close()  # type: ignore[attr-defined]

    return results


# ── orchestration ──────────────────────────────────────────────


async def run_all(
    *,
    tmpdir: str,
    iters: int = 200,
    concurrency: int = 16,
) -> list[BenchResult]:
    results: list[BenchResult] = []

    # Density at different oversubscription ratios.
    for actors, pool_size in [(10, 2), (50, 4), (100, 4)]:
        results.append(await bench_density(
            tmpdir, actors=actors, pool_size=pool_size,
            iters=min(iters, 100), concurrency=concurrency,
        ))

    # Snapshot latency comparison.
    results.extend(await bench_snapshot_latency(
        tmpdir, iters=min(iters, 50),
    ))

    # Singleflight dedup.
    results.extend(await bench_singleflight(
        tmpdir, callers=10, iters=min(iters, 30),
    ))

    return results


def format_table(results: list[BenchResult]) -> str:
    headers = [
        "benchmark", "iters", "conc", "ops/s",
        "p50 ms", "p95 ms", "p99 ms",
    ]
    rows = [
        [
            r.name,
            str(r.iterations),
            str(r.concurrency),
            f"{r.ops_per_s:.1f}",
            f"{r.p50_ms:.3f}",
            f"{r.p95_ms:.3f}",
            f"{r.p99_ms:.3f}",
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
    parser.add_argument("--iters", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument(
        "--json", type=str, default=None,
        help="write JSON results to this path",
    )
    parser.add_argument(
        "--tmpdir", type=str, default=None,
        help="temp directory for snapshot stores (default: system temp)",
    )
    args = parser.parse_args(argv)

    import tempfile
    tmpdir = args.tmpdir or tempfile.mkdtemp(prefix="actor_bench_")
    os.makedirs(tmpdir, exist_ok=True)

    results = asyncio.run(run_all(
        tmpdir=tmpdir,
        iters=args.iters,
        concurrency=args.concurrency,
    ))

    print(format_table(results))
    print(f"\nSnapshot store: {tmpdir}")

    payload = {
        "config": {
            "iters": args.iters,
            "concurrency": args.concurrency,
            "tmpdir": tmpdir,
        },
        "results": [
            {k: v for k, v in asdict(r).items() if k != "latencies_ms"}
            for r in results
        ],
    }
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"Wrote JSON to {args.json}")
    else:
        out = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "docs",
            "actor-benchmark-results.json",
        )
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"Wrote snapshot to {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
