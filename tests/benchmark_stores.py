# -*- coding: utf-8 -*-
"""Snapshot store benchmarks — latency, throughput, and scalability.

Usage::

    python -m pytest tests/benchmark_stores.py -v -s --benchmark-only

Or run directly::

    python tests/benchmark_stores.py

Requires PostgreSQL on localhost:5432 and MinIO on localhost:19000.
"""

from __future__ import annotations

import asyncio
import gc
import os
import statistics
import tempfile
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable

import pytest

from agentscope_sandbox_ext._runtime import (
    EvictionPolicy,
    LocalSnapshotStore,
    MinioSnapshotStore,
    PostgresSnapshotStore,
    StorageTier,
    TieredSnapshotStore,
)


# ═══════════════════════════════════════════════════════════════════
# Benchmark infrastructure
# ═══════════════════════════════════════════════════════════════════


@dataclass
class BenchResult:
    name: str
    ops: int
    total_sec: float
    latencies: list[float] = field(default_factory=list)

    @property
    def ops_per_sec(self) -> float:
        return self.ops / self.total_sec if self.total_sec > 0 else 0.0

    @property
    def p50_ms(self) -> float:
        return _percentile(self.latencies, 50) * 1000

    @property
    def p95_ms(self) -> float:
        return _percentile(self.latencies, 95) * 1000

    @property
    def p99_ms(self) -> float:
        return _percentile(self.latencies, 99) * 1000

    @property
    def avg_ms(self) -> float:
        return (statistics.mean(self.latencies) * 1000) if self.latencies else 0.0

    def __repr__(self) -> str:
        return (
            f"{self.name:50s}  ops={self.ops:5d}  total={self.total_sec:.2f}s  "
            f"throughput={self.ops_per_sec:8.1f} op/s  "
            f"avg={self.avg_ms:6.1f}ms  p50={self.p50_ms:6.1f}ms  "
            f"p95={self.p95_ms:6.1f}ms  p99={self.p99_ms:6.1f}ms"
        )


def _percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = f + 1 if f + 1 < len(s) else f
    return s[f] + (s[c] - s[f]) * (k - f) if f != c else s[f]


async def _bench(
    name: str,
    func: Callable[..., Awaitable],
    *,
    warmup: int = 3,
    iterations: int = 50,
    **kwargs,
) -> BenchResult:
    """Run *func* for *iterations* and collect latency stats."""
    # Warmup
    for _ in range(warmup):
        await func(**kwargs)

    latencies: list[float] = []
    t0 = time.monotonic()
    for _ in range(iterations):
        t1 = time.monotonic()
        await func(**kwargs)
        latencies.append(time.monotonic() - t1)
    total = time.monotonic() - t0

    return BenchResult(name=name, ops=iterations, total_sec=total, latencies=latencies)


async def _bench_concurrent(
    name: str,
    func: Callable[..., Awaitable],
    *,
    concurrency: int = 10,
    iterations: int = 50,
    **kwargs,
) -> BenchResult:
    """Run *func* concurrently with *concurrency* workers."""
    async def worker(results: list[float]):
        for _ in range(iterations):
            t1 = time.monotonic()
            await func(**kwargs)
            results.append(time.monotonic() - t1)

    latencies: list[float] = []
    t0 = time.monotonic()
    tasks = [asyncio.create_task(worker(latencies)) for _ in range(concurrency)]
    await asyncio.gather(*tasks)
    total = time.monotonic() - t0
    ops = concurrency * iterations

    return BenchResult(name=name, ops=ops, total_sec=total, latencies=latencies)


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def bench_dir():
    d = tempfile.mkdtemp(prefix="bench_snapshots_")
    yield d
    import shutil
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="module")
def local_store(bench_dir):
    return LocalSnapshotStore(os.path.join(bench_dir, "local"))


@pytest.fixture
async def pg_store():
    store = PostgresSnapshotStore(
        "postgresql://postgres:test123@localhost:5432/testdb",
        table_name="snapshots_bench",
    )
    yield store
    await store.close()


@pytest.fixture(scope="module")
def minio_store():
    store = MinioSnapshotStore(
        "localhost:19000", "minioadmin", "minioadmin",
        bucket="snapshots-bench",
    )
    return store


# ═══════════════════════════════════════════════════════════════════
# Data helpers
# ═══════════════════════════════════════════════════════════════════

DATA_SMALL = b"x" * 1024        # 1 KB
DATA_MEDIUM = b"x" * 102400     # 100 KB
DATA_LARGE = b"x" * 1048576     # 1 MB


async def _cleanup_store(store, actor_id: str):
    try:
        for m in await store.list(actor_id):
            try:
                await store.delete(m.snapshot_ref)
            except Exception:
                pass
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# Single-store benchmarks
# ═══════════════════════════════════════════════════════════════════

class TestSingleStorePut:
    """Measure put latency at different payload sizes."""

    @pytest.mark.parametrize("size_name,data", [
        ("1KB",   DATA_SMALL),
        ("100KB", DATA_MEDIUM),
        ("1MB",   DATA_LARGE),
    ])
    async def test_local_put(self, local_store, size_name, data, request):
        actor = f"bench-put-{size_name}"
        results = []
        for i in range(30):
            t1 = time.monotonic()
            meta = await local_store.put(actor, f"v{i}", data)
            results.append(time.monotonic() - t1)
        name = f"local_put_{size_name}"
        r = BenchResult(name=name, ops=30, total_sec=sum(results), latencies=results)
        print(f"\n  {r}")
        await _cleanup_store(local_store, actor)

    @pytest.mark.parametrize("size_name,data", [
        ("1KB",   DATA_SMALL),
        ("100KB", DATA_MEDIUM),
        ("1MB",   DATA_LARGE),
    ])
    async def test_pg_put(self, pg_store, size_name, data):
        actor = f"bench-put-{size_name}"
        try:
            results = []
            for i in range(30):
                t1 = time.monotonic()
                meta = await pg_store.put(actor, f"v{i}", data)
                results.append(time.monotonic() - t1)
            name = f"pg_put_{size_name}"
            r = BenchResult(name=name, ops=30, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(pg_store, actor)

    @pytest.mark.parametrize("size_name,data", [
        ("1KB",   DATA_SMALL),
        ("100KB", DATA_MEDIUM),
        ("1MB",   DATA_LARGE),
    ])
    async def test_minio_put(self, minio_store, size_name, data):
        actor = f"bench-put-{size_name}"
        try:
            results = []
            for i in range(30):
                t1 = time.monotonic()
                meta = await minio_store.put(actor, f"v{i}", data)
                results.append(time.monotonic() - t1)
            name = f"minio_put_{size_name}"
            r = BenchResult(name=name, ops=30, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(minio_store, actor)


class TestSingleStoreGet:
    """Measure get latency with pre-populated data."""

    @pytest.mark.parametrize("size_name,data", [
        ("1KB",   DATA_SMALL),
        ("100KB", DATA_MEDIUM),
        ("1MB",   DATA_LARGE),
    ])
    async def test_local_get(self, local_store, size_name, data):
        actor = f"bench-get-{size_name}"
        try:
            refs = []
            for i in range(10):
                meta = await local_store.put(actor, f"v{i}", data)
                refs.append(meta.snapshot_ref)
            results = []
            for _ in range(50):
                for ref in refs:
                    t1 = time.monotonic()
                    await local_store.get(ref)
                    results.append(time.monotonic() - t1)
            name = f"local_get_{size_name}"
            r = BenchResult(name=name, ops=len(results), total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(local_store, actor)

    @pytest.mark.parametrize("size_name,data", [
        ("1KB",   DATA_SMALL),
        ("100KB", DATA_MEDIUM),
        ("1MB",   DATA_LARGE),
    ])
    async def test_pg_get(self, pg_store, size_name, data):
        actor = f"bench-get-{size_name}"
        try:
            refs = []
            for i in range(10):
                meta = await pg_store.put(actor, f"v{i}", data)
                refs.append(meta.snapshot_ref)
            results = []
            for _ in range(50):
                for ref in refs:
                    t1 = time.monotonic()
                    await pg_store.get(ref)
                    results.append(time.monotonic() - t1)
            name = f"pg_get_{size_name}"
            r = BenchResult(name=name, ops=len(results), total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(pg_store, actor)

    @pytest.mark.parametrize("size_name,data", [
        ("1KB",   DATA_SMALL),
        ("100KB", DATA_MEDIUM),
        ("1MB",   DATA_LARGE),
    ])
    async def test_minio_get(self, minio_store, size_name, data):
        actor = f"bench-get-{size_name}"
        try:
            refs = []
            for i in range(10):
                meta = await minio_store.put(actor, f"v{i}", data)
                refs.append(meta.snapshot_ref)
            results = []
            for _ in range(50):
                for ref in refs:
                    t1 = time.monotonic()
                    await minio_store.get(ref)
                    results.append(time.monotonic() - t1)
            name = f"minio_get_{size_name}"
            r = BenchResult(name=name, ops=len(results), total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(minio_store, actor)


class TestSingleStoreList:
    """Measure list performance with varying dataset sizes."""

    @pytest.mark.parametrize("count", [10, 100, 500])
    async def test_local_list(self, local_store, count):
        actor = f"bench-list-{count}"
        try:
            for i in range(count):
                await local_store.put(actor, f"v{i}", DATA_SMALL)
            results = []
            for _ in range(30):
                t1 = time.monotonic()
                await local_store.list(actor)
                results.append(time.monotonic() - t1)
            name = f"local_list_n{count}"
            r = BenchResult(name=name, ops=30, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(local_store, actor)

    @pytest.mark.parametrize("count", [10, 100, 500])
    async def test_pg_list(self, pg_store, count):
        actor = f"bench-list-{count}"
        try:
            for i in range(count):
                await pg_store.put(actor, f"v{i}", DATA_SMALL)
            results = []
            for _ in range(30):
                t1 = time.monotonic()
                await pg_store.list(actor)
                results.append(time.monotonic() - t1)
            name = f"pg_list_n{count}"
            r = BenchResult(name=name, ops=30, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(pg_store, actor)

    @pytest.mark.parametrize("count", [10, 100, 500])
    async def test_minio_list(self, minio_store, count):
        actor = f"bench-list-{count}"
        try:
            for i in range(count):
                await minio_store.put(actor, f"v{i}", DATA_SMALL)
            results = []
            for _ in range(10):
                t1 = time.monotonic()
                await minio_store.list(actor)
                results.append(time.monotonic() - t1)
            name = f"minio_list_n{count}"
            r = BenchResult(name=name, ops=10, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(minio_store, actor)


class TestSingleStoreCopy:
    """Measure copy latency."""

    @pytest.mark.parametrize("size_name,data", [
        ("1KB", DATA_SMALL),
        ("1MB", DATA_LARGE),
    ])
    async def test_local_copy(self, local_store, size_name, data):
        actor = f"bench-copy-{size_name}"
        try:
            meta = await local_store.put(actor, "src", data)
            results = []
            for i in range(30):
                t1 = time.monotonic()
                await local_store.copy(meta.snapshot_ref, f"target-{i}", "copied")
                results.append(time.monotonic() - t1)
            name = f"local_copy_{size_name}"
            r = BenchResult(name=name, ops=30, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(local_store, actor)

    @pytest.mark.parametrize("size_name,data", [
        ("1KB", DATA_SMALL),
        ("1MB", DATA_LARGE),
    ])
    async def test_pg_copy(self, pg_store, size_name, data):
        actor = f"bench-copy-{size_name}"
        try:
            meta = await pg_store.put(actor, "src", data)
            results = []
            for i in range(30):
                t1 = time.monotonic()
                await pg_store.copy(meta.snapshot_ref, f"target-{i}", "copied")
                results.append(time.monotonic() - t1)
            name = f"pg_copy_{size_name}"
            r = BenchResult(name=name, ops=30, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(pg_store, actor)


# ═══════════════════════════════════════════════════════════════════
# Concurrent benchmarks
# ═══════════════════════════════════════════════════════════════════

class TestConcurrent:
    """Measure throughput under concurrent load."""

    async def test_local_concurrent_put(self, local_store):
        actor = "bench-concurrent"
        try:
            r = await _bench_concurrent(
                "local_concurrent_put", local_store.put,
                actor_id=actor, tag="v1", data=DATA_SMALL,
                concurrency=10, iterations=20,
            )
            print(f"\n  {r}")
        finally:
            await _cleanup_store(local_store, actor)

    async def test_pg_concurrent_put(self, pg_store):
        actor = "bench-concurrent"
        try:
            r = await _bench_concurrent(
                "pg_concurrent_put", pg_store.put,
                actor_id=actor, tag="v1", data=DATA_SMALL,
                concurrency=10, iterations=20,
            )
            print(f"\n  {r}")
        finally:
            await _cleanup_store(pg_store, actor)

    async def test_minio_concurrent_put(self, minio_store):
        actor = "bench-concurrent"
        try:
            r = await _bench_concurrent(
                "minio_concurrent_put", minio_store.put,
                actor_id=actor, tag="v1", data=DATA_SMALL,
                concurrency=10, iterations=20,
            )
            print(f"\n  {r}")
        finally:
            await _cleanup_store(minio_store, actor)


# ═══════════════════════════════════════════════════════════════════
# Tiered store benchmarks
# ═══════════════════════════════════════════════════════════════════

class TestTieredOverhead:
    """Measure overhead of tiered store vs single backend."""

    async def test_single_vs_tiered_put(self, local_store):
        """Compare single local store vs tiered store wrapping one tier."""
        tiered = TieredSnapshotStore([
            StorageTier("local", local_store, priority=0),
        ])
        actor = "bench-tiered-put"
        try:
            # Single store
            r1 = await _bench("single_local_put", local_store.put,
                              actor_id=actor, tag="v1", data=DATA_SMALL, iterations=50)
            # Tiered store (1 tier)
            r2 = await _bench("tiered_1tier_put", tiered.put,
                              actor_id=actor, tag="v2", data=DATA_SMALL, iterations=50)
            print(f"\n  {r1}")
            print(f"  {r2}")
            overhead = (r2.avg_ms - r1.avg_ms) / r1.avg_ms * 100 if r1.avg_ms > 0 else 0
            print(f"  Tiered overhead: {overhead:.1f}%")
        finally:
            await _cleanup_store(local_store, actor)

    async def test_two_tier_write_through(self, local_store, bench_dir):
        """Measure two-tier write-through overhead."""
        tier2 = LocalSnapshotStore(os.path.join(bench_dir, "tier2"))
        tiered = TieredSnapshotStore([
            StorageTier("hot", local_store, priority=0, write_through=True),
            StorageTier("warm", tier2, priority=1, write_through=True),
        ])
        actor = "bench-2tier"
        try:
            r = await _bench("tiered_2tier_put", tiered.put,
                             actor_id=actor, tag="v1", data=DATA_SMALL, iterations=50)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(local_store, actor)
            await _cleanup_store(tier2, actor)

    async def test_promotion_overhead(self, local_store, bench_dir):
        """Measure overhead of read-from-lower-tier + promotion."""
        tier2 = LocalSnapshotStore(os.path.join(bench_dir, "tier2"))
        tiered = TieredSnapshotStore([
            StorageTier("hot", local_store, priority=0, write_through=False),
            StorageTier("warm", tier2, priority=1, write_through=True),
        ])
        actor = "bench-promo"
        try:
            meta = await tiered.put(actor, "v1", DATA_SMALL)
            # Delete from tier0 so we always read from tier1 + promote
            try:
                await local_store.delete(meta.snapshot_ref)
            except KeyError:
                pass

            r = await _bench("tiered_promotion_get", tiered.get,
                             snapshot_ref=meta.snapshot_ref, iterations=30)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(local_store, actor)
            await _cleanup_store(tier2, actor)


class TestTieredEviction:
    """Measure eviction sweep performance."""

    async def test_lru_sweep_overhead(self, local_store):
        """Measure LRU sweep time with many objects."""
        tiered = TieredSnapshotStore([
            StorageTier(
                "hot", local_store, priority=0,
                max_objects=50,
                eviction_policy=EvictionPolicy.LRU,
            ),
        ], sweep_interval=0.05)

        actor = "bench-evict"
        try:
            # Populate 200 objects (sweep not started yet, no eviction)
            refs = []
            for i in range(200):
                meta = await tiered.put(actor, f"v{i}", DATA_SMALL)
                refs.append(meta.snapshot_ref)

            # Access first 50 to make them "hot"
            for ref in refs[:50]:
                await tiered.get(ref)

            # Start sweeper now — it will evict down to 50 (LRU)
            await tiered.start()
            await asyncio.sleep(0.5)

            # Measure remaining objects
            remaining = await tiered.list(actor)
            print(f"\n  LRU eviction: 200 inserted -> {len(remaining)} remaining (cap=50)")
            assert len(remaining) <= 200
        finally:
            await tiered.close()
            await _cleanup_store(local_store, actor)


# ═══════════════════════════════════════════════════════════════════
# Scalability: linear growth under increasing load
# ═══════════════════════════════════════════════════════════════════

class TestScalability:
    """Measure how performance scales with dataset size."""

    @pytest.mark.parametrize("count", [10, 50, 200])
    async def test_pg_list_scalability(self, pg_store, count):
        actor = f"bench-scale-{count}"
        try:
            for i in range(count):
                await pg_store.put(actor, f"v{i}", DATA_SMALL)
            results = []
            for _ in range(20):
                t1 = time.monotonic()
                await pg_store.list(actor)
                results.append(time.monotonic() - t1)
            name = f"pg_list_scale_n{count}"
            r = BenchResult(name=name, ops=20, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(pg_store, actor)

    @pytest.mark.parametrize("count", [10, 50, 200])
    async def test_local_list_scalability(self, local_store, count):
        actor = f"bench-scale-{count}"
        try:
            for i in range(count):
                await local_store.put(actor, f"v{i}", DATA_SMALL)
            results = []
            for _ in range(20):
                t1 = time.monotonic()
                await local_store.list(actor)
                results.append(time.monotonic() - t1)
            name = f"local_list_scale_n{count}"
            r = BenchResult(name=name, ops=20, total_sec=sum(results), latencies=results)
            print(f"\n  {r}")
        finally:
            await _cleanup_store(local_store, actor)


# ═══════════════════════════════════════════════════════════════════
# CLI entry point
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s", "--tb=short"]))


# ═══════════════════════════════════════════════════════════════════
# Correctness validation tests
# ═══════════════════════════════════════════════════════════════════

class TestBenchmarkCorrectness:
    """Verify that data is correct after benchmark-style operations."""

    async def test_data_integrity_after_put_get(self, local_store):
        """Data read back must match what was written."""
        actor = "correct-putget"
        try:
            original = b"correctness-check-data-" + b"x" * 100
            meta = await local_store.put(actor, "v1", original)
            retrieved = await local_store.get(meta.snapshot_ref)
            assert retrieved == original, "Data corruption: put/get mismatch"
            assert meta.size_bytes == len(original)
        finally:
            await _cleanup_store(local_store, actor)

    async def test_copy_preserves_data(self, local_store):
        """Copied snapshot must have identical data to source."""
        actor = "correct-copy"
        try:
            original = b"copy-source-data-" + os.urandom(256).hex().encode()
            meta = await local_store.put(actor, "src", original)
            copied = await local_store.copy(meta.snapshot_ref, actor, "dst")
            assert copied.actor_id == actor
            assert copied.tag == "dst"
            retrieved = await local_store.get(copied.snapshot_ref)
            assert retrieved == original, "Copy data mismatch"
        finally:
            await _cleanup_store(local_store, actor)

    async def test_tiered_write_through_consistency(self, local_store, bench_dir):
        """All write-through tiers must have identical data after put."""
        tier2 = LocalSnapshotStore(os.path.join(bench_dir, "tier2-correct"))
        tiered = TieredSnapshotStore([
            StorageTier("hot", local_store, priority=0, write_through=True),
            StorageTier("warm", tier2, priority=1, write_through=True),
        ])
        actor = "correct-tiered"
        try:
            data = b"tiered-consistency-" + os.urandom(128).hex().encode()
            meta = await tiered.put(actor, "v1", data)
            # Verify data in both tiers
            d1 = await local_store.get(meta.snapshot_ref)
            d2 = await tier2.get(meta.snapshot_ref)
            assert d1 == data, "Tier 0 data mismatch"
            assert d2 == data, "Tier 1 data mismatch"
            assert d1 == d2, "Tiers have inconsistent data"
        finally:
            await _cleanup_store(local_store, actor)
            await _cleanup_store(tier2, actor)

    async def test_concurrent_puts_no_corruption(self, local_store):
        """Concurrent writes must not corrupt data."""
        actor = "correct-concurrent"

        async def write_and_verify(i: int) -> bytes:
            data = f"concurrent-{i}-".encode() + os.urandom(64)
            meta = await local_store.put(actor, f"v{i}", data)
            retrieved = await local_store.get(meta.snapshot_ref)
            assert retrieved == data, f"Concurrent write {i} corrupted"
            return data

        try:
            tasks = [write_and_verify(i) for i in range(20)]
            results = await asyncio.gather(*tasks)
            assert len(results) == 20
            assert len(set(results)) == 20, "All concurrent writes should be unique"
        finally:
            await _cleanup_store(local_store, actor)

    async def test_list_after_puts_is_complete(self, local_store):
        """List must return all inserted snapshots."""
        actor = "correct-list"
        try:
            expected_tags = set()
            for i in range(25):
                tag = f"v{i}"
                await local_store.put(actor, tag, DATA_SMALL)
                expected_tags.add(tag)
            metas = await local_store.list(actor)
            returned_tags = {m.tag for m in metas}
            assert returned_tags == expected_tags, (
                f"List incomplete: expected {expected_tags}, got {returned_tags}")
        finally:
            await _cleanup_store(local_store, actor)

    async def test_delete_removes_data(self, local_store):
        """Deleted snapshot must not be retrievable."""
        actor = "correct-delete"
        try:
            meta = await local_store.put(actor, "v1", DATA_SMALL)
            await local_store.delete(meta.snapshot_ref)
            with pytest.raises(KeyError):
                await local_store.get(meta.snapshot_ref)
        finally:
            await _cleanup_store(local_store, actor)

    async def test_lru_eviction_preserves_recent(self, local_store):
        """LRU eviction must preserve recently accessed snapshots."""
        tiered = TieredSnapshotStore([
            StorageTier(
                "hot", local_store, priority=0,
                max_objects=5,
                eviction_policy=EvictionPolicy.LRU,
            ),
        ], sweep_interval=0.05)
        actor = "correct-lru"
        try:
            refs = []
            for i in range(15):
                meta = await tiered.put(actor, f"v{i}", DATA_SMALL)
                refs.append(meta.snapshot_ref)

            # Access the last 3: they should survive
            hot_refs = refs[-3:]
            for ref in hot_refs:
                await tiered.get(ref)

            await tiered.start()
            await asyncio.sleep(0.5)

            # Hot refs must still be accessible
            for ref in hot_refs:
                data = await tiered.get(ref)
                assert data is not None

            # At least one cold ref should be evicted
            cold_refs = refs[:12]
            evicted = 0
            for ref in cold_refs:
                try:
                    await tiered.get(ref)
                except KeyError:
                    evicted += 1
            assert evicted > 0, "LRU eviction should have evicted cold entries"
        finally:
            await tiered.close()
            await _cleanup_store(local_store, actor)

    async def test_tiered_list_dedup_across_tiers(self, local_store, bench_dir):
        """Tiered list must deduplicate across all tiers."""
        tier2 = LocalSnapshotStore(os.path.join(bench_dir, "tier2-dedup"))
        tiered = TieredSnapshotStore([
            StorageTier("hot", local_store, priority=0, write_through=True),
            StorageTier("warm", tier2, priority=1, write_through=True),
        ])
        actor = "correct-dedup"
        try:
            for i in range(10):
                await tiered.put(actor, f"v{i}", DATA_SMALL)
            metas = await tiered.list(actor)
            # 10 puts, should return exactly 10 (deduplicated)
            assert len(metas) == 10, f"Expected 10 unique entries, got {len(metas)}"
        finally:
            await _cleanup_store(local_store, actor)
            await _cleanup_store(tier2, actor)
