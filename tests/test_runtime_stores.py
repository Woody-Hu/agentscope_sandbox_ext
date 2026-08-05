# -*- coding: utf-8 -*-
"""Real (no-mock) tests for PostgresSnapshotStore, MinioSnapshotStore,
LocalSnapshotStore, and TieredSnapshotStore.

These tests require:
- PostgreSQL on localhost:5432 (db: testdb, user: postgres, pass: test123)
- MinIO on localhost:19000 (access: minioadmin, secret: minioadmin)
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

import pytest

from agentscope_sandbox_ext._runtime import (
    EvictionPolicy,
    LocalSnapshotStore,
    MinioSnapshotStore,
    PostgresSnapshotStore,
    SnapshotMeta,
    SnapshotStore,
    StorageTier,
    TieredSnapshotStore,
)


# ── fixtures ────────────────────────────────────────────────────


@pytest.fixture
def pg_store():
    store = PostgresSnapshotStore(
        "postgresql://postgres:test123@localhost:5432/testdb",
        table_name="snapshots_test",
    )
    return store


@pytest.fixture
async def clean_pg(pg_store):
    """Ensure PG table is clean before test."""
    import asyncpg
    pool = await pg_store._ensure_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {pg_store._table_name}")
    return pg_store


@pytest.fixture
def minio_store():
    store = MinioSnapshotStore(
        "localhost:19000", "minioadmin", "minioadmin",
        bucket="snapshots-test",
    )
    # Clean bucket before test
    client = store._ensure_client()
    for obj in list(client.list_objects("snapshots-test", recursive=True)):
        try:
            client.remove_object("snapshots-test", obj.object_name)
        except Exception:
            pass
    return store


@pytest.fixture
def local_store(tmp_path):
    return LocalSnapshotStore(str(tmp_path / "snapshots"))


# ── helpers ─────────────────────────────────────────────────────


async def _cleanup(store: SnapshotStore, actor_id: str):
    try:
        for m in await store.list(actor_id):
            try:
                await store.delete(m.snapshot_ref)
            except Exception:
                pass
    except Exception:
        pass


def _make_data(content: bytes = b"hello snapshot world") -> bytes:
    return content


# ── PostgresSnapshotStore tests ─────────────────────────────────


async def test_pg_put_and_get(pg_store):
    """Write a snapshot to Postgres and read it back."""
    actor_id = "pg-test-actor-1"
    try:
        meta = await pg_store.put(actor_id, "v1", _make_data(), compression="none")
        assert meta.actor_id == actor_id
        assert meta.tag == "v1"
        assert meta.size_bytes > 0
        assert meta.checksum is not None

        data = await pg_store.get(meta.snapshot_ref)
        assert data == _make_data()
    finally:
        await _cleanup(pg_store, actor_id)


async def test_pg_get_missing_raises_keyerror(pg_store):
    with pytest.raises(KeyError):
        await pg_store.get("pg:nonexistent:ref")


async def test_pg_delete(pg_store):
    actor_id = "pg-test-actor-2"
    try:
        meta = await pg_store.put(actor_id, "v1", _make_data())
        await pg_store.delete(meta.snapshot_ref)
        with pytest.raises(KeyError):
            await pg_store.get(meta.snapshot_ref)
    finally:
        await _cleanup(pg_store, actor_id)


async def test_pg_delete_missing_raises_keyerror(pg_store):
    with pytest.raises(KeyError):
        await pg_store.delete("pg:nonexistent:ref")


async def test_pg_list(pg_store):
    actor_id = "pg-test-actor-3"
    try:
        await pg_store.put(actor_id, "v1", _make_data(b"v1"))
        await asyncio.sleep(0.01)
        await pg_store.put(actor_id, "v2", _make_data(b"v2-data"))

        metas = await pg_store.list(actor_id)
        assert len(metas) == 2
        # Most recent first
        assert metas[0].tag == "v2"

        # Filter by tag
        v1_metas = await pg_store.list(actor_id, tag="v1")
        assert len(v1_metas) == 1
        assert v1_metas[0].tag == "v1"
    finally:
        await _cleanup(pg_store, actor_id)


async def test_pg_list_empty_actor(pg_store):
    metas = await pg_store.list("nonexistent-actor")
    assert metas == []


async def test_pg_copy(pg_store):
    actor_id = "pg-test-actor-4"
    try:
        meta = await pg_store.put(actor_id, "v1", _make_data(b"copy-me"))
        copied = await pg_store.copy(meta.snapshot_ref, "pg-target", "cloned")
        assert copied.actor_id == "pg-target"
        assert copied.tag == "cloned"

        data = await pg_store.get(copied.snapshot_ref)
        assert data == b"copy-me"
    finally:
        await _cleanup(pg_store, actor_id)
        await _cleanup(pg_store, "pg-target")


async def test_pg_metrics(pg_store):
    actor_id = "pg-test-metrics"
    try:
        await pg_store.put(actor_id, "v1", _make_data(b"x" * 100))
        m = await pg_store.metrics()
        assert m["type"] == "PostgresSnapshotStore"
        assert m["total_objects"] >= 1
        assert m["total_size_bytes"] >= 100
    finally:
        await _cleanup(pg_store, actor_id)


# ── MinioSnapshotStore tests ────────────────────────────────────


async def test_minio_put_and_get(minio_store):
    actor_id = "minio-test-actor-1"
    try:
        meta = await minio_store.put(actor_id, "v1", _make_data(), compression="none")
        assert meta.actor_id == actor_id
        assert meta.tag == "v1"
        assert meta.size_bytes > 0

        data = await minio_store.get(meta.snapshot_ref)
        assert data == _make_data()
    finally:
        await _cleanup(minio_store, actor_id)


async def test_minio_get_missing_raises_keyerror(minio_store):
    with pytest.raises(KeyError):
        await minio_store.get("minio:nonexistent/ref")


async def test_minio_delete(minio_store):
    actor_id = "minio-test-actor-2"
    try:
        meta = await minio_store.put(actor_id, "v1", _make_data())
        await minio_store.delete(meta.snapshot_ref)
        with pytest.raises(KeyError):
            await minio_store.get(meta.snapshot_ref)
    finally:
        await _cleanup(minio_store, actor_id)


async def test_minio_list(minio_store):
    actor_id = "minio-test-actor-3"
    try:
        await minio_store.put(actor_id, "v1", _make_data(b"v1"))
        await asyncio.sleep(0.02)
        await minio_store.put(actor_id, "v2", _make_data(b"v2-data"))

        metas = await minio_store.list(actor_id)
        assert len(metas) == 2

        v1_metas = await minio_store.list(actor_id, tag="v1")
        assert len(v1_metas) == 1
    finally:
        await _cleanup(minio_store, actor_id)


async def test_minio_copy(minio_store):
    actor_id = "minio-test-actor-4"
    try:
        meta = await minio_store.put(actor_id, "v1", _make_data(b"copy-me"))
        copied = await minio_store.copy(meta.snapshot_ref, "minio-target", "cloned")
        assert copied.actor_id == "minio-target"
        data = await minio_store.get(copied.snapshot_ref)
        assert data == b"copy-me"
    finally:
        await _cleanup(minio_store, actor_id)
        await _cleanup(minio_store, "minio-target")


# ── LocalSnapshotStore tests ────────────────────────────────────


async def test_local_put_and_get(local_store):
    actor_id = "local-1"
    meta = await local_store.put(actor_id, "v1", _make_data())
    data = await local_store.get(meta.snapshot_ref)
    assert data == _make_data()


async def test_local_put_tree_and_restore(local_store, tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "file.txt").write_text("hello world")

    meta = await local_store.put_tree("local-2", "tree-v1", str(src_dir))

    restore_dir = tmp_path / "restored"
    restore_dir.mkdir()
    await local_store.restore_tree(meta.snapshot_ref, str(restore_dir))

    assert (restore_dir / "file.txt").read_text() == "hello world"


async def test_local_list_and_delete(local_store):
    await local_store.put("local-3", "v1", b"data1")
    await local_store.put("local-3", "v2", b"data2")

    metas = await local_store.list("local-3")
    assert len(metas) == 2

    for m in metas:
        await local_store.delete(m.snapshot_ref)

    assert await local_store.list("local-3") == []


# ── TieredSnapshotStore tests ───────────────────────────────────


async def test_tiered_single_tier(local_store):
    """Single-tier operation should work identically."""
    tiered = TieredSnapshotStore([
        StorageTier("local", local_store, priority=0),
    ])
    meta = await tiered.put("tiered-1", "v1", b"data")
    data = await tiered.get(meta.snapshot_ref)
    assert data == b"data"


async def test_tiered_two_tiers_write_through(local_store, tmp_path):
    """Two tiers, both write-through — data in both."""
    tier2 = LocalSnapshotStore(str(tmp_path / "tier2"))
    tiered = TieredSnapshotStore([
        StorageTier("hot", local_store, priority=0, write_through=True),
        StorageTier("warm", tier2, priority=1, write_through=True),
    ])

    meta = await tiered.put("tiered-2", "v1", b"two-tier-data")

    # Both tiers should have the data
    data1 = await local_store.get(meta.snapshot_ref)
    data2 = await tier2.get(meta.snapshot_ref)
    assert data1 == b"two-tier-data"
    assert data2 == b"two-tier-data"


async def test_tiered_read_from_lower_tier(local_store, tmp_path):
    """When tier 0 doesn't have the data, tier 1 should supply it."""
    tier2 = LocalSnapshotStore(str(tmp_path / "tier2"))
    tiered = TieredSnapshotStore([
        StorageTier("hot", local_store, priority=0, write_through=False),
        StorageTier("warm", tier2, priority=1, write_through=True),
    ])

    meta = await tiered.put("tiered-3", "v1", b"warm-data")

    # Delete from tier 0 (hot) so it's only in tier 1 (warm)
    try:
        await local_store.delete(meta.snapshot_ref)
    except KeyError:
        pass

    # Should still be readable from tier 1
    data = await tiered.get(meta.snapshot_ref)
    assert data == b"warm-data"


async def test_tiered_delete_propagates(local_store, tmp_path):
    """Delete should remove from all tiers."""
    tier2 = LocalSnapshotStore(str(tmp_path / "tier2"))
    tiered = TieredSnapshotStore([
        StorageTier("hot", local_store, priority=0, write_through=True),
        StorageTier("warm", tier2, priority=1, write_through=True),
    ])

    meta = await tiered.put("tiered-4", "v1", b"delete-me")
    await tiered.delete(meta.snapshot_ref)

    with pytest.raises(KeyError):
        await tiered.get(meta.snapshot_ref)


async def test_tiered_list_dedup(local_store, tmp_path):
    """List should deduplicate across tiers."""
    tier2 = LocalSnapshotStore(str(tmp_path / "tier2"))
    tiered = TieredSnapshotStore([
        StorageTier("hot", local_store, priority=0, write_through=True),
        StorageTier("warm", tier2, priority=1, write_through=True),
    ])

    await tiered.put("tiered-5", "v1", b"data")
    metas = await tiered.list("tiered-5")
    # Should be deduplicated — only 1 entry, not 2
    assert len(metas) == 1


async def test_tiered_copy(local_store, tmp_path):
    tiered = TieredSnapshotStore([
        StorageTier("local", local_store, priority=0, write_through=True),
    ])
    meta = await tiered.put("src", "v1", b"copy-me")
    copied = await tiered.copy(meta.snapshot_ref, "dst", "cloned")
    data = await tiered.get(copied.snapshot_ref)
    assert data == b"copy-me"


async def test_tiered_metrics(local_store):
    tiered = TieredSnapshotStore([
        StorageTier("local", local_store, priority=0),
    ])
    await tiered.put("m", "v1", b"x")
    m = await tiered.metrics()
    assert m["type"] == "TieredSnapshotStore"
    assert m["num_tiers"] == 1
    assert "local" in m["tiers"]


# ── eviction tests ──────────────────────────────────────────────


async def test_tiered_lru_eviction(local_store, tmp_path):
    """LRU eviction removes oldest-accessed entries when over capacity."""
    tiered = TieredSnapshotStore([
        StorageTier(
            "hot", local_store, priority=0,
            max_objects=2,
            eviction_policy=EvictionPolicy.LRU,
        ),
    ], sweep_interval=0.1)
    await tiered.start()

    try:
        m1 = await tiered.put("evict-1", "v1", b"a")
        await asyncio.sleep(0.02)
        m2 = await tiered.put("evict-1", "v2", b"b")
        await asyncio.sleep(0.02)
        m3 = await tiered.put("evict-1", "v3", b"c")

        # Access m1 to make it recently used
        await tiered.get(m1.snapshot_ref)

        # Trigger eviction sweep
        await asyncio.sleep(0.3)

        # m2 should be evicted (oldest, not recently accessed)
        with pytest.raises(KeyError):
            await tiered.get(m2.snapshot_ref)

        # m1 and m3 should survive
        assert await tiered.get(m1.snapshot_ref) == b"a"
        assert await tiered.get(m3.snapshot_ref) == b"c"
    finally:
        await tiered.close()


async def test_tiered_ttl_eviction(local_store):
    """TTL eviction removes entries older than ttl_seconds."""
    tiered = TieredSnapshotStore([
        StorageTier(
            "hot", local_store, priority=0,
            eviction_policy=EvictionPolicy.TTL,
            ttl_seconds=0.2,
        ),
    ], sweep_interval=0.1)
    await tiered.start()

    try:
        meta = await tiered.put("ttl-1", "v1", b"ttl-data")

        # Access it to record access time
        await tiered.get(meta.snapshot_ref)

        # Wait for TTL to expire + sweep
        await asyncio.sleep(0.5)

        # Should be evicted
        with pytest.raises(KeyError):
            await tiered.get(meta.snapshot_ref)
    finally:
        await tiered.close()


# ── cross-store integration test ────────────────────────────────


async def test_cross_store_local_minio_pg(local_store, minio_store, pg_store):
    """Three-tier: local (hot) -> MinIO (warm) -> Postgres (cold).

    Write to all, read from local first, verify fallback to MinIO
    and Postgres when local is cleared.
    """
    tiered = TieredSnapshotStore([
        StorageTier("hot", local_store, priority=0, write_through=True),
        StorageTier("warm", minio_store, priority=1, write_through=True),
        StorageTier("cold", pg_store, priority=2, write_through=True),
    ])

    actor_id = "cross-test-1"
    try:
        meta = await tiered.put(actor_id, "v1", b"cross-tier-data")

        # Read from tier 0 (local) first
        data = await tiered.get(meta.snapshot_ref)
        assert data == b"cross-tier-data"

        # Delete from local (tier 0), should still read from tier 1 (MinIO)
        await local_store.delete(meta.snapshot_ref)
        data = await tiered.get(meta.snapshot_ref)
        assert data == b"cross-tier-data"

        # Delete from MinIO (tier 1), should still read from tier 2 (PG)
        await minio_store.delete(meta.snapshot_ref)
        data = await tiered.get(meta.snapshot_ref)
        assert data == b"cross-tier-data"
    finally:
        await _cleanup(tiered, actor_id)
        await _cleanup(local_store, actor_id)
        await _cleanup(minio_store, actor_id)
        await _cleanup(pg_store, actor_id)
