# -*- coding: utf-8 -*-
"""Tests for :mod:`agentscope_sandbox_ext._checkpoint._manager`.

The checkpoint manager orchestrates pause (node-local), suspend
(durable upload), bake_golden (one-time per-template), and resume
(restore from any of the three kinds).  These tests use a real
:class:`LocalSnapshotStore` (real on-disk tar archives) and a real
:class:`FakeSandboxRuntime` so the snapshot / stage / restore path
moves real bytes.
"""

from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

from agentscope_sandbox_ext._actor._types import (
    KIND_GOLDEN,
    KIND_LAST,
    KIND_PAUSE,
    SCOPE_DATA,
    SCOPE_FULL,
    ActorRef,
    TemplateRef,
)
from agentscope_sandbox_ext._checkpoint._manager import (
    CheckpointManager,
    _safe_component,
    _tag,
)
from agentscope_sandbox_ext._checkpoint._types import (
    CheckpointConfig,
    PauseSnapshotNotLocal,
)
from agentscope_sandbox_ext._runtime.local_snapshot_store import (
    LocalSnapshotStore,
)
from agentscope_sandbox_ext._worker._types import Worker
from tests._helpers.fake_runtime import FakeSandboxRuntime


# ── fixtures ────────────────────────────────────────────────────


@pytest.fixture
def durable_store(tmp_path):
    return LocalSnapshotStore(str(tmp_path / "durable"))


@pytest.fixture
def checkpoint(durable_store):
    return CheckpointManager(
        CheckpointConfig(durable_store=durable_store),
    )


@pytest.fixture
def actor():
    return ActorRef(namespace="ns", name="a1")


@pytest.fixture
def template_ref():
    return TemplateRef(name="t", version=1)


async def _make_worker(*, node: str = "n1", cls: str = "vfs") -> Worker:
    rt = FakeSandboxRuntime(sandbox_class_value=cls, node_value=node)
    await rt.provision()
    return Worker(worker_id=rt.worker_id, runtime=rt)


# ── _safe_component / _tag ──────────────────────────────────────


def test_safe_component_strips_separators():
    assert "/" not in _safe_component("a/b/c")
    assert _safe_component("a/b") == "a-b"


def test_safe_component_preserves_alnum_and_dash():
    assert _safe_component("agent-1") == "agent-1"


def test_safe_component_falls_back_for_empty():
    assert _safe_component("///") == "x"


def test_tag_combines_namespace_name_suffix():
    ref = ActorRef(namespace="ns", name="a")
    assert _tag(ref, "pause") == "ns_a_pause"


# ── CheckpointConfig validation ─────────────────────────────────


def test_checkpoint_config_rejects_unknown_pause_scope(durable_store):
    with pytest.raises(ValueError, match="on_pause"):
        CheckpointConfig(durable_store=durable_store, on_pause="bogus")


def test_checkpoint_config_rejects_unknown_suspend_scope(durable_store):
    with pytest.raises(ValueError, match="on_suspend"):
        CheckpointConfig(durable_store=durable_store, on_suspend="bogus")


def test_checkpoint_config_enforces_subset_invariant(durable_store):
    """on_suspend=full is NOT a subset of on_pause=data."""
    with pytest.raises(ValueError, match="subset"):
        CheckpointConfig(
            durable_store=durable_store,
            on_pause=SCOPE_DATA,
            on_suspend=SCOPE_FULL,
        )


def test_checkpoint_config_allows_full_full(durable_store):
    cfg = CheckpointConfig(
        durable_store=durable_store,
        on_pause=SCOPE_FULL,
        on_suspend=SCOPE_FULL,
    )
    assert cfg.on_pause == SCOPE_FULL


def test_checkpoint_config_allows_data_data(durable_store):
    cfg = CheckpointConfig(
        durable_store=durable_store,
        on_pause=SCOPE_DATA,
        on_suspend=SCOPE_DATA,
    )
    assert cfg.on_pause == SCOPE_DATA


# ── pause ───────────────────────────────────────────────────────


async def test_pause_captures_node_locally(checkpoint, actor):
    w = await _make_worker(node="n1")
    try:
        snap = await checkpoint.pause(actor, w)
        assert snap.kind == KIND_PAUSE
        assert snap.node == "n1"
        assert snap.scope == SCOPE_FULL
        # Pause should NOT upload to durable store.
        metas = await checkpoint._config.durable_store.list(actor.key)
        assert metas == []
        # Worker runtime really snapshotted.
        assert any(c[0] == "snapshot" for c in w.runtime.calls)
    finally:
        await w.runtime.close()


async def test_pause_uses_configured_scope(durable_store, actor):
    cp = CheckpointManager(
        CheckpointConfig(
            durable_store=durable_store,
            on_pause=SCOPE_DATA,
            on_suspend=SCOPE_DATA,
        ),
    )
    w = await _make_worker()
    try:
        snap = await cp.pause(actor, w)
        assert snap.scope == SCOPE_DATA
    finally:
        await w.runtime.close()


# ── suspend ─────────────────────────────────────────────────────


async def test_suspend_uploads_to_durable_store(checkpoint, actor):
    w = await _make_worker(node="n1")
    try:
        w.runtime.write_file("data.txt", b"payload")
        snap = await checkpoint.suspend(actor, w)
        assert snap.kind == KIND_LAST
        assert snap.node is None  # location-independent
        # Durable store now has the snapshot.
        metas = await checkpoint._config.durable_store.list(actor.key)
        assert len(metas) >= 1
        # The snapshot_id should be resolvable by the durable store.
        data = await checkpoint._config.durable_store.get(snap.snapshot_id)
        assert isinstance(data, (bytes, bytearray))
    finally:
        await w.runtime.close()


# ── bake_golden ─────────────────────────────────────────────────


async def test_bake_golden_uploads_under_template_id(checkpoint, template_ref):
    w = await _make_worker()
    try:
        w.runtime.write_file("seed.txt", b"golden-state")
        snap = await checkpoint.bake_golden(template_ref, w)
        assert snap.kind == KIND_GOLDEN
        assert snap.scope == SCOPE_FULL
        assert snap.node is None  # golden is durable / location-independent
        # Durable store has it under template:<name>@<version>.
        metas = await checkpoint._config.durable_store.list(
            f"template:{template_ref.key}",
        )
        assert len(metas) >= 1
    finally:
        await w.runtime.close()


# ── resume ──────────────────────────────────────────────────────


async def test_resume_from_pause_same_node_restores_locally(checkpoint, actor):
    w = await _make_worker(node="n1")
    try:
        w.runtime.write_file("state.txt", b"v1")
        snap = await checkpoint.pause(actor, w)
        # Mutate then restore.
        w.runtime.write_file("state.txt", b"v2-mutated")
        await checkpoint.resume(actor, w, snap)
        assert w.runtime.read_file("state.txt") == b"v1"
        # No stage call (local-only path).
        assert not any(c[0] == "stage" for c in w.runtime.calls)
    finally:
        await w.runtime.close()


async def test_resume_from_pause_different_node_raises(checkpoint, actor):
    w1 = await _make_worker(node="n1")
    w2 = await _make_worker(node="n2")
    try:
        snap = await checkpoint.pause(actor, w1)
        with pytest.raises(PauseSnapshotNotLocal):
            await checkpoint.resume(actor, w2, snap)
    finally:
        await w1.runtime.close()
        await w2.runtime.close()


async def test_resume_from_last_downloads_and_stages(checkpoint, actor):
    """A ``last`` snapshot is durable: resume downloads it, stages it
    into the worker's snapshot slot, and restores it."""
    w1 = await _make_worker(node="n1")
    w2 = await _make_worker(node="n2")
    try:
        w1.runtime.write_file("data.bin", b"suspend-state")
        snap = await checkpoint.suspend(actor, w1)
        # Restore on a *different* worker — proves location independence.
        await checkpoint.resume(actor, w2, snap)
        assert w2.runtime.read_file("data.bin") == b"suspend-state"
        # stage + restore both called.
        ops = [c[0] for c in w2.runtime.calls]
        assert "stage" in ops
        assert "restore" in ops
    finally:
        await w1.runtime.close()
        await w2.runtime.close()


async def test_resume_from_golden_downloads_and_stages(checkpoint, actor, template_ref):
    w1 = await _make_worker(node="n1")
    w2 = await _make_worker(node="n2")
    try:
        w1.runtime.write_file("golden.txt", b"golden-seed")
        golden = await checkpoint.bake_golden(template_ref, w1)
        await checkpoint.resume(actor, w2, golden)
        assert w2.runtime.read_file("golden.txt") == b"golden-seed"
    finally:
        await w1.runtime.close()
        await w2.runtime.close()


# ── singleflight integration ────────────────────────────────────


async def test_concurrent_pause_same_actor_dedups_to_one_snapshot(checkpoint, actor):
    """Two concurrent pause() calls on the same actor must collapse
    into a single snapshot() invocation on the runtime.

    A small ``snapshot_delay`` forces a real suspension point inside
    the factory so the second caller arrives while the first is still
    in flight (otherwise singleflight has nothing to dedup).
    """
    rt = FakeSandboxRuntime(
        sandbox_class_value="vfs", node_value="n1", snapshot_delay=0.05,
    )
    await rt.provision()
    w = Worker(worker_id=rt.worker_id, runtime=rt)
    try:
        snaps = await asyncio.gather(
            checkpoint.pause(actor, w),
            checkpoint.pause(actor, w),
        )
        # Same snapshot_id returned to both callers.
        assert snaps[0].snapshot_id == snaps[1].snapshot_id
        # Exactly one snapshot call on the runtime.
        snapshot_calls = [c for c in w.runtime.calls if c[0] == "snapshot"]
        assert len(snapshot_calls) == 1
    finally:
        await w.runtime.close()


async def test_concurrent_suspend_same_actor_dedups_to_one_upload(checkpoint, actor):
    """Same dedup guarantee for suspend (snapshot + durable upload)."""
    rt = FakeSandboxRuntime(
        sandbox_class_value="vfs", node_value="n1", snapshot_delay=0.05,
    )
    await rt.provision()
    w = Worker(worker_id=rt.worker_id, runtime=rt)
    try:
        snaps = await asyncio.gather(
            checkpoint.suspend(actor, w),
            checkpoint.suspend(actor, w),
        )
        assert snaps[0].snapshot_id == snaps[1].snapshot_id
        metas = await checkpoint._config.durable_store.list(actor.key)
        assert len(metas) == 1
    finally:
        await w.runtime.close()


# ── list_snapshots ──────────────────────────────────────────────


async def test_list_snapshots_returns_durable_entries(checkpoint, actor):
    w = await _make_worker()
    try:
        await checkpoint.suspend(actor, w)
        snaps = await checkpoint.list_snapshots(actor)
        assert len(snaps) == 1
        assert snaps[0].kind == KIND_LAST
    finally:
        await w.runtime.close()


async def test_list_snapshots_empty_for_unknown_actor(checkpoint):
    snaps = await checkpoint.list_snapshots(ActorRef(namespace="ns", name="ghost"))
    assert snaps == []
