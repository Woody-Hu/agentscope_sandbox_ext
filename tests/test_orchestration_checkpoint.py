# -*- coding: utf-8 -*-
"""Tests for :class:`CheckpointBridge` — durable actor checkpoints.

The bridge is exercised with a **real** :class:`LocalSnapshotStore`
(writing real tar archives to a real tempdir) and a **real**
:class:`AgentFSWorkspace` (real host I/O, real ``snapshot()`` /
``restore()``).  No mocking.

This gives the bridge a genuine workout of:

* :meth:`CheckpointBridge.checkpoint` (DATA scope) — archives the
  workdir tree into the durable store and returns a ``snapshot_ref``.
* :meth:`CheckpointBridge.checkpoint` (FULL scope) — uses the backend's
  in-workspace ``snapshot()`` first, then mirrors into the durable
  store.  Backends without the primitive degrade to DATA.
* :meth:`CheckpointBridge.restore` — materialises a durable snapshot
  back into a worker's sandbox workdir, content-identical to the
  checkpoint.
* the round-trip property: checkpoint → mutate → restore ⇒ state rolled
  back to the checkpoint.
* durability: the checkpoint outlives the worker (freeing the worker
  does not delete the snapshot).
* error paths: missing sandbox, missing snapshot.
"""

from __future__ import annotations

import os
import shutil
import tempfile

import pytest

from agentscope_sandbox_ext import AgentFSWorkspace
from agentscope_sandbox_ext._orchestration import (
    Actor,
    ActorStatus,
    CheckpointBridge,
    CheckpointError,
    CheckpointScope,
    SandboxClass,
    Worker,
    WorkerState,
)
from agentscope_sandbox_ext._runtime import LocalSnapshotStore


# ── fixtures ───────────────────────────────────────────────────


@pytest.fixture
def snap_store(tmp_path):
    return LocalSnapshotStore(str(tmp_path / "snaps"))


@pytest.fixture
async def workspace(tmp_path):
    """A real, provisioned AgentFSWorkspace with a seeded workdir."""
    workdir = str(tmp_path / "ws")
    ws = AgentFSWorkspace(host_workdir=workdir)
    await ws._provision_backend()
    await ws.initialize()
    # Seed a realistic tree.
    _seed(workdir, {
        "data/notes.md": b"# notes\n- alpha\n",
        "skills/search/SKILL.md": b"# search\n",
        "config.json": b'{"k": "v"}\n',
    })
    return ws


def _seed(root: str, files: dict[str, bytes]) -> None:
    for rel, body in files.items():
        full = os.path.join(root, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(body)


def _actor(actor_id: str = "a1", template_id: str = "bash") -> Actor:
    return Actor(
        actor_id=actor_id,
        namespace="default",
        template_id=template_id,
        status=ActorStatus.RUNNING,
    )


def _worker_with(ws: AgentFSWorkspace, worker_id: str = "w1") -> Worker:
    return Worker(
        worker_id=worker_id,
        state=WorkerState.ACTIVE,
        sandbox_class=SandboxClass.VFS,
        node="test-node",
        sandbox=ws,
    )


# ── checkpoint (DATA scope) ────────────────────────────────────


async def test_checkpoint_data_returns_snapshot_ref(snap_store, workspace):
    bridge = CheckpointBridge(snap_store)
    ref = await bridge.checkpoint(
        _actor(), _worker_with(workspace), CheckpointScope.DATA
    )
    assert isinstance(ref, str)
    assert ref  # non-empty
    # The durable store really has the archive on disk.
    assert os.path.isfile(ref)


async def test_checkpoint_data_archives_workdir_tree(snap_store, workspace):
    """The checkpoint captures the workdir contents at checkpoint time."""
    bridge = CheckpointBridge(snap_store)
    ref = await bridge.checkpoint(
        _actor(), _worker_with(workspace), CheckpointScope.DATA
    )
    # Restore into a fresh temp dir and verify content.
    target = tempfile.mkdtemp(prefix="ckpt-verify-")
    try:
        await snap_store.restore_tree(ref, target)
        with open(os.path.join(target, "data", "notes.md"), "rb") as f:
            assert f.read() == b"# notes\n- alpha\n"
        with open(os.path.join(target, "config.json"), "rb") as f:
            assert f.read() == b'{"k": "v"}\n'
    finally:
        shutil.rmtree(target, ignore_errors=True)


async def test_checkpoint_data_is_namespaced_per_actor(snap_store, workspace):
    """Two actors get distinct snapshot_refs even for the same tag."""
    bridge = CheckpointBridge(snap_store)
    ref_a = await bridge.checkpoint(
        _actor("a1"), _worker_with(workspace), CheckpointScope.DATA
    )
    ref_b = await bridge.checkpoint(
        _actor("a2"), _worker_with(workspace), CheckpointScope.DATA
    )
    assert ref_a != ref_b


# ── checkpoint (FULL scope) ────────────────────────────────────


async def test_checkpoint_full_uses_backend_snapshot(snap_store, workspace):
    """FULL scope on agentfs takes the backend's in-workspace snapshot
    first (agentfs supports ``snapshot()``), then mirrors it into the
    durable store."""
    bridge = CheckpointBridge(snap_store)
    ref = await bridge.checkpoint(
        _actor(), _worker_with(workspace), CheckpointScope.FULL
    )
    assert isinstance(ref, str)
    assert os.path.isfile(ref)
    # The durable archive contains the seeded content.
    target = tempfile.mkdtemp(prefix="ckpt-full-")
    try:
        await snap_store.restore_tree(ref, target)
        with open(os.path.join(target, "skills", "search", "SKILL.md"), "rb") as f:
            assert f.read() == b"# search\n"
    finally:
        shutil.rmtree(target, ignore_errors=True)


async def test_checkpoint_full_degrades_to_data_when_backend_unsupported(tmp_path):
    """A backend whose ``snapshot()`` raises NotImplementedError makes
    FULL degrade to DATA (with the workdir archived directly)."""
    store = LocalSnapshotStore(str(tmp_path / "snaps"))
    bridge = CheckpointBridge(store)

    class _NoSnapWorkspace(AgentFSWorkspace):
        """agentfs variant whose in-workspace snapshot is unavailable."""

        async def snapshot(self, tag: str) -> str:  # type: ignore[override]
            raise NotImplementedError("no snapshot primitive")

    workdir = str(tmp_path / "ws")
    ws = _NoSnapWorkspace(host_workdir=workdir)
    await ws._provision_backend()
    await ws.initialize()
    _seed(workdir, {"data.txt": b"hello"})
    ref = await bridge.checkpoint(
        _actor(), _worker_with(ws), CheckpointScope.FULL
    )
    # Despite requesting FULL, we still got a durable DATA-scope archive.
    assert os.path.isfile(ref)
    target = tempfile.mkdtemp(prefix="ckpt-deg-")
    try:
        await store.restore_tree(ref, target)
        with open(os.path.join(target, "data.txt"), "rb") as f:
            assert f.read() == b"hello"
    finally:
        shutil.rmtree(target, ignore_errors=True)


async def test_checkpoint_full_degrades_when_backend_snapshot_raises(tmp_path):
    """A backend whose ``snapshot()`` raises a generic exception (not
    NotImplementedError) also degrades to DATA rather than failing."""
    store = LocalSnapshotStore(str(tmp_path / "snaps"))
    bridge = CheckpointBridge(store)

    class _BrokenSnapWorkspace(AgentFSWorkspace):
        async def snapshot(self, tag: str) -> str:  # type: ignore[override]
            raise RuntimeError("backend exploded")

    workdir = str(tmp_path / "ws")
    ws = _BrokenSnapWorkspace(host_workdir=workdir)
    await ws._provision_backend()
    await ws.initialize()
    _seed(workdir, {"data.txt": b"hello"})
    ref = await bridge.checkpoint(
        _actor(), _worker_with(ws), CheckpointScope.FULL
    )
    assert os.path.isfile(ref)


# ── restore ────────────────────────────────────────────────────


async def test_restore_replaces_workdir_with_snapshot_content(snap_store, workspace):
    """The round-trip property: checkpoint → mutate → restore ⇒ the
    workdir is rolled back to the checkpointed state."""
    bridge = CheckpointBridge(snap_store)
    ref = await bridge.checkpoint(
        _actor(), _worker_with(workspace), CheckpointScope.DATA
    )
    # Mutate the workdir after the checkpoint.
    root = workspace._host_workdir
    _seed(root, {
        "data/notes.md": b"# notes\n- MUTATED\n",
        "scratch.tmp": b"throwaway",
    })
    # Sanity: the mutation really took effect.
    with open(os.path.join(root, "data", "notes.md"), "rb") as f:
        assert b"MUTATED" in f.read()

    # Restore — the workdir should now match the checkpoint.
    await bridge.restore(_actor(), _worker_with(workspace), ref)

    with open(os.path.join(root, "data", "notes.md"), "rb") as f:
        assert f.read() == b"# notes\n- alpha\n"  # rolled back
    # The mutation-only file is gone.
    assert not os.path.exists(os.path.join(root, "scratch.tmp"))
    # The pre-existing config.json is back.
    with open(os.path.join(root, "config.json"), "rb") as f:
        assert f.read() == b'{"k": "v"}\n'


async def test_restore_into_different_worker(snap_store, tmp_path):
    """A checkpoint taken in worker A restores correctly into a
    *different* worker B — the durability + portability property the
    orchestrator's suspend/resume relies on."""
    bridge = CheckpointBridge(snap_store)

    # Worker A: seed + checkpoint.
    workdir_a = str(tmp_path / "wsA")
    ws_a = AgentFSWorkspace(host_workdir=workdir_a)
    await ws_a._provision_backend()
    await ws_a.initialize()
    _seed(workdir_a, {"state.db": b"A's state", "data/x.txt": b"xxx"})
    ref = await bridge.checkpoint(
        _actor("a1"), _worker_with(ws_a, "wa"), CheckpointScope.DATA
    )

    # Worker B: different workdir, restore A's checkpoint into it.
    workdir_b = str(tmp_path / "wsB")
    ws_b = AgentFSWorkspace(host_workdir=workdir_b)
    await ws_b._provision_backend()
    await ws_b.initialize()
    _seed(workdir_b, {"pre-existing.txt": b"b-only"})  # wiped on restore
    await bridge.restore(_actor("a1"), _worker_with(ws_b, "wb"), ref)

    # B's workdir now matches A's checkpoint.
    with open(os.path.join(workdir_b, "state.db"), "rb") as f:
        assert f.read() == b"A's state"
    with open(os.path.join(workdir_b, "data", "x.txt"), "rb") as f:
        assert f.read() == b"xxx"
    # B's pre-existing file is gone (restore is a wipe-and-replace).
    assert not os.path.exists(os.path.join(workdir_b, "pre-existing.txt"))


async def test_restore_is_repeatable(snap_store, workspace):
    """Restoring the same snapshot twice yields the same state."""
    bridge = CheckpointBridge(snap_store)
    ref = await bridge.checkpoint(
        _actor(), _worker_with(workspace), CheckpointScope.DATA
    )
    root = workspace._host_workdir
    # Mutate, restore, mutate again, restore again.
    for _ in range(2):
        _seed(root, {"junk.txt": b"junk", "data/notes.md": b"changed"})
        await bridge.restore(_actor(), _worker_with(workspace), ref)
        with open(os.path.join(root, "data", "notes.md"), "rb") as f:
            assert f.read() == b"# notes\n- alpha\n"
        assert not os.path.exists(os.path.join(root, "junk.txt"))


# ── durability ─────────────────────────────────────────────────


async def test_checkpoint_outlives_worker(snap_store, workspace):
    """The durable snapshot survives the worker's sandbox being torn
    down — the whole point of a checkpoint vs an in-workspace snapshot."""
    bridge = CheckpointBridge(snap_store)
    ref = await bridge.checkpoint(
        _actor(), _worker_with(workspace), CheckpointScope.DATA
    )
    # Tear down the worker's sandbox entirely.
    await workspace.close()
    assert workspace.is_alive is False
    # The durable archive is still on disk and restorable.
    assert os.path.isfile(ref)
    target = tempfile.mkdtemp(prefix="ckpt-dur-")
    try:
        await snap_store.restore_tree(ref, target)
        with open(os.path.join(target, "config.json"), "rb") as f:
            assert f.read() == b'{"k": "v"}\n'
    finally:
        shutil.rmtree(target, ignore_errors=True)


# ── error paths ────────────────────────────────────────────────


async def test_checkpoint_raises_when_worker_has_no_sandbox(snap_store):
    bridge = CheckpointBridge(snap_store)
    worker = Worker(
        worker_id="w1",
        state=WorkerState.ACTIVE,
        sandbox_class=SandboxClass.VFS,
        sandbox=None,  # no live sandbox
    )
    with pytest.raises(CheckpointError, match="no live sandbox"):
        await bridge.checkpoint(_actor(), worker, CheckpointScope.DATA)


async def test_restore_raises_when_worker_has_no_sandbox(snap_store):
    bridge = CheckpointBridge(snap_store)
    worker = Worker(
        worker_id="w1",
        state=WorkerState.ACTIVE,
        sandbox_class=SandboxClass.VFS,
        sandbox=None,
    )
    with pytest.raises(CheckpointError, match="no live sandbox"):
        await bridge.restore(_actor(), worker, "nonexistent-ref")


async def test_restore_raises_when_snapshot_missing(snap_store, workspace):
    bridge = CheckpointBridge(snap_store)
    with pytest.raises((KeyError, CheckpointError)):
        await bridge.restore(
            _actor(), _worker_with(workspace), "/no/such/snapshot.tar.gz"
        )


async def test_checkpoint_raises_when_workdir_missing(snap_store, tmp_path):
    """If the sandbox's host workdir cannot be located, checkpoint fails."""
    bridge = CheckpointBridge(snap_store)

    class _BareWorkspace(AgentFSWorkspace):
        """agentfs variant whose host workdir has been removed."""

    workdir = str(tmp_path / "ws")
    ws = _BareWorkspace(host_workdir=workdir)
    await ws._provision_backend()
    await ws.initialize()
    # Remove the workdir entirely so _workdir_of cannot locate it.
    shutil.rmtree(workdir, ignore_errors=True)
    ws._host_workdir = "/no/such/dir"  # point at a non-existent path
    with pytest.raises(CheckpointError, match="cannot locate workdir"):
        await bridge.checkpoint(
            _actor(), _worker_with(ws), CheckpointScope.DATA
        )
