# -*- coding: utf-8 -*-
"""Real (no-mock) tests for the VFS snapshot / restore feature.

These tests exercise the full snapshot → mutate → restore path with
real host I/O (``shutil.copytree``), real subprocesses (``diff -r`` to
prove byte-identical state, ``stat`` to prove mode preservation, real
skill-tree seeding via ``write_file``) and real filesystem isolation
(each workspace gets its own tempdir; snapshots live in a sibling dir).

Nothing is mocked.  ``diff -r`` is the correctness oracle — if the
restored tree ever disagrees with the snapshot, the test fails with
the actual diff.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

import pytest

from agentscope_sandbox_ext import (
    AgentFSWorkspace,
    SandboxedWorkspaceExtBase,
    VFSWorkspaceBase,
)


# ── fixtures ────────────────────────────────────────────────────


@pytest.fixture
def workdir():
    d = tempfile.mkdtemp(prefix="as_vfs_snap_test_")
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def snapshots_root(tmp_path: Path) -> str:
    d = tmp_path / "snaps"
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


# ── helpers ─────────────────────────────────────────────────────


def _tree_diff(a: str, b: str) -> str:
    """Return ``diff -r`` output between two dirs (empty if identical).

    Uses a real subprocess — never mock — so the test fails with the
    actual on-disk difference if restore is lossy.
    """
    proc = subprocess.run(
        ["diff", "-r", a, b],
        capture_output=True,
        text=True,
    )
    # diff -r exits 0 if identical, 1 if different, 2 on error.
    if proc.returncode == 2:
        raise RuntimeError(f"diff -r failed: {proc.stderr}")
    return proc.stdout


def _populate_tree(root: str, n_files: int = 10) -> None:
    """Write a deterministic nested tree under *root* for diff tests."""
    os.makedirs(os.path.join(root, "empty_dir"), exist_ok=True)
    os.makedirs(os.path.join(root, "nested", "deep"), exist_ok=True)
    for i in range(n_files):
        path = os.path.join(root, "nested", f"f{i}.txt")
        with open(path, "w") as f:
            f.write(f"content {i}\n")
    # An executable file to verify mode preservation.
    exe = os.path.join(root, "bin", "hello.sh")
    os.makedirs(os.path.dirname(exe), exist_ok=True)
    with open(exe, "w") as f:
        f.write("#!/bin/sh\necho hi\n")
    os.chmod(exe, 0o755)


# ── API contract: NotImplementedError on the base ───────────────


async def test_snapshot_not_implemented_on_ext_base_default():
    """A SandboxedWorkspaceExtBase subclass that does not override
    snapshot/restore must raise NotImplementedError, so the API is
    uniform across every backend (mirrors verify_runtime_available).
    """

    class _BareBackend(SandboxedWorkspaceExtBase):
        sandbox_kind = "bare"

        @classmethod
        async def verify_runtime_available(cls) -> None:
            return None

        # No snapshot/restore override → inherits NotImplementedError.

    bare = _BareBackend()
    with pytest.raises(NotImplementedError, match="does not support snapshots"):
        await bare.snapshot("v1")
    with pytest.raises(NotImplementedError, match="does not support snapshots"):
        await bare.restore("v1")


def test_vfs_workspace_overrides_snapshot_and_restore():
    """VFSWorkspaceBase ships a real impl; AgentFSWorkspace inherits it."""
    assert VFSWorkspaceBase.snapshot is not SandboxedWorkspaceExtBase.snapshot
    assert VFSWorkspaceBase.restore is not SandboxedWorkspaceExtBase.restore
    assert AgentFSWorkspace.snapshot is VFSWorkspaceBase.snapshot
    assert AgentFSWorkspace.restore is VFSWorkspaceBase.restore


# ── lifecycle guards ────────────────────────────────────────────


async def test_snapshot_before_provision_raises(workdir, snapshots_root):
    ws = AgentFSWorkspace(
        host_workdir=workdir,
        snapshots_root=snapshots_root,
    )
    with pytest.raises(RuntimeError, match="not provisioned"):
        await ws.snapshot("v1")
    with pytest.raises(RuntimeError, match="not provisioned"):
        await ws.restore("v1")


async def test_snapshot_after_teardown_raises(workdir, snapshots_root):
    ws = AgentFSWorkspace(
        host_workdir=workdir,
        snapshots_root=snapshots_root,
    )
    await ws._provision_backend()
    await ws._teardown_backend()
    with pytest.raises(RuntimeError, match="not provisioned"):
        await ws.snapshot("v1")


# ── tag validation ──────────────────────────────────────────────


async def test_snapshot_rejects_empty_tag(workdir, snapshots_root):
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        with pytest.raises(ValueError, match="Invalid snapshot tag"):
            await ws.snapshot("")
    finally:
        await ws._teardown_backend()


async def test_snapshot_rejects_path_traversal_tag(workdir, snapshots_root):
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        for bad in ("..", "a/b", "../escape", "."):
            with pytest.raises(ValueError, match="Invalid snapshot tag"):
                await ws.snapshot(bad)
    finally:
        await ws._teardown_backend()


async def test_restore_missing_tag_raises_keyerror(workdir, snapshots_root):
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        with pytest.raises(KeyError, match="No snapshot named"):
            await ws.restore("never-snapped")
    finally:
        await ws._teardown_backend()


# ── correctness: round-trip ─────────────────────────────────────


async def test_snapshot_then_mutate_then_restore_is_byte_identical(
    workdir, snapshots_root,
):
    """The correctness oracle: after restore, the live tree must be
    byte-identical to the snapshot, verified by real ``diff -r``.
    """
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        _populate_tree(workdir, n_files=20)
        snap_path = await ws.snapshot("v1")
        assert os.path.isdir(snap_path)
        assert snap_path == os.path.join(snapshots_root, "v1")

        # Mutate the live tree via both write_file and exec_shell —
        # exercising both translation hooks.
        b = ws.get_backend()
        await b.write_file("nested/NEW.txt", b"new content")
        await b.exec_shell(["sh", "-c", "echo appended >> nested/f0.txt"])
        await b.exec_shell(["sh", "-c", "rm nested/f1.txt"])
        await b.exec_shell(
            ["sh", "-c", "mkdir -p brand_new_dir && touch brand_new_dir/x"],
        )

        # Live tree now differs from snapshot — sanity check the diff
        # is non-empty (proves the mutations actually landed).
        assert _tree_diff(snap_path, workdir) != ""

        # Restore and verify byte-identical.
        await ws.restore("v1")
        diff = _tree_diff(snap_path, workdir)
        assert diff == "", f"restore not byte-identical to snapshot:\n{diff}"
    finally:
        await ws._teardown_backend()


async def test_snapshot_is_isolated_from_live_mutations(
    workdir, snapshots_root,
):
    """Mutating the live tree after snapshot must not affect the
    snapshot — proves the snapshot is a deep copy, not a hardlink tree.
    """
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        _populate_tree(workdir, n_files=5)
        snap_path = await ws.snapshot("iso")

        # Mutate live via exec_shell (in-place append — the dangerous
        # case for a hardlink impl).
        b = ws.get_backend()
        await b.exec_shell(["sh", "-c", "echo MUTANT >> nested/f0.txt"])
        await b.exec_shell(["sh", "-c", "rm nested/f1.txt"])

        # Snapshot must be unchanged.
        with open(os.path.join(snap_path, "nested", "f0.txt")) as f:
            assert f.read() == "content 0\n", (
                "snapshot leaked in-place append — not a deep copy"
            )
        assert os.path.isfile(
            os.path.join(snap_path, "nested", "f1.txt"),
        ), "snapshot leaked file deletion — not a deep copy"
    finally:
        await ws._teardown_backend()


# ── tag namespacing ─────────────────────────────────────────────


async def test_two_tags_are_independent(workdir, snapshots_root):
    """snapshot('v1') and snapshot('v2') are independent; restoring
    one does not affect the other."""
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        b = ws.get_backend()
        await b.write_file("state.txt", b"v1-state")
        await ws.snapshot("v1")
        await b.write_file("state.txt", b"v2-state")
        await ws.snapshot("v2")

        # Mutate past v2.
        await b.write_file("state.txt", b"mutated")

        # Restore v1 → state.txt == "v1-state".
        await ws.restore("v1")
        assert await b.read_file("state.txt") == b"v1-state"

        # Restore v2 → state.txt == "v2-state".  Restoring v1 must not
        # have damaged the v2 snapshot.
        await ws.restore("v2")
        assert await b.read_file("state.txt") == b"v2-state"
    finally:
        await ws._teardown_backend()


async def test_snapshot_same_tag_replaces_atomically(workdir, snapshots_root):
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        b = ws.get_backend()
        await b.write_file("a.txt", b"first")
        await ws.snapshot("tag")
        await b.write_file("a.txt", b"second")
        await ws.snapshot("tag")  # overwrite
        await b.write_file("a.txt", b"third")

        await ws.restore("tag")
        assert await b.read_file("a.txt") == b"second"
    finally:
        await ws._teardown_backend()


# ── snapshot preserves metadata ─────────────────────────────────


async def test_snapshot_preserves_modes_and_empty_dirs(
    workdir, snapshots_root,
):
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        _populate_tree(workdir, n_files=3)
        snap_path = await ws.snapshot("modes")

        # Empty dir preserved.
        assert os.path.isdir(os.path.join(snap_path, "empty_dir"))

        # Executable bit preserved.
        exe_mode = stat.S_IMODE(
            os.stat(os.path.join(snap_path, "bin", "hello.sh")).st_mode,
        )
        assert exe_mode == 0o755, f"exec bit lost: {oct(exe_mode)}"
    finally:
        await ws._teardown_backend()


async def test_restore_preserves_modes(workdir, snapshots_root):
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        _populate_tree(workdir, n_files=3)
        await ws.snapshot("modes")

        # Mutate the live exe to lose its exec bit, then restore.
        os.chmod(os.path.join(workdir, "bin", "hello.sh"), 0o644)
        await ws.restore("modes")

        exe_mode = stat.S_IMODE(
            os.stat(os.path.join(workdir, "bin", "hello.sh")).st_mode,
        )
        assert exe_mode == 0o755, f"exec bit not restored: {oct(exe_mode)}"
    finally:
        await ws._teardown_backend()


# ── snapshot survives close (durability) ────────────────────────


async def test_snapshot_outlives_close_and_can_restore_to_new_workspace(
    workdir, snapshots_root,
):
    """A snapshot must survive ``close()`` and be restorable into a
    fresh workspace bound to the same host_workdir — this is the
    E2B/Firecracker 'durable artifact' contract.
    """
    ws1 = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws1._provision_backend()
    b1 = ws1.get_backend()
    await b1.write_file("durable.txt", b"survives close")
    await b1.exec_shell(
        ["sh", "-c", "mkdir -p deep/nested && echo x > deep/nested/y"],
    )
    await ws1.snapshot("durable")
    await ws1._teardown_backend()

    # Wipe the workdir entirely — simulate a fresh provision.
    shutil.rmtree(workdir)
    os.makedirs(workdir)

    # New workspace on the same workdir + snapshots_root.
    ws2 = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws2._provision_backend()
    await ws2.restore("durable")
    try:
        b2 = ws2.get_backend()
        assert await b2.read_file("durable.txt") == b"survives close"
        assert await b2.read_file("deep/nested/y") == b"x\n"
    finally:
        await ws2._teardown_backend()


# ── snapshot after skill-style seeding ──────────────────────────


async def test_snapshot_preserves_seeded_skill_tree(tmp_path):
    """A skill is just a subdirectory under ``skills/`` (the agentscope
    convention).  snapshot must capture it; restore must bring it back
    without re-seeding — the dominant agent-workflow win.

    We seed the skill tree directly via ``write_file`` rather than
    ``skill_paths`` so the test exercises only the snapshot/restore
    path (``add_skill``'s tar round-trip is a separate concern and is
    already covered by the byte-identical ``diff -r`` test above).
    """
    workdir = tmp_path / "ws"
    workdir.mkdir()
    snaps = tmp_path / "snaps"

    ws = AgentFSWorkspace(host_workdir=str(workdir), snapshots_root=str(snaps))
    await ws._provision_backend()
    try:
        b = ws.get_backend()
        # Seed a skill tree the way _setup_skills would leave it.
        await b.write_file("skills/my_skill/SKILL.md", b"# my skill\n")
        await b.write_file(
            "skills/my_skill/helper.py", b"def run(): return 42\n",
        )
        # And some workspace layout state.
        await b.exec_shell(
            ["sh", "-c", "mkdir -p data sessions && echo mcp > .mcp.json"],
        )

        await ws.snapshot("with-skills")

        # Wipe the skill tree + layout via exec_shell.
        await b.exec_shell(["sh", "-c", "rm -rf skills/my_skill data sessions .mcp.json"])
        assert "my_skill" not in await b.list_dir("skills")

        # Restore — the skill tree + layout come back without re-seeding.
        await ws.restore("with-skills")
        assert "my_skill" in await b.list_dir("skills")
        assert await b.read_file("skills/my_skill/SKILL.md") == b"# my skill\n"
        assert (
            await b.read_file("skills/my_skill/helper.py")
            == b"def run(): return 42\n"
        )
        assert await b.read_file(".mcp.json") == b"mcp\n"
    finally:
        await ws._teardown_backend()


# ── isolation across workspaces ─────────────────────────────────


async def test_two_workspaces_snapshots_are_isolated(tmp_path):
    """Two workspaces with independent snapshots_root must not collide
    even when using the same tag."""
    ws1_dir = tmp_path / "ws1"
    ws1_dir.mkdir()
    ws2_dir = tmp_path / "ws2"
    ws2_dir.mkdir()
    snaps1 = tmp_path / "snaps1"
    snaps2 = tmp_path / "snaps2"

    ws1 = AgentFSWorkspace(host_workdir=str(ws1_dir), snapshots_root=str(snaps1))
    ws2 = AgentFSWorkspace(host_workdir=str(ws2_dir), snapshots_root=str(snaps2))
    await ws1._provision_backend()
    await ws2._provision_backend()
    try:
        await ws1.get_backend().write_file("owner.txt", b"ws1")
        await ws2.get_backend().write_file("owner.txt", b"ws2")
        await ws1.snapshot("v1")
        await ws2.snapshot("v1")

        # Mutate both, then restore both — each must get its own back.
        await ws1.get_backend().write_file("owner.txt", b"mutated1")
        await ws2.get_backend().write_file("owner.txt", b"mutated2")

        await ws1.restore("v1")
        await ws2.restore("v1")
        assert await ws1.get_backend().read_file("owner.txt") == b"ws1"
        assert await ws2.get_backend().read_file("owner.txt") == b"ws2"
    finally:
        await ws1._teardown_backend()
        await ws2._teardown_backend()


# ── metrics ─────────────────────────────────────────────────────


async def test_metrics_includes_snapshot_count(workdir, snapshots_root):
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        b = ws.get_backend()
        await b.write_file("f.txt", b"x")

        m0 = await ws.metrics()
        assert m0["snapshot_count"] == 0
        assert m0["snapshots_root"] == ws._snapshots_root

        await ws.snapshot("a")
        await ws.snapshot("b")
        m1 = await ws.metrics()
        assert m1["snapshot_count"] == 2

        # Atomic replace of an existing tag must not double-count.
        await ws.snapshot("a")
        m2 = await ws.metrics()
        assert m2["snapshot_count"] == 2
    finally:
        await ws._teardown_backend()


# ── restore keeps the workspace usable end-to-end ───────────────


async def test_restore_then_exec_shell_still_works(workdir, snapshots_root):
    """After restore, the backend's workdir pointer is still valid —
    a real exec_shell must succeed against the restored tree."""
    ws = AgentFSWorkspace(host_workdir=workdir, snapshots_root=snapshots_root)
    await ws._provision_backend()
    try:
        b = ws.get_backend()
        await b.exec_shell(["sh", "-c", "echo original > note.txt"])
        await ws.snapshot("exec-test")

        await b.exec_shell(["sh", "-c", "echo garbage > note.txt"])
        await b.exec_shell(["sh", "-c", "rm -rf nested"])

        await ws.restore("exec-test")

        # Real exec against the restored tree.
        r = await b.exec_shell(["sh", "-c", "cat note.txt"])
        assert r.exit_code == 0
        assert r.stdout.strip() == b"original"

        # And new writes land in the restored tree.
        await b.write_file("after.txt", b"ok")
        r2 = await b.exec_shell(["sh", "-c", "ls after.txt"])
        assert r2.exit_code == 0
    finally:
        await ws._teardown_backend()
