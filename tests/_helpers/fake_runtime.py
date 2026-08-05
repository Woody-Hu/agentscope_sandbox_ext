# -*- coding: utf-8 -*-
"""In-process :class:`SandboxRuntime` test double + worker factory.

This is the *real* counterpart to ``_RealTestSandbox`` in
``tests/test_pool.py``: it implements the :class:`SandboxRuntime`
protocol with real on-disk I/O (each snapshot is a deep ``shutil``
copy of the live workdir, each restore is a deep copy back), so the
actor / worker / checkpoint tests get a genuine workout of the
snapshot / restore / stage primitives without needing a microVM.

Nothing here is a mock: ``snapshot``/``restore``/``stage`` really
move bytes on disk, ``provision`` really creates the workdir, and
``close`` really marks the runtime dead.  The recorded ``calls``
list lets tests assert on the exact call sequence the layer above
issued.
"""

from __future__ import annotations

import itertools
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Any

from agentscope_sandbox_ext._actor._types import SandboxClass
from agentscope_sandbox_ext._worker._types import Worker


@dataclass
class FakeSandboxRuntime:
    """Real-I/O :class:`SandboxRuntime` for unit tests.

    Each instance owns a tempdir ``workdir``; ``snapshot(tag)`` deep-copies
    it to ``snapshots/<tag>`` and ``restore(tag)`` deep-copies it back.
    ``stage(tag, src)`` deep-copies an external tree into ``snapshots/<tag>``
    so a subsequent ``restore(tag)`` activates it — exactly the path the
    checkpoint manager uses to materialise a durable snapshot.
    """

    sandbox_class_value: str = "vfs"
    node_value: str = "node-0"
    worker_id_value: str = field(default_factory=lambda: f"w-{uuid.uuid4().hex[:8]}")
    labels: dict[str, str] = field(default_factory=dict)
    provision_delay: float = 0.0
    snapshot_delay: float = 0.0
    restore_delay: float = 0.0
    fail_on_snapshot: bool = False
    fail_on_restore: bool = False

    # Recorded state (not part of the protocol surface).
    is_alive: bool = field(default=False, init=False)
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = field(
        default_factory=list, init=False,
    )
    _workdir: str | None = field(default=None, init=False)
    _snapshots_root: str | None = field(default=None, init=False)

    # ── protocol attributes ─────────────────────────────────────

    @property
    def worker_id(self) -> str:
        return self.worker_id_value

    @property
    def sandbox_class(self) -> SandboxClass:
        return SandboxClass.of(self.sandbox_class_value)

    @property
    def node(self) -> str:
        return self.node_value

    # ── protocol methods ────────────────────────────────────────

    async def provision(self) -> None:
        if self.provision_delay:
            import asyncio
            await asyncio.sleep(self.provision_delay)
        self._workdir = tempfile.mkdtemp(prefix="fake_ws_")
        self._snapshots_root = os.path.join(
            self._workdir, "..", f"snaps_{uuid.uuid4().hex[:6]}",
        )
        os.makedirs(self._snapshots_root, exist_ok=True)
        self.is_alive = True
        self.calls.append(("provision", (), {}))

    async def snapshot(self, tag: str) -> str:
        if self.snapshot_delay:
            import asyncio
            await asyncio.sleep(self.snapshot_delay)
        if self.fail_on_snapshot:
            raise RuntimeError(f"snapshot {tag!r} failed (test-injected)")
        assert self._workdir is not None, "snapshot before provision"
        dest = os.path.join(self._snapshots_root, tag)
        if os.path.exists(dest):
            shutil.rmtree(dest)
        shutil.copytree(self._workdir, dest, symlinks=True)
        self.calls.append(("snapshot", (tag,), {}))
        return dest

    async def restore(self, tag: str) -> None:
        if self.restore_delay:
            import asyncio
            await asyncio.sleep(self.restore_delay)
        if self.fail_on_restore:
            raise RuntimeError(f"restore {tag!r} failed (test-injected)")
        assert self._workdir is not None, "restore before provision"
        src = os.path.join(self._snapshots_root, tag)
        if not os.path.isdir(src):
            raise KeyError(f"no snapshot tagged {tag!r}")
        # Wipe workdir contents and copy snapshot back.
        for entry in os.listdir(self._workdir):
            full = os.path.join(self._workdir, entry)
            if os.path.isdir(full):
                shutil.rmtree(full)
            else:
                os.unlink(full)
        shutil.copytree(src, self._workdir, dirs_exist_ok=True, symlinks=True)
        self.calls.append(("restore", (tag,), {}))

    async def stage(self, tag: str, source_path: str) -> None:
        assert self._snapshots_root is not None, "stage before provision"
        dest = os.path.join(self._snapshots_root, tag)
        if os.path.exists(dest):
            shutil.rmtree(dest)
        shutil.copytree(source_path, dest, symlinks=True)
        self.calls.append(("stage", (tag, source_path), {}))

    async def close(self) -> None:
        self.is_alive = False
        if self._workdir is not None and os.path.isdir(self._workdir):
            shutil.rmtree(self._workdir, ignore_errors=True)
        if self._snapshots_root is not None and os.path.isdir(self._snapshots_root):
            shutil.rmtree(self._snapshots_root, ignore_errors=True)
        self.calls.append(("close", (), {}))

    # ── test helpers ────────────────────────────────────────────

    def write_file(self, rel: str, data: bytes) -> None:
        """Write a file under the live workdir (test seeding)."""
        assert self._workdir is not None
        full = os.path.join(self._workdir, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(data)

    def read_file(self, rel: str) -> bytes:
        assert self._workdir is not None
        with open(os.path.join(self._workdir, rel), "rb") as f:
            return f.read()


# ── worker factory ──────────────────────────────────────────────


_NODE_COUNTER = itertools.count()


def make_worker_factory(
    *,
    sandbox_class: str = "vfs",
    node: str | None = None,
    labels: dict[str, str] | None = None,
    provision_delay: float = 0.0,
    snapshot_delay: float = 0.0,
    restore_delay: float = 0.0,
    fail_on_snapshot: bool = False,
    fail_on_restore: bool = False,
):
    """Return an async factory that builds a real :class:`Worker`."""

    async def _factory() -> Worker:
        n = node or f"node-{next(_NODE_COUNTER)}"
        runtime = FakeSandboxRuntime(
            sandbox_class_value=sandbox_class,
            node_value=n,
            labels=dict(labels or {}),
            provision_delay=provision_delay,
            snapshot_delay=snapshot_delay,
            restore_delay=restore_delay,
            fail_on_snapshot=fail_on_snapshot,
            fail_on_restore=fail_on_restore,
        )
        await runtime.provision()
        return Worker(
            worker_id=runtime.worker_id,
            runtime=runtime,
            labels=dict(labels or {}),
        )

    return _factory


__all__ = ["FakeSandboxRuntime", "make_worker_factory"]
