# -*- coding: utf-8 -*-
"""Local filesystem :class:`SnapshotStore` implementation.

Snapshots are stored as compressed tar archives under a configurable
base directory.  Each actor's snapshots live in a per-actor subdirectory.

In *tiered mode* (when ``snapshot_ref`` is provided via :meth:`put`),
files are stored flat under ``base_dir/`` with a ``.meta.json`` sidecar
so that ``get`` / ``delete`` / ``list`` can resolve the unified ref.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tarfile
import time
from typing import BinaryIO

from agentscope._logging import logger

from .snapshot_store import SnapshotMeta, SnapshotStore


class LocalSnapshotStore(SnapshotStore):
    """Local filesystem snapshot store.

    Snapshot artifacts are stored as ``.tar`` archives under
    ``base_dir/<actor_id>/``.

    Args:
        base_dir: Root directory for all snapshots.
        compression: Compression for tar archives (``"gz"``, ``"bz2"``,
            ``"xz"``, or ``""`` for none). Defaults to ``"gz"``.
    """

    def __init__(self, base_dir: str, *, compression: str = "gz") -> None:
        self._base_dir = base_dir
        self._compression = compression
        self._lock = asyncio.Lock()
        os.makedirs(self._base_dir, exist_ok=True)

    @property
    def _suffix(self) -> str:
        if self._compression:
            return f".tar.{self._compression}"
        return ".tar"

    @staticmethod
    def _safe_ref(snapshot_ref: str) -> str:
        return snapshot_ref.replace("/", "_").replace(":", "_")

    # ── SnapshotStore interface ──────────────────────────────────

    async def put(
        self,
        actor_id: str,
        tag: str,
        data: bytes | BinaryIO,
        *,
        compression: str = "none",
        template_id: str | None = None,
        snapshot_ref: str | None = None,
    ) -> SnapshotMeta:
        ts = time.monotonic()
        if snapshot_ref is not None:
            # Tiered mode: store flat under base_dir with metadata sidecar
            os.makedirs(self._base_dir, exist_ok=True)
            safe = self._safe_ref(snapshot_ref)
            archive_path = os.path.join(self._base_dir, f"{safe}{self._suffix}")
            meta_path = archive_path + ".meta.json"
        else:
            # Standalone mode: store at base_dir/<actor_id>/<tag>.<ts>.tar.gz
            actor_dir = os.path.join(self._base_dir, actor_id)
            os.makedirs(actor_dir, exist_ok=True)
            archive_path = os.path.join(actor_dir, f"{tag}.{ts:.6f}.tar{self._suffix}")
            meta_path = None

        size_bytes = 0
        hasher = hashlib.sha256()

        async with self._lock:
            if isinstance(data, bytes):
                with open(archive_path, "wb") as f:
                    f.write(data)
                size_bytes = len(data)
                hasher.update(data)
            else:
                tmp_path = archive_path + ".tmp"
                try:
                    with open(tmp_path, "wb") as f:
                        while True:
                            chunk = data.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                            hasher.update(chunk)
                            size_bytes += len(chunk)
                    os.replace(tmp_path, archive_path)
                except Exception:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                    raise

            # Write metadata sidecar for tiered mode
            if meta_path is not None:
                meta_dict = {
                    "snapshot_ref": snapshot_ref,
                    "actor_id": actor_id,
                    "tag": tag,
                    "template_id": template_id,
                    "size_bytes": size_bytes,
                    "created_at": ts,
                    "compression": compression,
                    "checksum": hasher.hexdigest(),
                }
                with open(meta_path, "w") as f:
                    json.dump(meta_dict, f)

        return SnapshotMeta(
            snapshot_ref=snapshot_ref if snapshot_ref is not None else archive_path,
            actor_id=actor_id,
            template_id=template_id,
            tag=tag,
            size_bytes=size_bytes,
            created_at=ts,
            compression=compression,
            checksum=hasher.hexdigest(),
        )

    async def get(self, snapshot_ref: str) -> bytes:
        # Try direct path (standalone mode)
        if os.path.isfile(snapshot_ref):
            with open(snapshot_ref, "rb") as f:
                return f.read()
        # Try sanitized path (tiered mode)
        safe = self._safe_ref(snapshot_ref)
        path = os.path.join(self._base_dir, f"{safe}{self._suffix}")
        if os.path.isfile(path):
            with open(path, "rb") as f:
                return f.read()
        raise KeyError(f"Snapshot not found: {snapshot_ref!r}")

    async def delete(self, snapshot_ref: str) -> None:
        if os.path.isfile(snapshot_ref):
            os.unlink(snapshot_ref)
            return
        safe = self._safe_ref(snapshot_ref)
        path = os.path.join(self._base_dir, f"{safe}{self._suffix}")
        meta_path = path + ".meta.json"
        if os.path.isfile(path):
            os.unlink(path)
            if os.path.isfile(meta_path):
                os.unlink(meta_path)
            return
        raise KeyError(f"Snapshot not found: {snapshot_ref!r}")

    async def list(
        self, actor_id: str, *, tag: str | None = None,
    ) -> list[SnapshotMeta]:
        results: list[SnapshotMeta] = []

        # First, try tiered mode: scan .meta.json sidecars
        if os.path.isdir(self._base_dir):
            for entry in os.scandir(self._base_dir):
                if not entry.is_file() or not entry.name.endswith(".meta.json"):
                    continue
                try:
                    with open(entry.path, "r") as f:
                        m = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                if m.get("actor_id") != actor_id:
                    continue
                if tag is not None and m.get("tag") != tag:
                    continue
                results.append(SnapshotMeta(
                    snapshot_ref=m["snapshot_ref"],
                    actor_id=m["actor_id"],
                    template_id=m.get("template_id"),
                    tag=m["tag"],
                    size_bytes=m["size_bytes"],
                    created_at=m["created_at"],
                    compression=m.get("compression", "none"),
                    checksum=m.get("checksum"),
                ))

        # Also check standalone mode: per-actor subdirectory
        actor_dir = os.path.join(self._base_dir, actor_id)
        if os.path.isdir(actor_dir):
            suffix = self._suffix
            for entry in sorted(os.scandir(actor_dir), key=lambda e: e.name, reverse=True):
                if not entry.is_file() or not entry.name.endswith(suffix):
                    continue
                name_no_suffix = entry.name[:-len(suffix)]
                parts = name_no_suffix.split(".", 1)
                entry_tag = parts[0] if parts else "unknown"
                if tag is not None and entry_tag != tag:
                    continue
                try:
                    entry_ts = float(parts[1]) if len(parts) > 1 else 0.0
                except (ValueError, IndexError):
                    entry_ts = 0.0
                results.append(SnapshotMeta(
                    snapshot_ref=entry.path,
                    actor_id=actor_id,
                    tag=entry_tag,
                    size_bytes=entry.stat().st_size,
                    created_at=entry_ts,
                ))

        return sorted(results, key=lambda m: m.created_at, reverse=True)

    async def copy(
        self, snapshot_ref: str, target_actor_id: str, target_tag: str,
    ) -> SnapshotMeta:
        data = await self.get(snapshot_ref)
        return await self.put(target_actor_id, target_tag, data, compression="none")

    # ── tree-based snapshot helpers ──────────────────────────────

    async def put_tree(
        self,
        actor_id: str,
        tag: str,
        source_dir: str,
        *,
        template_id: str | None = None,
    ) -> SnapshotMeta:
        if not os.path.isdir(source_dir):
            raise ValueError(f"Source directory does not exist: {source_dir!r}")

        actor_dir = os.path.join(self._base_dir, actor_id)
        os.makedirs(actor_dir, exist_ok=True)

        ts = time.monotonic()
        archive_path = os.path.join(actor_dir, f"{tag}.{ts:.6f}.tar{self._suffix}")

        async with self._lock:
            tmp_path = archive_path + ".tmp"
            try:
                mode = f"w:{self._compression}" if self._compression else "w"
                with tarfile.open(tmp_path, mode) as tar:
                    tar.add(source_dir, arcname=".")
                os.replace(tmp_path, archive_path)
            except Exception:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise

        size_bytes = os.path.getsize(archive_path)
        hasher = hashlib.sha256()
        with open(archive_path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)

        return SnapshotMeta(
            snapshot_ref=archive_path,
            actor_id=actor_id,
            template_id=template_id,
            tag=tag,
            size_bytes=size_bytes,
            created_at=ts,
            compression=self._compression,
            checksum=hasher.hexdigest(),
        )

    async def restore_tree(self, snapshot_ref: str, target_dir: str) -> None:
        if not os.path.isfile(snapshot_ref):
            raise KeyError(f"Snapshot not found: {snapshot_ref!r}")
        if not os.path.isdir(target_dir):
            raise ValueError(f"Target directory does not exist: {target_dir!r}")
        mode = f"r:{self._compression}" if self._compression else "r"
        with tarfile.open(snapshot_ref, mode) as tar:
            tar.extractall(target_dir)

    async def metrics(self) -> dict:
        total_size = 0
        total_files = 0
        if os.path.isdir(self._base_dir):
            for root, _, files in os.walk(self._base_dir):
                for f in files:
                    fp = os.path.join(root, f)
                    total_size += os.path.getsize(fp)
                    total_files += 1
        return {
            "type": "LocalSnapshotStore",
            "base_dir": self._base_dir,
            "total_size_bytes": total_size,
            "total_files": total_files,
        }
