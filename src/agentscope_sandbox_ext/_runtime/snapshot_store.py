# -*- coding: utf-8 -*-
"""Abstract snapshot storage backend with multi-tier support.

Defines the :class:`SnapshotStore` ABC that all backends implement,
plus the :class:`StorageTier` dataclass for tier configuration and
:class:`EvictionPolicy` for cache-management strategies.

See ``docs/AGENT_RUNTIME.md`` for the full design.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import BinaryIO


@dataclass
class SnapshotMeta:
    """Metadata for a stored snapshot.

    Attributes:
        snapshot_ref: Opaque store-specific reference.
        actor_id: Owner actor.
        template_id: Template this snapshot was created from (if any).
        tag: User-supplied tag.
        size_bytes: Total size of the snapshot artifact(s).
        created_at: POSIX timestamp of creation.
        compression: Compression algorithm.
        checksum: SHA256 hex digest (if available).
    """
    snapshot_ref: str
    actor_id: str
    template_id: str | None = None
    tag: str = "default"
    size_bytes: int = 0
    created_at: float = 0.0
    compression: str = "none"
    checksum: str | None = None


class EvictionPolicy(Enum):
    """Eviction strategies for the tiered store."""
    NONE = "none"
    LRU = "lru"
    SIZE_BASED = "size_based"
    TTL = "ttl"


@dataclass
class StorageTier:
    """Configuration for one storage tier.

    Attributes:
        name: Human-readable tier name (e.g. ``"hot"``, ``"warm"``).
        store: The :class:`SnapshotStore` backend for this tier.
        priority: Lower number = higher priority.  Tier 0 is checked first.
        max_size_bytes: Hard cap on this tier.  When exceeded, eviction fires.
        max_objects: Max number of snapshots in this tier.
        eviction_policy: Which strategy to use when the tier is full.
        ttl_seconds: TTL for ``TTL`` eviction policy.
        write_through: If True, writes go to this tier (not just reads).
    """
    name: str
    store: "SnapshotStore"
    priority: int = 0
    max_size_bytes: int = 0
    max_objects: int = 0
    eviction_policy: EvictionPolicy = EvictionPolicy.NONE
    ttl_seconds: float = 0.0
    write_through: bool = True


class SnapshotStore(ABC):
    """Abstract snapshot storage backend.

    Every backend (local FS, Postgres, MinIO, S3, GCS) implements this
    interface.  The :class:`TieredSnapshotStore` wraps multiple backends
    arranged in priority tiers.
    """

    @abstractmethod
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
        """Store a snapshot artifact and return its metadata.

        Args:
            actor_id: Owner actor.
            tag: User-supplied tag.
            data: Snapshot payload bytes or stream.
            compression: Compression algorithm hint.
            template_id: Template this snapshot was created from.
            snapshot_ref: Optional pre-generated reference.  When provided
                the backend MUST use this value as the ``snapshot_ref``
                in the returned ``SnapshotMeta``.  Backends that derive
                the ref from the storage path should still store data at
                their usual location but return the given ref.
        """
        ...

    @abstractmethod
    async def get(self, snapshot_ref: str) -> bytes:
        """Retrieve a snapshot artifact by reference."""
        ...

    @abstractmethod
    async def delete(self, snapshot_ref: str) -> None:
        """Delete a snapshot artifact."""
        ...

    @abstractmethod
    async def list(
        self,
        actor_id: str,
        *,
        tag: str | None = None,
    ) -> list[SnapshotMeta]:
        """List snapshots for an actor, optionally filtered by tag."""
        ...

    @abstractmethod
    async def copy(
        self,
        snapshot_ref: str,
        target_actor_id: str,
        target_tag: str,
    ) -> SnapshotMeta:
        """Copy a snapshot to another actor (used for template cloning)."""
        ...

    # ── tree-based helpers (optional, with default NotImplemented) ─

    async def put_tree(
        self,
        actor_id: str,
        tag: str,
        source_dir: str,
        *,
        template_id: str | None = None,
    ) -> SnapshotMeta:
        """Archive a directory tree into a snapshot.

        The default implementation reads the tree into a tar archive
        and calls :meth:`put`.  Backends may override for efficiency.
        """
        import io
        import tarfile

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            tar.add(source_dir, arcname=".")
        buf.seek(0)
        return await self.put(
            actor_id, tag, buf,
            compression="gz",
            template_id=template_id,
        )

    async def restore_tree(self, snapshot_ref: str, target_dir: str) -> None:
        """Restore a tree snapshot to a directory.

        The default implementation calls :meth:`get` and untars.
        Backends may override for efficiency.
        """
        import io
        import tarfile

        data = await self.get(snapshot_ref)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tar.extractall(target_dir)

    async def metrics(self) -> dict:
        """Return backend-specific observability fields."""
        return {"type": type(self).__name__}
