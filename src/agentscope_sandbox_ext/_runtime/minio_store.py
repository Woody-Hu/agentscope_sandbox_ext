# -*- coding: utf-8 -*-
""":class:`SnapshotStore` backed by MinIO (S3-compatible object storage).

Snapshot data is stored as objects in a MinIO bucket, organized by
``{actor_id}/{tag}/{timestamp}.tar``.  Metadata is stored as object
custom metadata.
"""

from __future__ import annotations

import hashlib
import io
import time
from typing import BinaryIO

from agentscope._logging import logger

from .snapshot_store import SnapshotMeta, SnapshotStore


class MinioSnapshotStore(SnapshotStore):
    """MinIO (S3-compatible) snapshot store.

    Args:
        endpoint: MinIO server endpoint (e.g. ``"localhost:19000"``).
        access_key: Access key.
        secret_key: Secret key.
        bucket: Bucket name for snapshots.
        secure: Use HTTPS (default ``False``).
        region: Region (default ``"us-east-1"``).
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str = "snapshots",
        *,
        secure: bool = False,
        region: str = "us-east-1",
    ) -> None:
        self._endpoint = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._secure = secure
        self._region = region
        self._client = None

    def _ensure_client(self):
        if self._client is None:
            from minio import Minio
            self._client = Minio(
                self._endpoint,
                access_key=self._access_key,
                secret_key=self._secret_key,
                secure=self._secure,
                region=self._region,
            )
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
        return self._client

    @staticmethod
    def _object_name(actor_id: str, tag: str, ts: float) -> str:
        return f"{actor_id}/{tag}/{ts:.6f}.tar"

    _META_PREFIX = "x-amz-meta-"

    @classmethod
    def _parse_metadata(cls, stat_metadata: dict | None) -> dict:
        """Strip ``x-amz-meta-`` prefix from MinIO stat metadata keys."""
        if not stat_metadata:
            return {}
        result = {}
        for k, v in stat_metadata.items():
            k_lower = k.lower()
            if k_lower.startswith(cls._META_PREFIX):
                result[k_lower[len(cls._META_PREFIX):]] = v
        return result

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
        if isinstance(data, BinaryIO):
            data = data.read()

        ts = time.monotonic()
        hasher = hashlib.sha256()
        hasher.update(data)

        if snapshot_ref is None:
            snapshot_ref = f"minio:{self._bucket}/{self._object_name(actor_id, tag, ts)}"

        # Use snapshot_ref as the object name directly
        obj_name = snapshot_ref

        client = self._ensure_client()
        client.put_object(
            self._bucket,
            obj_name,
            data=io.BytesIO(data),
            length=len(data),
            metadata={
                "actor_id": actor_id,
                "tag": tag,
                "compression": compression,
                "checksum": hasher.hexdigest(),
                "created_at": str(ts),
                "template_id": template_id or "",
            },
        )

        return SnapshotMeta(
            snapshot_ref=snapshot_ref,
            actor_id=actor_id,
            template_id=template_id,
            tag=tag,
            size_bytes=len(data),
            created_at=ts,
            compression=compression,
            checksum=hasher.hexdigest(),
        )

    async def get(self, snapshot_ref: str) -> bytes:
        client = self._ensure_client()
        try:
            response = client.get_object(self._bucket, snapshot_ref)
            return response.read()
        except Exception as e:
            raise KeyError(f"Snapshot not found: {snapshot_ref!r}") from e

    async def delete(self, snapshot_ref: str) -> None:
        client = self._ensure_client()
        try:
            client.remove_object(self._bucket, snapshot_ref)
        except Exception as e:
            raise KeyError(f"Snapshot not found: {snapshot_ref!r}") from e

    async def list(
        self, actor_id: str, *, tag: str | None = None,
    ) -> list[SnapshotMeta]:
        client = self._ensure_client()
        results: list[SnapshotMeta] = []

        # Two prefix patterns: tiered mode (snap:actor:) and standalone (minio:bucket/actor/)
        prefixes = [
            f"snap:{actor_id}:",                           # tiered mode
            f"minio:{self._bucket}/{actor_id}/",           # standalone mode
        ]
        if tag is not None:
            prefixes = [
                f"snap:{actor_id}:{tag}:",                 # tiered + tag filter
                f"minio:{self._bucket}/{actor_id}/{tag}/", # standalone + tag filter
            ]

        seen: set[str] = set()
        for prefix in prefixes:
            for obj in client.list_objects(self._bucket, prefix=prefix, recursive=True):
                obj_name = obj.object_name
                if obj_name in seen:
                    continue
                seen.add(obj_name)

                # Parse tag from object name (no stat_object call needed)
                obj_tag = self._parse_tag_from_name(obj_name, actor_id)

                # Parse timestamp from object name
                obj_ts = self._parse_ts_from_name(obj_name)

                if tag is not None and obj_tag != tag:
                    continue

                results.append(SnapshotMeta(
                    snapshot_ref=obj_name,
                    actor_id=actor_id,
                    template_id=None,
                    tag=obj_tag,
                    size_bytes=obj.size,
                    created_at=obj_ts,
                    compression="none",
                    checksum=None,
                ))

        return sorted(results, key=lambda m: m.created_at, reverse=True)

    @staticmethod
    def _parse_tag_from_name(obj_name: str, actor_id: str) -> str:
        """Extract tag from object name without a stat_object round-trip.

        Tiered mode:  ``snap:<actor_id>:<tag>:<ts>``
        Standalone:   ``minio:<bucket>/<actor_id>/<tag>/<ts>.tar``
        """
        # Tiered mode: snap:actor:tag:ts
        if obj_name.startswith("snap:"):
            # Format: snap:actor_id:tag:ts
            rest = obj_name[len("snap:"):]
            parts = rest.split(":")
            if len(parts) >= 2:
                return parts[1]  # tag is after actor_id
            return "default"

        # Standalone mode: minio:bucket/actor_id/tag/ts.tar
        parts = obj_name.split("/")
        if len(parts) >= 4:
            return parts[2]  # tag is parts[2]
        if len(parts) >= 3:
            return parts[1]
        return "default"

    @staticmethod
    def _parse_ts_from_name(obj_name: str) -> float:
        """Extract creation timestamp from object name."""
        # Tiered mode: snap:actor:tag:123456.789
        if obj_name.startswith("snap:"):
            parts = obj_name.rsplit(":", 1)
            if len(parts) == 2:
                try:
                    return float(parts[1])
                except (ValueError, TypeError):
                    pass
            return 0.0

        # Standalone mode: minio:bucket/actor/tag/ts.tar
        base = obj_name.rsplit("/", 1)[-1]  # "ts.tar"
        base = base.rsplit(".tar", 1)[0]    # "ts"
        try:
            return float(base)
        except (ValueError, TypeError):
            return 0.0

    async def copy(
        self, snapshot_ref: str, target_actor_id: str, target_tag: str,
    ) -> SnapshotMeta:
        client = self._ensure_client()

        # Stat source to get metadata and size
        try:
            stat = client.stat_object(self._bucket, snapshot_ref)
        except Exception as e:
            raise KeyError(f"Source snapshot not found: {snapshot_ref!r}") from e

        meta = self._parse_metadata(stat.metadata)
        template_id = meta.get("template_id") or None
        compression = meta.get("compression", "none")
        checksum = meta.get("checksum")

        # Create target object name
        ts = time.monotonic()
        target_ref = f"minio:{self._bucket}/{self._object_name(target_actor_id, target_tag, ts)}"

        # Server-side copy with replaced metadata
        from minio.commonconfig import CopySource
        client.copy_object(
            self._bucket,
            target_ref,
            CopySource(self._bucket, snapshot_ref),
            metadata={
                "actor_id": target_actor_id,
                "tag": target_tag,
                "compression": compression,
                "checksum": checksum or "",
                "created_at": str(ts),
                "template_id": template_id or "",
            },
        )

        return SnapshotMeta(
            snapshot_ref=target_ref,
            actor_id=target_actor_id,
            template_id=template_id,
            tag=target_tag,
            size_bytes=stat.size,
            created_at=ts,
            compression=compression,
            checksum=checksum,
        )

    async def metrics(self) -> dict:
        client = self._ensure_client()
        total_size = 0
        total_objects = 0
        for obj in client.list_objects(self._bucket, recursive=True):
            total_size += obj.size
            total_objects += 1
        return {
            "type": "MinioSnapshotStore",
            "endpoint": self._endpoint,
            "bucket": self._bucket,
            "total_objects": total_objects,
            "total_size_bytes": total_size,
        }
