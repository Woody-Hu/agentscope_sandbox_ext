# -*- coding: utf-8 -*-
""":class:`SnapshotStore` backed by PostgreSQL.

Snapshot data is stored as ``BYTEA`` in a ``snapshots`` table.
Uses ``asyncpg`` for async access.
"""

from __future__ import annotations

import hashlib
import time
from typing import BinaryIO

from agentscope._logging import logger

from .snapshot_store import SnapshotMeta, SnapshotStore


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    snapshot_ref TEXT PRIMARY KEY,
    actor_id     TEXT NOT NULL,
    template_id  TEXT,
    tag          TEXT NOT NULL DEFAULT 'default',
    size_bytes   BIGINT NOT NULL DEFAULT 0,
    created_at   DOUBLE PRECISION NOT NULL DEFAULT 0,
    compression  TEXT NOT NULL DEFAULT 'none',
    checksum     TEXT,
    data         BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_{table}_actor_tag
    ON {table} (actor_id, tag);

CREATE INDEX IF NOT EXISTS idx_{table}_created_at
    ON {table} (created_at);
"""


class PostgresSnapshotStore(SnapshotStore):
    """PostgreSQL-backed snapshot store.

    Args:
        dsn: PostgreSQL connection string
            (e.g. ``"postgresql://user:pass@localhost:5432/db"``).
        table_name: Table name for snapshots (default: ``"snapshots"``).
    """

    def __init__(self, dsn: str, *, table_name: str = "snapshots") -> None:
        self._dsn = dsn
        self._table_name = table_name
        self._pool = None

    async def _ensure_pool(self):
        if self._pool is None:
            import asyncpg
            self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=4)
            async with self._pool.acquire() as conn:
                await conn.execute(_SCHEMA_SQL.format(table=self._table_name))
        return self._pool

    async def _conn(self):
        pool = await self._ensure_pool()
        return pool.acquire()

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
            snapshot_ref = f"pg:{actor_id}:{tag}:{ts:.6f}"

        async with await self._conn() as conn:
            await conn.execute(
                f"INSERT INTO {self._table_name} "
                "(snapshot_ref, actor_id, template_id, tag, size_bytes, "
                " created_at, compression, checksum, data) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                snapshot_ref, actor_id, template_id, tag,
                len(data), ts, compression, hasher.hexdigest(), data,
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
        async with await self._conn() as conn:
            row = await conn.fetchrow(
                f"SELECT data FROM {self._table_name} WHERE snapshot_ref = $1",
                snapshot_ref,
            )
        if row is None:
            raise KeyError(f"Snapshot not found: {snapshot_ref!r}")
        return row["data"]

    async def delete(self, snapshot_ref: str) -> None:
        async with await self._conn() as conn:
            result = await conn.execute(
                f"DELETE FROM {self._table_name} WHERE snapshot_ref = $1",
                snapshot_ref,
            )
        if result == "DELETE 0":
            raise KeyError(f"Snapshot not found: {snapshot_ref!r}")

    async def list(
        self, actor_id: str, *, tag: str | None = None,
    ) -> list[SnapshotMeta]:
        async with await self._conn() as conn:
            if tag is not None:
                rows = await conn.fetch(
                    f"SELECT snapshot_ref, actor_id, template_id, tag, "
                    "size_bytes, created_at, compression, checksum "
                    f"FROM {self._table_name} "
                    "WHERE actor_id = $1 AND tag = $2 "
                    "ORDER BY created_at DESC",
                    actor_id, tag,
                )
            else:
                rows = await conn.fetch(
                    f"SELECT snapshot_ref, actor_id, template_id, tag, "
                    "size_bytes, created_at, compression, checksum "
                    f"FROM {self._table_name} "
                    "WHERE actor_id = $1 "
                    "ORDER BY created_at DESC",
                    actor_id,
                )
        return [
            SnapshotMeta(
                snapshot_ref=r["snapshot_ref"],
                actor_id=r["actor_id"],
                template_id=r["template_id"],
                tag=r["tag"],
                size_bytes=r["size_bytes"],
                created_at=r["created_at"],
                compression=r["compression"],
                checksum=r["checksum"],
            )
            for r in rows
        ]

    async def copy(
        self, snapshot_ref: str, target_actor_id: str, target_tag: str,
    ) -> SnapshotMeta:
        ts = time.monotonic()
        new_ref = f"pg:{target_actor_id}:{target_tag}:{ts:.6f}"

        async with await self._conn() as conn:
            row = await conn.fetchrow(
                f"INSERT INTO {self._table_name} "
                "(snapshot_ref, actor_id, template_id, tag, size_bytes, "
                " created_at, compression, checksum, data) "
                "SELECT $1, $2, template_id, $3, size_bytes, "
                "       $4, compression, checksum, data "
                f"FROM {self._table_name} WHERE snapshot_ref = $5 "
                "RETURNING template_id, size_bytes, compression, checksum",
                new_ref, target_actor_id, target_tag, ts, snapshot_ref,
            )
        if row is None:
            raise KeyError(f"Source snapshot not found: {snapshot_ref!r}")

        return SnapshotMeta(
            snapshot_ref=new_ref,
            actor_id=target_actor_id,
            template_id=row["template_id"],
            tag=target_tag,
            size_bytes=row["size_bytes"],
            created_at=ts,
            compression=row["compression"],
            checksum=row["checksum"],
        )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def metrics(self) -> dict:
        async with await self._conn() as conn:
            row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt, COALESCE(SUM(size_bytes), 0) as total "
                f"FROM {self._table_name}"
            )
        return {
            "type": "PostgresSnapshotStore",
            "dsn": self._dsn,
            "total_objects": row["cnt"],
            "total_size_bytes": row["total"],
        }
