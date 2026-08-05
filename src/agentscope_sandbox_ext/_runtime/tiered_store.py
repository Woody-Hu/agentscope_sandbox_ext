# -*- coding: utf-8 -*-
""":class:`TieredSnapshotStore` — multi-tier snapshot storage orchestration.

Wraps multiple :class:`SnapshotStore` backends arranged in priority tiers.
On read, higher-priority tiers are checked first.  On a miss, the data is
fetched from a lower tier and *promoted* into the higher tier.  On write
(put), data is written to all tiers that have ``write_through=True``.

A unified ``snapshot_ref`` (``snap:<actor_id>:<tag>:<ts>``) is generated
by the tiered store and passed to every backend so that the same reference
resolves across all tiers.

Eviction is triggered when a tier exceeds its ``max_size_bytes`` or
``max_objects`` caps, respecting the configured :class:`EvictionPolicy`.

Supports single-tier operation (just one tier) or multi-tier.

See ``docs/AGENT_RUNTIME.md`` for the full design.
"""

from __future__ import annotations

import asyncio
import time
from typing import BinaryIO

from agentscope._logging import logger

from .snapshot_store import (
    EvictionPolicy,
    SnapshotMeta,
    SnapshotStore,
    StorageTier,
)

_UNIFIED_REF_PREFIX = "snap:"


class TieredSnapshotStore(SnapshotStore):
    """Multi-tier snapshot storage with promotion and eviction.

    Tiers are ordered by priority (lower = checked first).  On a get miss
    in a higher tier, the data is fetched from the next tier and promoted
    upward.  Eviction fires when a tier exceeds its capacity limits.

    Args:
        tiers: Ordered list of :class:`StorageTier` configs.
        sweep_interval: Seconds between eviction sweeps (default 60s).
    """

    def __init__(
        self,
        tiers: list[StorageTier],
        *,
        sweep_interval: float = 60.0,
    ) -> None:
        if not tiers:
            raise ValueError("At least one tier is required")
        self._tiers = sorted(tiers, key=lambda t: t.priority)
        self._sweep_interval = sweep_interval
        self._lock = asyncio.Lock()
        # Per-tier tracking: tier_name -> set of snapshot_refs in that tier
        self._tier_refs: dict[str, set[str]] = {t.name: set() for t in self._tiers}
        # snapshot_ref -> last_access monotonic timestamp (for LRU / TTL)
        self._access_times: dict[str, float] = {}
        # snapshot_ref -> (actor_id, tag, template_id, compression)
        self._meta_cache: dict[str, tuple[str, str, str | None, str]] = {}
        self._sweep_task: asyncio.Task | None = None
        self._closed = False

    # ── lifecycle ────────────────────────────────────────────────

    async def start(self) -> None:
        """Start background eviction sweeper."""
        if self._sweep_task is None:
            self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def close(self) -> None:
        """Stop background tasks."""
        if self._closed:
            return
        self._closed = True
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass
            self._sweep_task = None

    # ── helpers ──────────────────────────────────────────────────

    @staticmethod
    def _make_ref(actor_id: str, tag: str, ts: float) -> str:
        return f"{_UNIFIED_REF_PREFIX}{actor_id}:{tag}:{ts:.6f}"

    @staticmethod
    def _parse_ref(snapshot_ref: str) -> tuple[str, str, float]:
        """Parse unified ref into (actor_id, tag, ts)."""
        if not snapshot_ref.startswith(_UNIFIED_REF_PREFIX):
            raise ValueError(f"Not a unified ref: {snapshot_ref!r}")
        inner = snapshot_ref[len(_UNIFIED_REF_PREFIX):]
        parts = inner.rsplit(":", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid unified ref: {snapshot_ref!r}")
        actor_tag, ts_str = parts
        actor_id, _, tag = actor_tag.partition(":")
        return actor_id, tag, float(ts_str)

    # ── SnapshotStore interface ──────────────────────────────────

    async def put(
        self,
        actor_id: str,
        tag: str,
        data: bytes | BinaryIO,
        *,
        compression: str = "none",
        template_id: str | None = None,
    ) -> SnapshotMeta:
        """Write to all write-through tiers in parallel with a unified snapshot_ref."""
        if isinstance(data, BinaryIO):
            raw = data.read()
        else:
            raw = data

        ts = time.monotonic()
        unified_ref = self._make_ref(actor_id, tag, ts)

        write_tiers = [t for t in self._tiers if t.write_through]
        if not write_tiers:
            raise RuntimeError("No write-through tier configured")

        # Parallel writes to all write-through tiers
        async def _write_to_tier(tier: StorageTier) -> SnapshotMeta:
            meta = await tier.store.put(
                actor_id, tag, raw,
                compression=compression,
                template_id=template_id,
                snapshot_ref=unified_ref,
            )
            self._tier_refs[tier.name].add(unified_ref)
            return meta

        results = await asyncio.gather(*[_write_to_tier(t) for t in write_tiers])
        primary_meta = results[0]

        # Cache metadata for promotion
        self._meta_cache[unified_ref] = (actor_id, tag, template_id, compression)
        self._access_times[unified_ref] = ts

        return primary_meta

    async def get(self, snapshot_ref: str) -> bytes:
        """Try each tier in priority order.  On miss, promote from lower tier."""
        self._access_times[snapshot_ref] = time.monotonic()

        data: bytes | None = None
        found_tier_idx: int = -1

        for i, tier in enumerate(self._tiers):
            try:
                data = await tier.store.get(snapshot_ref)
                found_tier_idx = i
                break
            except KeyError:
                continue

        if data is None:
            raise KeyError(f"Snapshot not found in any tier: {snapshot_ref!r}")

        # Promote upward: if found in tier > 0, copy to higher tiers
        if found_tier_idx > 0:
            # Get cached metadata for promotion
            cached = self._meta_cache.get(snapshot_ref)
            if cached is not None:
                actor_id, tag, template_id, compression = cached
                for j in range(found_tier_idx):
                    higher_tier = self._tiers[j]
                    if not higher_tier.write_through:
                        continue
                    try:
                        await higher_tier.store.put(
                            actor_id, tag, data,
                            compression=compression,
                            template_id=template_id,
                            snapshot_ref=snapshot_ref,
                        )
                        self._tier_refs[higher_tier.name].add(snapshot_ref)
                        logger.debug(
                            "TieredStore: promoted %s to tier %s",
                            snapshot_ref, higher_tier.name)
                    except Exception:
                        logger.exception(
                            "TieredStore: failed to promote %s to tier %s",
                            snapshot_ref, higher_tier.name)

        return data

    async def delete(self, snapshot_ref: str) -> None:
        """Delete from all tiers."""
        errors: list[Exception] = []
        for tier in self._tiers:
            try:
                await tier.store.delete(snapshot_ref)
            except KeyError:
                pass
            except Exception as e:
                errors.append(e)
            self._tier_refs[tier.name].discard(snapshot_ref)
        self._access_times.pop(snapshot_ref, None)
        self._meta_cache.pop(snapshot_ref, None)
        if errors and len(errors) == len(self._tiers):
            raise RuntimeError(
                f"Failed to delete snapshot {snapshot_ref!r} from all tiers: {errors}")

    async def list(
        self, actor_id: str, *, tag: str | None = None,
    ) -> list[SnapshotMeta]:
        """List from all tiers, deduplicating by unified snapshot_ref."""
        seen: set[str] = set()
        results: list[SnapshotMeta] = []
        for tier in self._tiers:
            for meta in await tier.store.list(actor_id, tag=tag):
                if meta.snapshot_ref not in seen:
                    seen.add(meta.snapshot_ref)
                    results.append(meta)
        return results

    async def copy(
        self, snapshot_ref: str, target_actor_id: str, target_tag: str,
    ) -> SnapshotMeta:
        """Copy from the first tier that has it, replicating to write-through tiers."""
        data = await self.get(snapshot_ref)
        return await self.put(
            target_actor_id, target_tag, data,
            compression="none",
        )

    # ── tree helpers (delegate to tier 0) ────────────────────────

    async def put_tree(
        self, actor_id: str, tag: str, source_dir: str, *,
        template_id: str | None = None,
    ) -> SnapshotMeta:
        return await self._tiers[0].store.put_tree(
            actor_id, tag, source_dir, template_id=template_id)

    async def restore_tree(
        self, snapshot_ref: str, target_dir: str,
    ) -> None:
        data = await self.get(snapshot_ref)
        import io, tarfile
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tar.extractall(target_dir)

    # ── eviction ─────────────────────────────────────────────────

    async def _sweep_loop(self) -> None:
        while not self._closed:
            try:
                await asyncio.sleep(self._sweep_interval)
            except asyncio.CancelledError:
                return
            try:
                await self._sweep_once()
            except Exception:
                logger.exception("TieredStore: sweep tick failed")

    async def _sweep_once(self) -> None:
        for tier in self._tiers:
            if tier.eviction_policy == EvictionPolicy.NONE:
                continue
            await self._evict_tier(tier)

    async def _evict_tier(self, tier: StorageTier) -> None:
        """Evict entries from *tier* until constraints are satisfied."""
        refs = list(self._tier_refs.get(tier.name, set()))
        if not refs:
            return

        if tier.eviction_policy == EvictionPolicy.LRU:
            # Sort by last access time, oldest first
            sorted_refs = sorted(refs, key=lambda r: self._access_times.get(r, 0.0))
            for ref in sorted_refs:
                if self._tier_within_limits(tier, self._tier_refs[tier.name]):
                    break
                try:
                    await tier.store.delete(ref)
                    self._tier_refs[tier.name].discard(ref)
                    logger.debug("TieredStore: LRU evicted %s from tier %s", ref, tier.name)
                except KeyError:
                    self._tier_refs[tier.name].discard(ref)

        elif tier.eviction_policy == EvictionPolicy.TTL:
            if tier.ttl_seconds <= 0:
                return
            now = time.monotonic()
            for ref in list(refs):
                last_access = self._access_times.get(ref, 0.0)
                if now - last_access > tier.ttl_seconds:
                    try:
                        await tier.store.delete(ref)
                        self._tier_refs[tier.name].discard(ref)
                        logger.debug("TieredStore: TTL evicted %s from tier %s", ref, tier.name)
                    except KeyError:
                        self._tier_refs[tier.name].discard(ref)

    def _tier_within_limits(self, tier: StorageTier, refs: set[str]) -> bool:
        """Return True if *tier* is within its configured capacity limits."""
        if tier.max_objects > 0 and len(refs) > tier.max_objects:
            return False
        return True

    async def metrics(self) -> dict:
        tier_metrics = {}
        for tier in self._tiers:
            tier_metrics[tier.name] = {
                "priority": tier.priority,
                "eviction_policy": tier.eviction_policy.value,
                "num_objects": len(self._tier_refs.get(tier.name, set())),
                "store": await tier.store.metrics(),
            }
        return {
            "type": "TieredSnapshotStore",
            "num_tiers": len(self._tiers),
            "tiers": tier_metrics,
            "tracked_access_count": len(self._access_times),
        }
