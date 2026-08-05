# -*- coding: utf-8 -*-
"""Worker scheduler: pick an idle worker satisfying an actor's constraints.

The algorithm mirrors the reference runtime's scheduler: filter the
candidate workers by the hard constraints (sandbox class, label
selectors, required-nodes locality), then **random-spread** among the
survivors.  Random spreading is deliberately simple and stateless — it
avoids hotspot clustering without the cost (and failure modes) of a
scoring pass, and it has no shared mutable state to contend on.

A ``prefer_node`` hint (used by pause-locality resume) is honoured when
a survivor sits on that node: it is chosen immediately rather than
randomly.  This keeps the common "resume right after a pause" path on
the node that already holds the snapshot, avoiding a remote fetch.
"""

from __future__ import annotations

import random
from typing import Iterable

from .._worker._types import Worker
from ._types import (
    WORKER_ACTIVE,
    Constraints,
)


class NoCapacityError(RuntimeError):
    """No idle worker satisfies the constraints."""


class Scheduler:
    """Select an idle :class:`Worker` for an actor's :class:`Constraints`.

    Stateless aside from an optional RNG (injectable for deterministic
    tests).  ``pick`` does not mutate the workers — the caller (worker
    pool) reserves the chosen worker.
    """

    def __init__(self, rng: random.Random | None = None) -> None:
        self._rng = rng or random.Random()

    def pick(
        self,
        workers: Iterable[Worker],
        constraints: Constraints,
        *,
        prefer_node: str | None = None,
    ) -> Worker:
        """Return one idle worker matching *constraints*.

        Args:
            workers: The full pool of workers (idle ones are filtered).
            constraints: Hard scheduling constraints.
            prefer_node: Optional node hint; if a surviving candidate is
                on this node it is chosen immediately (pause locality).

        Raises:
            NoCapacityError: If no idle worker satisfies the constraints.
        """
        candidates = [
            w
            for w in workers
            if self.applies(w, constraints)
        ]
        if not candidates:
            raise NoCapacityError(
                "no idle worker satisfies constraints "
                f"(class={constraints.sandbox_class.value!r}, "
                f"required_nodes={constraints.required_nodes!r})",
            )

        # Pause-locality fast path: if the caller hinted a node and a
        # candidate is on it, take it without randomising.
        if prefer_node is not None:
            for w in candidates:
                if w.node == prefer_node:
                    return w

        # Random spreading — simple, stateless, avoids hotspots.
        return self._rng.choice(candidates)

    @staticmethod
    def applies(worker: Worker, constraints: Constraints) -> bool:
        """Whether *worker* can host an actor with *constraints*.

        A worker applies when it is **idle** (``ACTIVE``, no assigned
        actor), **alive**, of the **same sandbox class**, matches both
        label selectors, and — when ``required_nodes`` is set — sits on
        one of those nodes.  The class constraint is never relaxed
        (snapshots cannot cross classes).
        """
        if worker.status != WORKER_ACTIVE:
            return False
        if worker.assigned_actor is not None:
            return False
        if not worker.is_alive:
            return False
        if worker.sandbox_class != constraints.sandbox_class:
            return False
        if not _labels_match(worker.labels, constraints.template_selector):
            return False
        if not _labels_match(worker.labels, constraints.actor_selector):
            return False
        if constraints.required_nodes and worker.node not in constraints.required_nodes:
            return False
        return True


def _labels_match(
    worker_labels: dict[str, str],
    selector: dict[str, str],
) -> bool:
    """Subset check: every selector k=v must be present on the worker."""
    for k, v in selector.items():
        if worker_labels.get(k) != v:
            return False
    return True


__all__ = ["Scheduler", "NoCapacityError"]
