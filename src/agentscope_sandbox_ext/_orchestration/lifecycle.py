# -*- coding: utf-8 -*-
"""Idempotent workflow engine for actor lifecycle transitions.

Borrowed from the reference runtime's resume workflow engine, where a
transition (e.g. ``ResumeActor``) is decomposed into a sequence of
idempotent steps — ``LoadActor → ClaimWorker → Restore → Finalize`` —
each with an ``is_complete`` predicate and an ``execute`` action.  If
the orchestrator crashes mid-transition, re-running the workflow
resumes from the first non-complete step rather than restarting from
scratch or double-provisioning.

This module is deliberately small: a :class:`Workflow` is just an
ordered list of :class:`Step`s run under a shared :class:`WorkflowContext`.
The contract each step upholds is:

* ``is_complete(ctx)`` — ``True`` if the step's effect is already
  present (so re-running the workflow skips it).
* ``execute(ctx)`` — perform the effect idempotently; a second call
  must be a no-op or safely re-do the same thing.

Steps may store intermediate results on the context so later steps can
pick them up (e.g. ``ClaimWorker`` stores the claimed worker;
``Restore`` reads it).
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any

from agentscope._logging import logger


class WorkflowError(Exception):
    """A workflow step failed."""


@dataclass
class WorkflowContext:
    """Shared scratch state for one workflow run.

    Steps read inputs from and write results to ``data``.  The workflow
    itself never inspects the contents — it is opaque to the engine.

    ``actor_id`` + ``namespace`` identify the actor the workflow
    transitions (composite identity, matching :class:`ActorRef`).
    """

    actor_id: str
    namespace: str = "default"
    data: dict[str, Any] = field(default_factory=dict)


class Step(abc.ABC):
    """One idempotent step in a :class:`Workflow`.

    Subclasses implement :meth:`is_complete` and :meth:`execute`.
    """

    name: str = "step"

    @abc.abstractmethod
    async def is_complete(self, ctx: WorkflowContext) -> bool:
        """Return ``True`` if this step's effect is already present."""

    @abc.abstractmethod
    async def execute(self, ctx: WorkflowContext) -> None:
        """Perform the step's effect (idempotently)."""

    async def pre_check(self, ctx: WorkflowContext) -> None:
        """Hook for prerequisites; raise :class:`WorkflowError` if unmet.

        Default is a no-op.  Override to validate preconditions that
        ``is_complete`` does not already cover.
        """


class Workflow:
    """An ordered list of :class:`Step`s run against a :class:`WorkflowContext`.

    Running a workflow executes each step whose ``is_complete`` is
    ``False`` in order.  If a step raises, the workflow stops; the
    caller is expected to fix the root cause and re-run the workflow,
    which resumes from the first non-complete step.

    This is the resilience primitive that lets the orchestrator crash
    mid-resume and recover without double-provisioning: steps already
    completed are skipped on the re-run.
    """

    def __init__(self, name: str, steps: list[Step]) -> None:
        if not steps:
            raise ValueError("workflow must have at least one step")
        self.name = name
        self._steps = list(steps)

    async def run(self, ctx: WorkflowContext) -> WorkflowContext:
        """Execute every non-complete step in order."""
        for step in self._steps:
            if await step.is_complete(ctx):
                logger.debug(
                    "Workflow %s: step %s already complete, skipping",
                    self.name,
                    step.name,
                )
                continue
            await step.pre_check(ctx)
            logger.debug(
                "Workflow %s: executing step %s", self.name, step.name
            )
            await step.execute(ctx)
        return ctx

    @property
    def steps(self) -> list[Step]:
        return list(self._steps)


__all__ = ["Workflow", "Step", "WorkflowContext", "WorkflowError"]
