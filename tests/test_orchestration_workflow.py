# -*- coding: utf-8 -*-
"""Tests for the idempotent workflow engine (:mod:`._orchestration.lifecycle`).

Verifies the resilience primitive the orchestrator's resume path relies
on: a :class:`Workflow` decomposed into :class:`Step`s can be re-run
after a crash and **resume from the first non-complete step** rather than
restarting from scratch or double-applying an effect.

The tests use real step objects with real side effects (a counter, a
scratch dict) — no mocking.  ``is_complete`` predicates drive the
skip-on-rerun behaviour; ``execute`` writes idempotent effects that a
second run must not corrupt.
"""

from __future__ import annotations

import asyncio

import pytest

from agentscope_sandbox_ext._orchestration import (
    Step,
    Workflow,
    WorkflowContext,
    WorkflowError,
)


# ── test step implementations ──────────────────────────────────


class _CountingStep(Step):
    """A step that increments a shared counter exactly once per execute."""

    name = "count"

    def __init__(self, counter: list[int], *, complete_after: int = 1) -> None:
        self._counter = counter
        self._complete_after = complete_after

    async def is_complete(self, ctx: WorkflowContext) -> bool:
        return self._counter[0] >= self._complete_after

    async def execute(self, ctx: WorkflowContext) -> None:
        self._counter[0] += 1
        ctx.data["count"] = self._counter[0]


class _RecordingStep(Step):
    """A step that appends its name to ctx.data['order'] when executed."""

    def __init__(self, name: str) -> None:
        self.name = name

    async def is_complete(self, ctx: WorkflowContext) -> bool:
        return self.name in ctx.data.setdefault("order", [])

    async def execute(self, ctx: WorkflowContext) -> None:
        ctx.data.setdefault("order", []).append(self.name)


class _FailingStep(Step):
    """A step that raises on execute until its flag is cleared.

    Completion is tracked by the step's *effect* (its name in the order
    list), not by the flag — so clearing the flag and re-running does
    execute the step rather than falsely reporting it complete.
    """

    name = "fail"

    def __init__(self, flag: list[bool]) -> None:
        self._flag = flag

    async def is_complete(self, ctx: WorkflowContext) -> bool:
        return self.name in ctx.data.setdefault("order", [])

    async def execute(self, ctx: WorkflowContext) -> None:
        if self._flag[0]:
            raise WorkflowError("intentional failure")
        ctx.data.setdefault("order", []).append(self.name)


class _PreCheckStep(Step):
    """A step that validates a precondition via ``pre_check``."""

    name = "precheck"

    def __init__(self, precondition: list[bool]) -> None:
        self._precondition = precondition

    async def is_complete(self, ctx: WorkflowContext) -> bool:
        return ctx.data.get("prechecked") is True

    async def pre_check(self, ctx: WorkflowContext) -> None:
        if not self._precondition[0]:
            raise WorkflowError("precondition unmet")

    async def execute(self, ctx: WorkflowContext) -> None:
        ctx.data["prechecked"] = True


# ── construction / validation ──────────────────────────────────


def test_workflow_rejects_empty_steps():
    with pytest.raises(ValueError, match="at least one step"):
        Workflow("empty", [])


def test_workflow_exposes_steps_copy():
    s = _RecordingStep("a")
    wf = Workflow("w", [s])
    assert wf.steps == [s]
    # Mutating the returned list must not affect the workflow.
    wf.steps.append(_RecordingStep("b"))
    assert len(wf.steps) == 1


# ── happy path ─────────────────────────────────────────────────


async def test_workflow_runs_all_steps_in_order():
    wf = Workflow(
        "happy",
        [_RecordingStep("a"), _RecordingStep("b"), _RecordingStep("c")],
    )
    ctx = WorkflowContext(actor_id="a1")
    await wf.run(ctx)
    assert ctx.data["order"] == ["a", "b", "c"]


async def test_workflow_returns_same_context():
    wf = Workflow("w", [_RecordingStep("a")])
    ctx = WorkflowContext(actor_id="a1")
    result = await wf.run(ctx)
    assert result is ctx


# ── idempotent re-run (the core resilience property) ───────────


async def test_rerun_skips_already_complete_steps():
    """Re-running a workflow after a crash resumes from the first
    non-complete step — completed steps are skipped, not re-executed."""
    counter = [0]
    wf = Workflow(
        "rerun",
        [
            _CountingStep(counter, complete_after=1),
            _RecordingStep("b"),
            _RecordingStep("c"),
        ],
    )
    ctx = WorkflowContext(actor_id="a1")
    await wf.run(ctx)
    assert counter[0] == 1
    assert ctx.data["order"] == ["b", "c"]

    # Re-run: every step is already complete → no re-execution.
    ctx2 = WorkflowContext(actor_id="a1", data=dict(ctx.data))
    await wf.run(ctx2)
    assert counter[0] == 1  # CountingStep NOT re-executed
    assert ctx2.data["order"] == ["b", "c"]  # unchanged


async def test_rerun_resumes_after_partial_completion():
    """A workflow that completed steps 1-2 but not 3 resumes at step 3."""
    wf = Workflow(
        "partial",
        [_RecordingStep("a"), _RecordingStep("b"), _RecordingStep("c")],
    )
    # Pre-seed the context as if steps a & b already ran in a prior
    # (crashed) attempt.
    ctx = WorkflowContext(actor_id="a1", data={"order": ["a", "b"]})
    await wf.run(ctx)
    # Only step c should have executed; a & b skipped.
    assert ctx.data["order"] == ["a", "b", "c"]


async def test_rerun_after_failure_does_not_double_apply_completed_steps():
    """A step that fails leaves earlier completed steps intact; the
    re-run skips them and only retries from the failing step."""
    flag = [True]  # the failing step will raise on first attempt
    order: list[str] = []
    a = _RecordingStep("a")
    b = _FailingStep(flag)
    c = _RecordingStep("c")
    wf = Workflow("crash", [a, b, c])
    ctx = WorkflowContext(actor_id="a1")

    # First run: a completes, b raises, c never runs.
    with pytest.raises(WorkflowError, match="intentional failure"):
        await wf.run(ctx)
    assert ctx.data["order"] == ["a"]

    # Simulate the root-cause fix: clear the flag, re-run.
    flag[0] = False
    await wf.run(ctx)
    # a was skipped (already complete), b & c ran.
    assert ctx.data["order"] == ["a", "fail", "c"]


# ── pre_check gating ───────────────────────────────────────────


async def test_pre_check_runs_before_execute_and_can_block():
    """A step's ``pre_check`` runs before ``execute`` and may raise."""
    precondition = [False]
    wf = Workflow("gate", [_PreCheckStep(precondition)])
    ctx = WorkflowContext(actor_id="a1")
    with pytest.raises(WorkflowError, match="precondition unmet"):
        await wf.run(ctx)
    assert ctx.data.get("prechecked") is not True

    # Fix the precondition and re-run: now it executes.
    precondition[0] = True
    await wf.run(ctx)
    assert ctx.data["prechecked"] is True


async def test_pre_check_skipped_when_step_already_complete():
    """``pre_check`` is not called when ``is_complete`` is True."""
    called = [False]

    class _Step(Step):
        name = "s"

        async def is_complete(self, ctx: WorkflowContext) -> bool:
            return True

        async def pre_check(self, ctx: WorkflowContext) -> None:
            called[0] = True

        async def execute(self, ctx: WorkflowContext) -> None:
            raise AssertionError("execute must not run when is_complete")

    wf = Workflow("skip", [_Step()])
    await wf.run(WorkflowContext(actor_id="a1"))
    assert called[0] is False


# ── step isolation ─────────────────────────────────────────────


async def test_steps_share_context_data():
    """Steps pass intermediate results through the shared context."""
    class _Producer(Step):
        name = "producer"

        async def is_complete(self, ctx: WorkflowContext) -> bool:
            return "value" in ctx.data

        async def execute(self, ctx: WorkflowContext) -> None:
            ctx.data["value"] = 42

    class _Consumer(Step):
        name = "consumer"

        async def is_complete(self, ctx: WorkflowContext) -> bool:
            return ctx.data.get("doubled") is not None

        async def execute(self, ctx: WorkflowContext) -> None:
            ctx.data["doubled"] = ctx.data["value"] * 2

    wf = Workflow("pipeline", [_Producer(), _Consumer()])
    ctx = WorkflowContext(actor_id="a1")
    await wf.run(ctx)
    assert ctx.data["value"] == 42
    assert ctx.data["doubled"] == 84


async def test_concurrent_workflows_have_isolated_contexts():
    """Two workflows running concurrently do not share context state."""
    wf = Workflow("iso", [_RecordingStep("a")])

    async def _run(tag: str) -> str:
        ctx = WorkflowContext(actor_id=tag)
        await wf.run(ctx)
        return ctx.actor_id

    tags = await asyncio.gather(*[_run(f"a{i}") for i in range(8)])
    assert sorted(tags) == [f"a{i}" for i in range(8)]
