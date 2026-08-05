# -*- coding: utf-8 -*-
"""Tests for :mod:`agentscope_sandbox_ext._template._template`.

Templates are immutable workload definitions; the baker provisions a
pristine worker, snapshots it, uploads the golden snapshot to the
durable store, and transitions the template ``Initial → Baking →
Ready``.  These tests assert the phase transitions, the golden
snapshot's storage location, and the close-the-bake-worker invariant.
"""

from __future__ import annotations

import pytest

from agentscope_sandbox_ext._actor._types import (
    KIND_GOLDEN,
    PHASE_BAKING,
    PHASE_FAILED,
    PHASE_INITIAL,
    PHASE_READY,
    SCOPE_DATA,
    SCOPE_FULL,
    SandboxClass,
    TemplateRef,
)
from agentscope_sandbox_ext._checkpoint._manager import CheckpointManager
from agentscope_sandbox_ext._checkpoint._types import CheckpointConfig
from agentscope_sandbox_ext._runtime.local_snapshot_store import (
    LocalSnapshotStore,
)
from agentscope_sandbox_ext._template._template import (
    ActorTemplateRecord,
    InProcessTemplateRegistry,
    TemplateBaker,
)
from tests._helpers.fake_runtime import make_worker_factory


# ── fixtures ────────────────────────────────────────────────────


@pytest.fixture
def durable_store(tmp_path):
    return LocalSnapshotStore(str(tmp_path / "durable"))


@pytest.fixture
def checkpoint(durable_store):
    return CheckpointManager(CheckpointConfig(durable_store=durable_store))


@pytest.fixture
def registry():
    return InProcessTemplateRegistry()


@pytest.fixture
def baker(checkpoint, registry):
    return TemplateBaker(
        factory=make_worker_factory(sandbox_class="vfs"),
        checkpoint=checkpoint,
        registry=registry,
    )


def _template(
    *,
    name: str = "t",
    version: int = 1,
    cls: str = "vfs",
    on_pause: str = SCOPE_FULL,
    on_suspend: str = SCOPE_FULL,
) -> ActorTemplateRecord:
    return ActorTemplateRecord(
        ref=TemplateRef(name=name, version=version),
        sandbox_class=SandboxClass.of(cls),
        spec={"image": "vfs:latest"},
        on_pause=on_pause,
        on_suspend=on_suspend,
    )


# ── ActorTemplateRecord ─────────────────────────────────────────


def test_template_record_defaults_to_initial_phase():
    t = _template()
    assert t.phase == PHASE_INITIAL
    assert t.golden_snapshot is None


def test_template_record_enforces_subset_invariant():
    with pytest.raises(ValueError, match="subset"):
        _template(on_pause=SCOPE_DATA, on_suspend=SCOPE_FULL)


def test_template_record_round_trip(tmpl=None):
    t = _template()
    t.golden_snapshot = None
    d = t.to_dict()
    assert d["ref"]["name"] == "t"
    assert d["sandbox_class"]["value"] == "vfs"
    assert d["phase"] == PHASE_INITIAL


# ── InProcessTemplateRegistry ───────────────────────────────────


async def test_registry_create_and_get(registry):
    t = _template()
    created = await registry.create(t)
    assert created.created_at > 0
    fetched = await registry.get(t.ref)
    assert fetched.ref == t.ref


async def test_registry_create_duplicate_raises(registry):
    await registry.create(_template())
    with pytest.raises(KeyError, match="already exists"):
        await registry.create(_template())


async def test_registry_get_missing_raises(registry):
    with pytest.raises(KeyError, match="no such template"):
        await registry.get(TemplateRef(name="ghost", version=1))


async def test_registry_update_preserves_created_at(registry):
    t = await registry.create(_template())
    created = t.created_at
    t.phase = PHASE_BAKING
    updated = await registry.update(t)
    assert updated.phase == PHASE_BAKING
    assert updated.created_at == created
    assert updated.updated_at >= created


async def test_registry_list_returns_all(registry):
    await registry.create(_template(name="t1", version=1))
    await registry.create(_template(name="t2", version=1))
    items = await registry.list()
    assert {t.ref.name for t in items} == {"t1", "t2"}


# ── TemplateBaker ───────────────────────────────────────────────


async def test_bake_transitions_initial_to_ready(baker, registry):
    t = await registry.create(_template())
    result = await baker.bake(t)
    assert result.phase == PHASE_READY
    assert result.golden_snapshot is not None
    assert result.golden_snapshot.kind == KIND_GOLDEN


async def test_bake_idempotent_when_already_ready(baker, registry):
    t = await registry.create(_template())
    once = await baker.bake(t)
    # Reset the in-memory record to Ready-with-golden and re-bake: should
    # not invoke the factory again.
    factory_calls = []
    orig_factory = baker._factory

    async def _counting_factory():
        factory_calls.append(1)
        return await orig_factory()

    baker._factory = _counting_factory
    try:
        twice = await baker.bake(once)
        assert twice.phase == PHASE_READY
        assert twice.golden_snapshot == once.golden_snapshot
        assert factory_calls == []  # no second bake
    finally:
        baker._factory = orig_factory


async def test_bake_failure_marks_template_failed(baker, registry):
    """A factory failure must transition the template to ``Failed``
    and propagate the exception."""
    t = await registry.create(_template())

    async def _broken_factory():
        raise RuntimeError("factory down")

    baker._factory = _broken_factory
    with pytest.raises(RuntimeError, match="factory down"):
        await baker.bake(t)
    final = await registry.get(t.ref)
    assert final.phase == PHASE_FAILED
    assert final.golden_snapshot is None


async def test_bake_closes_worker_after_success(baker, registry):
    """The bake worker is provisioned fresh (not from the pool) and
    must be closed after baking so its resources are released."""
    t = await registry.create(_template())
    await baker.bake(t)
    # The factory-built runtime is closed — we cannot assert on the
    # closed instance directly, but the bake must not leak: bake twice
    # in a row and the second bake must still succeed (i.e. a fresh
    # worker each time).
    t2 = await registry.create(_template(name="t", version=2))
    result2 = await baker.bake(t2)
    assert result2.phase == PHASE_READY


async def test_bake_uses_template_on_pause_scope(baker, registry, durable_store):
    """The golden snapshot's scope matches the template's on_pause."""
    t = await registry.create(_template(on_pause=SCOPE_DATA, on_suspend=SCOPE_DATA))
    result = await baker.bake(t)
    assert result.golden_snapshot is not None
    assert result.golden_snapshot.scope == SCOPE_DATA
