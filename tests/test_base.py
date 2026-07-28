# -*- coding: utf-8 -*-
"""Tests for the unified extension base classes and the gVisor / Kata
runtime probes.

The runtime-probe tests exercise the real ``docker info`` CLI: when
Docker is absent the probe really runs ``docker`` (and fails with a
``FileNotFoundError``/``RuntimeError``), and when Docker is present
but the runtime is not registered the probe really parses the JSON
and raises a descriptive error.  No mocking — the production code
path is exercised verbatim.
"""

from __future__ import annotations

import asyncio
import shutil
import sys
from typing import Any

import pytest

from agentscope_sandbox_ext._base import (
    SandboxedWorkspaceExtBase,
    SandboxExtManagerBase,
)
from agentscope_sandbox_ext._gvisor import (
    GVISOR_RUNTIME_NAME,
    GVisorWorkspace,
    GVisorWorkspaceManager,
)
from agentscope_sandbox_ext._kata import (
    KATA_DEFAULT_RUNTIME_NAME,
    KataWorkspace,
    KataWorkspaceManager,
)
from agentscope_sandbox_ext._sysbox import (
    SYSBOX_DEFAULT_RUNTIME_NAME,
    SysboxWorkspace,
    SysboxWorkspaceManager,
)
from agentscope_sandbox_ext._firecracker import (
    FirecrackerWorkspace,
    FirecrackerWorkspaceManager,
)
from agentscope.app.workspace_manager._base import IsolationPolicy


# ── base class surface ────────────────────────────────────────


def test_sandboxed_workspace_ext_base_declares_abstract_verify():
    """``verify_runtime_available`` is declared abstract via
    :func:`abc.abstractmethod` so subclasses are contractually required
    to override it.

    agentscope's own base classes don't use ``ABCMeta``, so the
    decorator is a documentation contract rather than a runtime check
    — we assert the decorator is present so future refactors don't
    silently drop the contract.
    """
    import abc
    raw = SandboxedWorkspaceExtBase.__dict__.get("verify_runtime_available")
    assert raw is not None
    assert getattr(raw, "__isabstractmethod__", False) is True


def test_sandbox_ext_manager_base_can_be_subclassed():
    """``SandboxExtManagerBase`` is a concrete subclass target."""
    class _M(SandboxExtManagerBase):
        backend_kind = "test"
        async def get_workspace(self, user_id, agent_id, session_id, workspace_id=None):
            raise NotImplementedError
        async def close(self, workspace_id):
            pass
        async def close_all(self):
            pass
    m = _M(isolation=IsolationPolicy.PER_AGENT)
    assert m.backend_kind == "test"


def test_sandbox_kind_default_is_ext():
    """The default ``sandbox_kind`` is ``"ext"``."""
    assert SandboxedWorkspaceExtBase.sandbox_kind == "ext"


# ── sandbox_kind discriminators ──────────────────────────────


def test_firecracker_workspace_has_correct_sandbox_kind():
    """``FirecrackerWorkspace.sandbox_kind`` is ``"firecracker"``."""
    assert FirecrackerWorkspace.sandbox_kind == "firecracker"


def test_gvisor_workspace_has_correct_sandbox_kind():
    """``GVisorWorkspace.sandbox_kind`` is ``"gvisor"``."""
    assert GVisorWorkspace.sandbox_kind == "gvisor"


def test_kata_workspace_has_correct_sandbox_kind():
    """``KataWorkspace.sandbox_kind`` is ``"kata"``."""
    assert KataWorkspace.sandbox_kind == "kata"


# ── gVisor / Kata inherit DockerWorkspace ────────────────────


def test_gvisor_workspace_subclasses_docker_workspace():
    """``GVisorWorkspace`` is a subclass of ``DockerWorkspace`` so it
    inherits the full image-build / bind-mount / gateway-bootstrap flow."""
    from agentscope.workspace import DockerWorkspace
    assert issubclass(GVisorWorkspace, DockerWorkspace)


def test_kata_workspace_subclasses_docker_workspace():
    """``KataWorkspace`` is a subclass of ``DockerWorkspace``."""
    from agentscope.workspace import DockerWorkspace
    assert issubclass(KataWorkspace, DockerWorkspace)


def test_gvisor_workspace_is_ext_subclass():
    """``GVisorWorkspace`` is also a ``SandboxedWorkspaceExtBase``."""
    assert issubclass(GVisorWorkspace, SandboxedWorkspaceExtBase)


def test_kata_workspace_is_ext_subclass():
    """``KataWorkspace`` is also a ``SandboxedWorkspaceExtBase``."""
    assert issubclass(KataWorkspace, SandboxedWorkspaceExtBase)


# ── Sysbox inherits DockerWorkspace ────────────────────────────


def test_sysbox_workspace_subclasses_docker_workspace():
    """``SysboxWorkspace`` is a subclass of ``DockerWorkspace`` so it
    inherits the full image-build / bind-mount / gateway-bootstrap flow."""
    from agentscope.workspace import DockerWorkspace
    assert issubclass(SysboxWorkspace, DockerWorkspace)


def test_sysbox_workspace_is_ext_subclass():
    """``SysboxWorkspace`` is also a ``SandboxedWorkspaceExtBase``."""
    assert issubclass(SysboxWorkspace, SandboxedWorkspaceExtBase)


def test_sysbox_workspace_has_correct_sandbox_kind():
    """``SysboxWorkspace.sandbox_kind`` is ``"sysbox"``."""
    assert SysboxWorkspace.sandbox_kind == "sysbox"


# ── gVisor constructor ───────────────────────────────────────


def test_gvisor_workspace_default_runtime_is_runsc():
    """The default runtime for ``GVisorWorkspace`` is ``runsc``."""
    ws = GVisorWorkspace()
    assert ws._runtime == GVISOR_RUNTIME_NAME == "runsc"


def test_gvisor_workspace_accepts_custom_runtime():
    """A custom runtime name can be supplied (for non-standard installs)."""
    ws = GVisorWorkspace(runtime="my-runsc")
    assert ws._runtime == "my-runsc"


def test_gvisor_workspace_inherits_docker_config():
    """``GVisorWorkspace`` inherits the Docker workspace's serialisable
    config (base_image, host_workdir, ...)."""
    ws = GVisorWorkspace(
        base_image="python:3.12-slim",
        host_workdir="/tmp/as-gvisor-test",
        gateway_port=7777,
    )
    assert ws.base_image == "python:3.12-slim"
    assert ws.host_workdir == "/tmp/as-gvisor-test"
    assert ws.gateway_port == 7777


# ── Kata constructor ─────────────────────────────────────────


def test_kata_workspace_default_runtime_is_kata_fc():
    """The default runtime for ``KataWorkspace`` is ``kata-fc``."""
    ws = KataWorkspace()
    assert ws._runtime == KATA_DEFAULT_RUNTIME_NAME == "kata-fc"


def test_kata_workspace_accepts_custom_runtime():
    """A custom runtime name can be supplied."""
    ws = KataWorkspace(runtime="kata-qemu")
    assert ws._runtime == "kata-qemu"


# ── Sysbox constructor ───────────────────────────────────────


def test_sysbox_workspace_default_runtime_is_sysbox_runc():
    """The default runtime for ``SysboxWorkspace`` is ``sysbox-runc``."""
    ws = SysboxWorkspace()
    assert ws._runtime == SYSBOX_DEFAULT_RUNTIME_NAME == "sysbox-runc"


def test_sysbox_workspace_accepts_custom_runtime():
    """A custom runtime name can be supplied (for non-standard installs)."""
    ws = SysboxWorkspace(runtime="sysbox")
    assert ws._runtime == "sysbox"


def test_sysbox_workspace_inherits_docker_config():
    """``SysboxWorkspace`` inherits the Docker workspace's serialisable
    config (base_image, host_workdir, ...)."""
    ws = SysboxWorkspace(
        base_image="python:3.12-slim",
        host_workdir="/tmp/as-sysbox-test",
        gateway_port=7777,
    )
    assert ws.base_image == "python:3.12-slim"
    assert ws.host_workdir == "/tmp/as-sysbox-test"
    assert ws.gateway_port == 7777


# ── manager construction ────────────────────────────────────


def test_gvisor_manager_construction():
    """``GVisorWorkspaceManager`` builds and exposes ``backend_kind``."""
    mgr = GVisorWorkspaceManager(
        basedir="/tmp/as-gvisor-mgr-test",
        isolation=IsolationPolicy.PER_AGENT,
    )
    assert mgr.backend_kind == "gvisor"


def test_kata_manager_construction():
    """``KataWorkspaceManager`` builds and exposes ``backend_kind``."""
    mgr = KataWorkspaceManager(
        basedir="/tmp/as-kata-mgr-test",
        isolation=IsolationPolicy.PER_AGENT,
    )
    assert mgr.backend_kind == "kata"


def test_sysbox_manager_construction():
    """``SysboxWorkspaceManager`` builds and exposes ``backend_kind``."""
    mgr = SysboxWorkspaceManager(
        basedir="/tmp/as-sysbox-mgr-test",
        isolation=IsolationPolicy.PER_AGENT,
    )
    assert mgr.backend_kind == "sysbox"


def test_sysbox_manager_propagates_runtime():
    """The ``runtime`` arg is stored on the manager and threaded into
    workspaces via ``_make_workspace``."""
    mgr = SysboxWorkspaceManager(
        basedir="/tmp/as-sysbox-mgr-test",
        isolation=IsolationPolicy.PER_AGENT,
        runtime="sysbox",
    )
    assert mgr._runtime == "sysbox"


def test_firecracker_manager_construction():
    """``FirecrackerWorkspaceManager`` builds and exposes ``backend_kind``."""
    mgr = FirecrackerWorkspaceManager(
        isolation=IsolationPolicy.PER_AGENT,
    )
    assert mgr.backend_kind == "firecracker"


# ── runtime probes (real docker CLI) ─────────────────────────


_DOCKER_AVAILABLE = shutil.which("docker") is not None


@pytest.mark.skipif(_DOCKER_AVAILABLE, reason="Docker is installed; probe should succeed or detect missing runtime")
async def test_gvisor_verify_runtime_raises_when_docker_missing():
    """When ``docker`` is not on ``$PATH`` the probe raises
    :class:`RuntimeError` (via ``FileNotFoundError``)."""
    with pytest.raises((RuntimeError, FileNotFoundError, OSError)):
        await GVisorWorkspace.verify_runtime_available()


@pytest.mark.skipif(_DOCKER_AVAILABLE, reason="Docker is installed; probe should succeed or detect missing runtime")
async def test_kata_verify_runtime_raises_when_docker_missing():
    """When ``docker`` is not on ``$PATH`` the probe raises
    :class:`RuntimeError`."""
    with pytest.raises((RuntimeError, FileNotFoundError, OSError)):
        await KataWorkspace.verify_runtime_available()


@pytest.mark.skipif(_DOCKER_AVAILABLE, reason="Docker is installed; probe should succeed or detect missing runtime")
async def test_sysbox_verify_runtime_raises_when_docker_missing():
    """When ``docker`` is not on ``$PATH`` the probe raises
    :class:`RuntimeError`."""
    with pytest.raises((RuntimeError, FileNotFoundError, OSError)):
        await SysboxWorkspace.verify_runtime_available()


@pytest.mark.skipif(not _DOCKER_AVAILABLE, reason="Docker not installed; cannot exercise the runtime-list parse path")
async def test_gvisor_verify_runtime_raises_when_runsc_not_registered():
    """When Docker is present but ``runsc`` is not registered the probe
    raises a descriptive :class:`RuntimeError`.

    This exercises the real JSON parse of ``docker info`` output."""
    # In most CI environments runsc is not registered, so this should
    # raise.  If runsc *is* registered (rare in CI), the probe succeeds
    # and the test is skipped via xfail.
    try:
        await GVisorWorkspace.verify_runtime_available()
    except RuntimeError as exc:
        assert "runsc" in str(exc) or "runtime" in str(exc).lower()
    else:
        pytest.skip("runsc is registered on this host; cannot assert failure")


@pytest.mark.skipif(not _DOCKER_AVAILABLE, reason="Docker not installed; cannot exercise the runtime-list parse path")
async def test_kata_verify_runtime_raises_when_no_kata_runtime():
    """When Docker is present but no Kata runtime is registered the
    probe raises a descriptive :class:`RuntimeError`."""
    try:
        await KataWorkspace.verify_runtime_available()
    except RuntimeError as exc:
        assert "kata" in str(exc).lower() or "runtime" in str(exc).lower()
    else:
        pytest.skip("a Kata runtime is registered on this host")


@pytest.mark.skipif(not _DOCKER_AVAILABLE, reason="Docker not installed; cannot exercise the runtime-list parse path")
async def test_sysbox_verify_runtime_raises_when_no_sysbox_runtime():
    """When Docker is present but no Sysbox runtime is registered the
    probe raises a descriptive :class:`RuntimeError`.

    This exercises the real JSON parse of ``docker info`` output and
    the candidate-list probing logic (``sysbox-runc`` then ``sysbox``)."""
    try:
        await SysboxWorkspace.verify_runtime_available()
    except RuntimeError as exc:
        assert "sysbox" in str(exc).lower() or "runtime" in str(exc).lower()
    else:
        pytest.skip("a Sysbox runtime is registered on this host")


# ── Firecracker verify_runtime (real firecracker binary) ────


_FC_AVAILABLE = shutil.which("firecracker") is not None


@pytest.mark.skipif(_FC_AVAILABLE, reason="firecracker is installed")
async def test_firecracker_verify_runtime_raises_when_binary_missing():
    """When ``firecracker`` is not on ``$PATH`` the probe raises
    a :class:`RuntimeError`` (via ``FileNotFoundError``)."""
    with pytest.raises((RuntimeError, FileNotFoundError, OSError)):
        await FirecrackerWorkspace.verify_runtime_available()
