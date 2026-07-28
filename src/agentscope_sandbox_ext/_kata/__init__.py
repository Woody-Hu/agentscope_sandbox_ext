# -*- coding: utf-8 -*-
"""Kata Containers sandbox backend.

Reuses :class:`agentscope.workspace.DockerWorkspace` verbatim — Kata
Containers is a Docker *runtime*, so the only change is the
``HostConfig.Runtime`` field set at container-creation time.  The
image build, bind-mount, gateway bootstrap and teardown flows are
inherited unchanged.

Kata runs each container inside a lightweight VM (using Firecracker
or QEMU as the hypervisor), combining VM-grade isolation with the
container ergonomics.

Requires:
- Docker daemon with a Kata runtime registered
  (``/etc/docker/daemon.json`` → ``"runtimes": {"kata-fc": {...}}``).
- Kata Containers installed (see https://katacontainers.io/docs/).
"""

from ._constants import (
    DEFAULT_GATEWAY_PORT,
    KATA_DEFAULT_RUNTIME_NAME,
)
from ._manager import KataWorkspaceManager
from ._workspace import KataWorkspace

__all__ = [
    "KataWorkspace",
    "KataWorkspaceManager",
    "DEFAULT_GATEWAY_PORT",
    "KATA_DEFAULT_RUNTIME_NAME",
]
