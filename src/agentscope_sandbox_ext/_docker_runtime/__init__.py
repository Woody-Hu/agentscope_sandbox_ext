# -*- coding: utf-8 -*-
"""Shared base for Docker-runtime-backed workspace managers.

gVisor (``runsc``) and Kata Containers are both Docker *runtimes* —
they reuse the entire :class:`agentscope.workspace.DockerWorkspace`
image-build / bind-mount / gateway-bootstrap flow and only differ in
the ``HostConfig.Runtime`` name.  Their managers are therefore
structurally identical; this package factors the shared cache +
TTL-sweeper + optional pool logic into a single base class.
"""

from ._base_manager import DockerRuntimeWorkspaceManagerBase

__all__ = ["DockerRuntimeWorkspaceManagerBase"]
