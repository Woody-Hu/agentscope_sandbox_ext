# -*- coding: utf-8 -*-
"""Firecracker microVM sandbox backend.

Spawns a Firecracker microVM per workspace, drives the Firecracker
REST API over a Unix-domain socket, and bridges host-side
:class:`BackendBase` calls into the VM via a tiny stdlib-only guest
agent reachable over virtio-vsock.

Requires:
- Linux host with ``/dev/kvm`` accessible.
- ``firecracker`` binary on ``$PATH``.
- A kernel image and ext4 rootfs (see ``docs/firecracker.md``).
"""

from ._api import FirecrackerApi, FirecrackerApiError, FirecrackerProcessHandle
from ._backend import FirecrackerBackend, GuestAgentClient, GuestAgentError
from ._constants import (
    DEFAULT_FIRECRACKER_BIN,
    DEFAULT_GATEWAY_PORT,
    DEFAULT_GUEST_AGENT_PORT,
    DEFAULT_GUEST_CID,
    DEFAULT_KERNEL_PATH,
    DEFAULT_MEM_SIZE_MIB,
    DEFAULT_ROOTFS_PATH,
    DEFAULT_VCPU_COUNT,
    GATEWAY_HOME,
)
from ._manager import FirecrackerWorkspaceManager
from ._workspace import FirecrackerWorkspace

__all__ = [
    "FirecrackerApi",
    "FirecrackerApiError",
    "FirecrackerBackend",
    "FirecrackerProcessHandle",
    "FirecrackerWorkspace",
    "FirecrackerWorkspaceManager",
    "GuestAgentClient",
    "GuestAgentError",
    "DEFAULT_FIRECRACKER_BIN",
    "DEFAULT_GATEWAY_PORT",
    "DEFAULT_GUEST_AGENT_PORT",
    "DEFAULT_GUEST_CID",
    "DEFAULT_KERNEL_PATH",
    "DEFAULT_MEM_SIZE_MIB",
    "DEFAULT_ROOTFS_PATH",
    "DEFAULT_VCPU_COUNT",
    "GATEWAY_HOME",
]
