# -*- coding: utf-8 -*-
"""Sysbox sandbox backend.

Reuses :class:`agentscope.workspace.DockerWorkspace` verbatim — Sysbox
(https://github.com/nestybox/sysbox) is a Docker *runtime* that gives
each container its own user + mount namespace, virtualising ``/proc``
and ``/sys`` and enabling Docker-in-Docker without ``--privileged``.
The only change vs the default ``runc`` runtime is the
``HostConfig.Runtime`` field set at container-creation time.  Image
build, bind-mount, gateway bootstrap and teardown flows are inherited
unchanged.

Requires:
- Docker daemon with the ``sysbox-runc`` runtime registered
  (``/etc/docker/daemon.json`` → ``"runtimes": {"sysbox-runc": {...}}``).
- Sysbox installed (see https://github.com/nestybox/sysbox/blob/master/docs/user-guide/install-package.md).
"""

from ._constants import (
    DEFAULT_GATEWAY_PORT,
    SYSBOX_DEFAULT_RUNTIME_NAME,
    SYSBOX_RUNTIME_CANDIDATES,
)
from ._manager import SysboxWorkspaceManager
from ._workspace import SysboxWorkspace

__all__ = [
    "SysboxWorkspace",
    "SysboxWorkspaceManager",
    "DEFAULT_GATEWAY_PORT",
    "SYSBOX_DEFAULT_RUNTIME_NAME",
    "SYSBOX_RUNTIME_CANDIDATES",
]
