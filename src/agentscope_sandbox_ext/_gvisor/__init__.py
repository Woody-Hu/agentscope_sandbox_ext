# -*- coding: utf-8 -*-
"""gVisor (runsc) sandbox backend.

Reuses :class:`agentscope.workspace.DockerWorkspace` verbatim — gVisor
is a Docker *runtime*, so the only change is the ``HostConfig.Runtime``
field set at container-creation time.  The image build, bind-mount,
gateway bootstrap and teardown flows are inherited unchanged.

Requires:
- Docker daemon with the ``runsc`` runtime registered
  (``/etc/docker/daemon.json`` → ``"runtimes": {"runsc": {...}}``).
- See https://gvisor.dev/docs/user_guide/quick_start/docker/
"""

from ._constants import (
    DEFAULT_GATEWAY_PORT,
    GVISOR_RUNTIME_NAME,
)
from ._manager import GVisorWorkspaceManager
from ._workspace import GVisorWorkspace

__all__ = [
    "GVisorWorkspace",
    "GVisorWorkspaceManager",
    "DEFAULT_GATEWAY_PORT",
    "GVISOR_RUNTIME_NAME",
]
