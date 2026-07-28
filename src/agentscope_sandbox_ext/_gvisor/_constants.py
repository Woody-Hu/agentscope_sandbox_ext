# -*- coding: utf-8 -*-
"""Constants for the gVisor (runsc) workspace backend.

gVisor reuses the agentscope :class:`DockerWorkspace` image-build and
gateway-bootstrap machinery unchanged; the only configuration that
differs is the Docker runtime name registered with the daemon.
"""

#: Docker runtime name for gVisor's ``runsc``.  Must match an entry in
#: the daemon's ``/etc/docker/daemon.json`` ``"runtimes"`` map.
GVISOR_RUNTIME_NAME = "runsc"

#: Default gateway port inside the container (no host port mapping).
DEFAULT_GATEWAY_PORT = 5600
