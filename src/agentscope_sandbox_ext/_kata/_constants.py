# -*- coding: utf-8 -*-
"""Constants for the Kata Containers workspace backend.

Kata reuses the agentscope :class:`DockerWorkspace` image-build and
gateway-bootstrap machinery unchanged; the only configuration that
differs is the Docker runtime name registered with the daemon.
"""

#: Default Docker runtime name for Kata Containers.  Most installs
#: register ``kata-fc`` (Firecracker hypervisor) or ``kata-qemu``
#: (QEMU hypervisor); ``kata-runtime`` is the legacy alias.  The
#: manager probes the daemon for whichever is present, in this order.
KATA_RUNTIME_CANDIDATES = ("kata-fc", "kata-qemu", "kata-runtime", "kata")

#: Default fallback runtime name used by the workspace when the
#: caller does not supply one.  Matches the first entry in
#: :data:`KATA_RUNTIME_CANDIDATES`.
KATA_DEFAULT_RUNTIME_NAME = "kata-fc"

#: Default gateway port inside the container (no host port mapping).
DEFAULT_GATEWAY_PORT = 5600
