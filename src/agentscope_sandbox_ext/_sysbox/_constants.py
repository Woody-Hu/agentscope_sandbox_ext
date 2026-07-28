# -*- coding: utf-8 -*-
"""Constants for the Sysbox workspace backend.

Sysbox reuses the agentscope :class:`DockerWorkspace` image-build and
gateway-bootstrap machinery unchanged; the only configuration that
differs is the Docker runtime name registered with the daemon.
"""

#: Docker runtime name for Sysbox.  The Sysbox installer registers
#: ``sysbox-runc``; older builds used ``sysbox``.  The manager probes
#: the daemon for whichever is present, in this order.
SYSBOX_RUNTIME_CANDIDATES = ("sysbox-runc", "sysbox")

#: Default fallback runtime name used by the workspace when the
#: caller does not supply one.  Matches the first entry in
#: :data:`SYSBOX_RUNTIME_CANDIDATES`.
SYSBOX_DEFAULT_RUNTIME_NAME = "sysbox-runc"

#: Default gateway port inside the container (no host port mapping).
DEFAULT_GATEWAY_PORT = 5600
