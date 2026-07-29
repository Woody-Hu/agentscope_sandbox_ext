# -*- coding: utf-8 -*-
"""Virtual File System (VFS) workspace backends.

A VFS backend translates the :class:`BackendBase` primitives
(``exec_shell`` / ``read_file`` / ``write_file``) into operations
against a virtual workspace, without ever spawning a real container
or microVM.  This makes VFS backends:

* **Zero-startup** — provisioning is microsecond-cheap, with no image
  build, no container start, no guest agent bootstrap.
* **No host runtime** — works anywhere Python runs; perfect for CI
  and development.
* **A natural benchmark baseline** — VFS represents the "theoretical
  upper bound" of how fast a workspace can be when there is nothing
  to isolate.

The reference implementation, :class:`AgentFSBackend`, translates
the primitives to host I/O + subprocess confined to a per-workspace
host directory.
"""

from ._agentfs import AgentFSBackend, AgentFSWorkspace
from ._base import VFSBackendBase, VFSWorkspaceBase

__all__ = [
    "VFSBackendBase",
    "VFSWorkspaceBase",
    "AgentFSBackend",
    "AgentFSWorkspace",
]
