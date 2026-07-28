# -*- coding: utf-8 -*-
"""Shared pytest fixtures for agentscope-sandbox-ext tests.

Tests are written to exercise **real** behaviour — no mocking.  The
Firecracker guest-agent protocol peer runs on a Unix-domain socket
(AF_VSOCK is not available in CI), but the handler functions are the
*exact same code* that ships into the microVM, exec'd from the
``GUEST_AGENT_SOURCE`` constant.  This is not a mock: real
subprocesses are spawned, real files are read/written, real frames
are exchanged over the wire.
"""

from __future__ import annotations

import importlib.util
import os
import socket
import sys
import tempfile
import threading
from pathlib import Path

import pytest

# Make ``tests/_helpers`` importable as a package.
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def tmp_workspace_dir(tmp_path: Path) -> Path:
    """A fresh temporary directory for test artefacts."""
    return tmp_path
