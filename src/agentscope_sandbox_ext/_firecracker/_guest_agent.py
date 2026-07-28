# -*- coding: utf-8 -*-
"""Guest agent source for the Firecracker microVM workspace.

This module does NOT import the guest agent at runtime — it stores the
agent's source as a string constant.  The Firecracker workspace writes
the constant into the VM via the vsock exec protocol's ``bootstrap``
command (which uses the VM's own shell to install the script), or
ships it as part of the rootfs build.

Wire protocol (host -> guest, framed as ``[4-byte big-endian
length][json]``):

* Request ``{"op": "exec", "argv": ["/bin/sh", "-c", "..."],
  "timeout": 30.0}`` → response ``{"ok": true, "exit_code": 0,
  "stdout": "<base64>", "stderr": "<base64>"}``.
* Request ``{"op": "read_file", "path": "/etc/hostname"}`` →
  response ``{"ok": true, "data": "<base64>"}`` or
  ``{"ok": false, "error": "..."}``.
* Request ``{"op": "write_file", "path": "/tmp/foo", "data":
  "<base64>"}`` → response ``{"ok": true}``.
* Request ``{"op": "ping"}`` → response ``{"ok": true,
  "pong": true}``.

The agent is intentionally tiny: stdlib only, no third-party imports,
so it runs on any image that ships ``python3`` (the same contract the
agentscope gateway shim requires).
"""

GUEST_AGENT_SOURCE = r'''#!/usr/bin/env python3
"""Tiny vsock guest agent for the agentscope-sandbox-ext Firecracker backend.

Listens on AF_VSOCK at the configured port and services length-prefixed
JSON requests (4-byte big-endian length header + JSON body).  See the
module docstring in the host-side wrapper for the wire protocol.

The agent is single-threaded and processes one connection at a time.
For the workload it serves (an agentscope workspace gateway driving a
handful of exec/file calls per turn) this is plenty; if a future
workload needs concurrency, swap this for a small thread pool.
"""

import base64
import json
import os
import socket
import struct
import subprocess
import sys


VSOCK_HOST_CID = 2  # the host, reserved by the kernel
DEFAULT_PORT = 1024


def _recv_exactly(conn, n):
    """Read exactly ``n`` bytes from ``conn`` or raise."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed mid-frame")
        buf.extend(chunk)
    return bytes(buf)


def _recv_frame(conn):
    """Read one length-prefixed JSON frame and return the parsed dict."""
    header = _recv_exactly(conn, 4)
    (length,) = struct.unpack(">I", header)
    if length <= 0 or length > 64 * 1024 * 1024:
        raise ValueError(f"frame length out of range: {length}")
    body = _recv_exactly(conn, length)
    return json.loads(body.decode("utf-8"))


def _send_frame(conn, obj):
    """Serialise ``obj`` as JSON and send it with a 4-byte length header."""
    body = json.dumps(obj).encode("utf-8")
    conn.sendall(struct.pack(">I", len(body)) + body)


def _handle_exec(req):
    """Run ``argv`` via subprocess and capture stdout/stderr/exit_code."""
    argv = req.get("argv")
    if not isinstance(argv, list) or not argv:
        return {"ok": False, "error": "missing or invalid 'argv'"}
    timeout = req.get("timeout")
    cwd = req.get("cwd")
    try:
        proc = subprocess.run(
            argv,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        return {
            "ok": True,
            "exit_code": 127,
            "stdout": b"",
            "stderr": str(exc).encode("utf-8"),
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": True,
            "exit_code": -1,
            "stdout": exc.stdout or b"",
            "stderr": (exc.stderr or b"") + b"\ntimed out",
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": f"exec failed: {exc!r}",
        }
    return {
        "ok": True,
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _handle_read_file(req):
    """Read a file's raw bytes."""
    path = req.get("path")
    if not isinstance(path, str):
        return {"ok": False, "error": "missing 'path'"}
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return {"ok": False, "error": "FileNotFoundError"}
    except OSError as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": True, "data": base64.b64encode(data).decode("ascii")}


def _handle_write_file(req):
    """Write raw bytes to a file, creating parent directories."""
    path = req.get("path")
    data_b64 = req.get("data")
    if not isinstance(path, str) or not isinstance(data_b64, str):
        return {"ok": False, "error": "missing 'path' or 'data'"}
    try:
        data = base64.b64decode(data_b64)
    except Exception as exc:
        return {"ok": False, "error": f"bad base64: {exc}"}
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    try:
        with open(path, "wb") as f:
            f.write(data)
    except OSError as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": True}


def _handle_ping(req):
    """Health-check probe."""
    return {"ok": True, "pong": True, "pid": os.getpid()}


HANDLERS = {
    "exec": _handle_exec,
    "read_file": _handle_read_file,
    "write_file": _handle_write_file,
    "ping": _handle_ping,
}


def _serve_connection(conn):
    """Serve one client connection until it closes or errors."""
    try:
        while True:
            try:
                req = _recv_frame(conn)
            except (EOFError, ConnectionResetError):
                return
            op = req.get("op") if isinstance(req, dict) else None
            handler = HANDLERS.get(op)
            if handler is None:
                _send_frame(conn, {"ok": False, "error": f"unknown op: {op!r}"})
                continue
            try:
                resp = handler(req)
            except Exception as exc:
                resp = {"ok": False, "error": f"handler raised: {exc!r}"}
            # ``exec`` / ``read_file`` return raw bytes — base64-encode
            # them so the JSON frame is wire-safe.
            if isinstance(resp, dict):
                for k in ("stdout", "stderr", "data"):
                    v = resp.get(k)
                    if isinstance(v, (bytes, bytearray)):
                        resp[k] = base64.b64encode(v).decode("ascii")
            _send_frame(conn, resp)
    finally:
        try:
            conn.close()
        except OSError:
            pass


def main():
    """Listen on AF_VSOCK and serve connections one at a time."""
    port = DEFAULT_PORT
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            sys.stderr.write(f"invalid port: {sys.argv[1]!r}\n")
            sys.exit(2)

    # AF_VSOCK is available on Linux 4.8+ when CONFIG_VHOST_VSOCK is
    # built into the host kernel; the guest kernel needs CONFIG_VIRTIO_VSOCKETS.
    try:
        srv = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    except (AttributeError, OSError) as exc:
        sys.stderr.write(
            f"AF_VSOCK unavailable on this kernel: {exc!r}\n"
        )
        sys.exit(1)
    srv.bind((VSOCK_HOST_CID, port))
    srv.listen(8)
    sys.stderr.write(f"guest-agent listening on vsock:{port}\n")
    sys.stderr.flush()
    while True:
        conn, _ = srv.accept()
        _serve_connection(conn)


if __name__ == "__main__":
    main()
'''


def get_guest_agent_bytes() -> bytes:
    """Return the guest-agent source as raw bytes.

    Used by the Firecracker workspace to ship the agent into the VM
    via the vsock exec protocol's ``write_file`` op (the first thing
    the host does after the VM boots is upload this script, then exec
    it via ``python3 <path>`` so it becomes the persistent agent).
    """
    return GUEST_AGENT_SOURCE.encode("utf-8")
