# -*- coding: utf-8 -*-
"""Tests for the Firecracker REST API client.

Drives :class:`FirecrackerApi` against a **real** HTTP server bound
to a Unix-domain socket — the same transport the production client
uses to talk to the ``firecracker`` process.  The server is a tiny
``asyncio.start_server`` loop that parses HTTP/1.1 requests and emits
hand-crafted responses, including the exact error envelope shape
Firecracker returns (``{"fault_message": "..."}``).

No ``unittest.mock`` is used anywhere — every byte on the wire is a
real byte.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import tempfile
from pathlib import Path

import pytest

from agentscope_sandbox_ext._firecracker._api import (
    BootSource,
    Drive,
    FirecrackerApi,
    FirecrackerApiError,
    MachineConfig,
    NetworkInterface,
    Vsock,
    wait_for_socket,
)


# ── real Unix-socket HTTP server ────────────────────────────────


class FakeFirecrackerServer:
    """A minimal HTTP/1.1 server speaking the Firecracker API shape.

    Records every request (method + path + body) so tests can assert
    on what the client sent, and responds with a canned status/body
    pair chosen by the test via :meth:`set_response`.

    Runs on a real ``asyncio.start_server`` Unix socket — the same
    transport Firecracker itself uses.
    """

    def __init__(self) -> None:
        self._tmpdir = tempfile.mkdtemp(prefix="fc-api-test-")
        self._socket_path = os.path.join(self._tmpdir, "api.sock")
        self._server: asyncio.AbstractServer | None = None
        self._requests: list[tuple[str, str, bytes]] = []
        self._responses: dict[tuple[str, str], tuple[int, bytes]] = {}
        # Default: empty 204 for any unmatched route.
        self._default_response: tuple[int, bytes] = (204, b"")

    @property
    def socket_path(self) -> str:
        return self._socket_path

    @property
    def requests(self) -> list[tuple[str, str, bytes]]:
        """Recorded (method, path, body) tuples in arrival order."""
        return list(self._requests)

    def set_response(
        self,
        method: str,
        path: str,
        *,
        status: int,
        body: bytes | str | dict,
    ) -> None:
        """Canned response for ``METHOD path``.

        ``body`` may be ``bytes`` (sent verbatim), ``str`` (encoded
        utf-8), or ``dict`` (JSON-encoded).
        """
        if isinstance(body, dict):
            raw = json.dumps(body).encode("utf-8")
        elif isinstance(body, str):
            raw = body.encode("utf-8")
        else:
            raw = body
        self._responses[(method.upper(), path)] = (status, raw)

    def set_default(self, *, status: int, body: bytes = b"") -> None:
        self._default_response = (status, body)

    async def start(self) -> None:
        self._server = await asyncio.start_unix_server(
            self._handle_conn,
            path=self._socket_path,
        )

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        try:
            os.unlink(self._socket_path)
        except FileNotFoundError:
            pass
        os.rmdir(self._tmpdir)

    async def _handle_conn(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            await self._serve_one(reader, writer)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _serve_one(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        # Parse request line + headers (minimal HTTP/1.1).
        request_line = await reader.readuntil(b"\r\n")
        method, path, _ = (
            request_line.decode("ascii").strip().split(" ", 2)
        )
        headers: dict[str, str] = {}
        while True:
            line = await reader.readuntil(b"\r\n")
            if line == b"\r\n":
                break
            name, _, value = line.decode("ascii").partition(":")
            headers[name.strip().lower()] = value.strip()

        body = b""
        cl = headers.get("content-length")
        if cl:
            body = await reader.readexactly(int(cl))

        self._requests.append((method.upper(), path, body))

        status, payload = self._responses.get(
            (method.upper(), path),
            self._default_response,
        )
        reason = {200: "OK", 204: "No Content", 400: "Bad Request"}.get(
            status, "OK",
        )
        header_block = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("ascii")
        writer.write(header_block + payload)
        await writer.drain()


@pytest.fixture
async def fc_server():
    """Start a real :class:`FakeFirecrackerServer`."""
    srv = FakeFirecrackerServer()
    await srv.start()
    yield srv
    await srv.stop()


@pytest.fixture
async def api(fc_server: FakeFirecrackerServer):
    """A :class:`FirecrackerApi` pointed at the fake server."""
    client = FirecrackerApi(fc_server.socket_path, request_timeout=3.0)
    yield client
    await client.aclose()


# ── dataclass serialisation ─────────────────────────────────────


def test_machine_config_to_json_minimal():
    """``MachineConfig.to_json`` omits ``cpu_template`` when ``None``."""
    cfg = MachineConfig(vcpu_count=2, mem_size_mib=512)
    body = cfg.to_json()
    assert body["vcpu_count"] == 2
    assert body["mem_size_mib"] == 512
    assert body["smt"] is False
    assert "cpu_template" not in body
    assert body["track_dirty_pages"] is False


def test_machine_config_to_json_with_cpu_template():
    """``cpu_template`` is included when set."""
    cfg = MachineConfig(
        vcpu_count=4,
        mem_size_mib=2048,
        smt=True,
        cpu_template="T2",
        track_dirty_pages=True,
    )
    body = cfg.to_json()
    assert body["cpu_template"] == "T2"
    assert body["smt"] is True
    assert body["track_dirty_pages"] is True


def test_boot_source_to_json():
    """``BootSource.to_json`` includes kernel + boot_args."""
    src = BootSource(kernel_image_path="/k", boot_args="console=ttyS0")
    body = src.to_json()
    assert body["kernel_image_path"] == "/k"
    assert body["boot_args"] == "console=ttyS0"
    assert "initrd_path" not in body


def test_boot_source_to_json_with_initrd():
    """``initrd_path`` is included when set."""
    src = BootSource(
        kernel_image_path="/k",
        initrd_path="/initrd",
    )
    assert src.to_json()["initrd_path"] == "/initrd"


def test_drive_to_json():
    """``Drive.to_json`` round-trips all fields."""
    drv = Drive(
        drive_id="rootfs",
        path_on_host="/r.ext4",
        is_root_device=True,
        is_read_only=False,
        cache_type="Writeback",
    )
    body = drv.to_json()
    assert body == {
        "drive_id": "rootfs",
        "path_on_host": "/r.ext4",
        "is_root_device": True,
        "is_read_only": False,
        "cache_type": "Writeback",
    }


def test_network_interface_to_json_without_mac():
    """``guest_mac`` is omitted when ``None``."""
    iface = NetworkInterface(iface_id="eth0", host_dev_name="tap0")
    body = iface.to_json()
    assert body == {"iface_id": "eth0", "host_dev_name": "tap0"}
    assert "guest_mac" not in body


def test_network_interface_to_json_with_mac():
    """``guest_mac`` is included when set."""
    iface = NetworkInterface(
        iface_id="eth0",
        host_dev_name="tap0",
        guest_mac="AA:BB:CC:DD:EE:FF",
    )
    assert iface.to_json()["guest_mac"] == "AA:BB:CC:DD:EE:FF"


def test_vsock_to_json():
    """``Vsock.to_json`` round-trips all fields."""
    vs = Vsock(vsock_id="v0", guest_cid=3, uds_path="/tmp/v.sock")
    assert vs.to_json() == {
        "vsock_id": "v0",
        "guest_cid": 3,
        "uds_path": "/tmp/v.sock",
    }


# ── HTTP round-trips against the real server ───────────────────


async def test_put_boot_source_sends_put_with_json_body(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``put_boot_source`` sends ``PUT /boot-source`` with the JSON body."""
    await api.put_boot_source(
        BootSource(kernel_image_path="/vmlinux", boot_args="console=ttyS0"),
    )
    method, path, body = fc_server.requests[-1]
    assert method == "PUT"
    assert path == "/boot-source"
    parsed = json.loads(body)
    assert parsed["kernel_image_path"] == "/vmlinux"
    assert parsed["boot_args"] == "console=ttyS0"


async def test_put_machine_config_sends_expected_body(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``put_machine_config`` forwards vCPU / mem in the JSON body."""
    await api.put_machine_config(
        MachineConfig(vcpu_count=2, mem_size_mib=1024),
    )
    _, _, body = fc_server.requests[-1]
    parsed = json.loads(body)
    assert parsed["vcpu_count"] == 2
    assert parsed["mem_size_mib"] == 1024


async def test_put_drive_uses_drive_id_in_path(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``put_drive`` interpolates the drive id into the URL path."""
    await api.put_drive(
        Drive(drive_id="rootfs", path_on_host="/r.ext4", is_root_device=True),
    )
    method, path, _ = fc_server.requests[-1]
    assert method == "PUT"
    assert path == "/drives/rootfs"


async def test_put_vsock_sends_correct_path_and_body(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``put_vsock`` sends ``PUT /vsock`` with the right body."""
    await api.put_vsock(Vsock(vsock_id="v0", guest_cid=5, uds_path="/v.sock"))
    method, path, body = fc_server.requests[-1]
    assert method == "PUT"
    assert path == "/vsock"
    parsed = json.loads(body)
    assert parsed["guest_cid"] == 5


async def test_start_instance_sends_instance_start_action(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``start_instance`` sends the ``InstanceStart`` action."""
    await api.start_instance()
    _, _, body = fc_server.requests[-1]
    assert json.loads(body) == {"action_type": "InstanceStart"}


async def test_send_ctrl_alt_del_sends_correct_action(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``send_ctrl_alt_del`` sends the ``SendCtrlAltDel`` action."""
    await api.send_ctrl_alt_del()
    _, _, body = fc_server.requests[-1]
    assert json.loads(body) == {"action_type": "SendCtrlAltDel"}


async def test_describe_parses_instance_info(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``describe`` parses the ``GET /`` response."""
    fc_server.set_response(
        "GET",
        "/",
        status=200,
        body={"state": "Running", "vmm_version": "1.5.0"},
    )
    info = await api.describe()
    assert info.state == "Running"
    assert info.vmm_version == "1.5.0"


async def test_get_vm_state_returns_state_string(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``get_vm_state`` returns the ``state`` string from ``GET /vm``."""
    fc_server.set_response(
        "GET",
        "/vm",
        status=200,
        body={"state": "Paused"},
    )
    assert await api.get_vm_state() == "Paused"


async def test_get_vm_config_returns_full_dict(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """``get_vm_config`` returns the full config dict."""
    cfg = {"vcpu_count": 4, "mem_size_mib": 2048}
    fc_server.set_response("GET", "/vm/config", status=200, body=cfg)
    assert await api.get_vm_config() == cfg


# ── error handling ─────────────────────────────────────────────


async def test_non_2xx_raises_api_error_with_fault_message(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """A 400 with a ``fault_message`` body raises :class:`FirecrackerApiError`
    exposing the fault message."""
    fc_server.set_response(
        "PUT",
        "/boot-source",
        status=400,
        body={"fault_message": "kernel image not found"},
    )
    with pytest.raises(FirecrackerApiError) as exc_info:
        await api.put_boot_source(BootSource(kernel_image_path="/x"))
    assert exc_info.value.status_code == 400
    assert "kernel image not found" in exc_info.value.fault_message


async def test_non_2xx_with_no_body_still_raises(
    api: FirecrackerApi,
    fc_server: FakeFirecrackerServer,
):
    """A 500 with no body raises :class:`FirecrackerApiError` anyway."""
    fc_server.set_response(
        "PUT",
        "/machine-config",
        status=500,
        body=b"",
    )
    fc_server.set_default(status=500, body=b"")
    with pytest.raises(FirecrackerApiError):
        await api.put_machine_config(MachineConfig(vcpu_count=1, mem_size_mib=128))


# ── wait_for_socket ─────────────────────────────────────────────


async def test_wait_for_socket_succeeds_when_connectable(tmp_path):
    """``wait_for_socket`` returns immediately when the socket accepts."""
    srv = await asyncio.start_unix_server(
        lambda r, w: None,
        path=str(tmp_path / "ready.sock"),
    )
    try:
        await asyncio.wait_for(
            wait_for_socket(str(tmp_path / "ready.sock"), timeout=1.0),
            timeout=2.0,
        )
    finally:
        srv.close()
        await srv.wait_closed()


async def test_wait_for_socket_times_out_when_never_ready(tmp_path):
    """``wait_for_socket`` raises :class:`TimeoutError` when the socket
    never appears."""
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(
            wait_for_socket(
                str(tmp_path / "never.sock"),
                timeout=0.3,
            ),
            timeout=2.0,
        )
