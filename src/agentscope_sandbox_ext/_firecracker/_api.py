# -*- coding: utf-8 -*-
"""Async client for the Firecracker REST API over a Unix-domain socket.

Firecracker exposes a synchronous HTTP/1.1 server on a Unix socket
path supplied via ``--api-sock``.  Mutations return ``204 No Content``
on success; errors carry a JSON body with a ``fault_message`` field.

This module is intentionally free of any agentscope dependency — it
talks raw HTTP over the socket so it can be unit-tested in isolation
against a real HTTP server bound to a Unix socket pair.  The workspace
backend in :mod:`._workspace` composes this client with the
:class:`SandboxedWorkspaceExtBase` lifecycle.

Endpoints covered (per ``src/firecracker/swagger/firecracker.yaml``):

* ``GET /``                — instance info
* ``GET /vm``              — VM state (``Running`` / ``Paused``)
* ``PATCH /vm``            — pause / resume
* ``GET /vm/config``       — full machine configuration
* ``PUT /boot-source``     — kernel + initrd + kernel cmdline
* ``PUT /machine-config``  — vCPU / mem / SMT / cpu_template
* ``PUT /drives/{id}``     — attach a block device
* ``PATCH /drives/{id}``   — notify host file changed (post-boot)
* ``PUT /network-interfaces/{id}`` — attach a virtio-net (TAP-backed)
* ``PUT /vsock``           — attach a virtio-vsock device
* ``PUT /actions``         — InstanceStart / SendCtrlAltDel / FlushMetrics
* ``PUT /logger``          — configure log file / level
"""

from __future__ import annotations

import asyncio
import os
import socket
from dataclasses import dataclass, field
from typing import Any

import httpx

from .._firecracker._constants import (
    DEFAULT_API_READY_TIMEOUT,
    DEFAULT_BOOT_ARGS,
)


class FirecrackerApiError(RuntimeError):
    """Raised when the Firecracker API returns a non-2xx response.

    Attributes:
        status_code (`int`):
            HTTP status code returned by Firecracker.
        fault_message (`str`):
            Parsed ``fault_message`` field from the JSON body, or the
            raw response text when no body was returned.
    """

    def __init__(self, status_code: int, fault_message: str) -> None:
        """Capture status and message, build a friendly ``str``."""
        super().__init__(
            f"Firecracker API error: HTTP {status_code}: {fault_message}",
        )
        self.status_code = status_code
        self.fault_message = fault_message


@dataclass
class MachineConfig:
    """``PUT /machine-config`` body.

    Attributes:
        vcpu_count (`int`):
            Number of virtual CPUs (1..32).
        mem_size_mib (`int`):
            Guest memory in MiB (>= 128).
        smt (`bool`):
            Enable Simultaneous Multi-Threading.  ``False`` on most
            production deployments.
        cpu_template (`str | None`):
            CPUID / MSR template (``C3``, ``T2``, ``T2S``, ``T2CL``,
            ``T2A``) or ``None`` to disable.
        track_dirty_pages (`bool`):
            Enable dirty-page tracking for snapshot workflows.
    """

    vcpu_count: int
    mem_size_mib: int
    smt: bool = False
    cpu_template: str | None = None
    track_dirty_pages: bool = False

    def to_json(self) -> dict[str, Any]:
        """Serialise to the Firecracker JSON body shape."""
        body: dict[str, Any] = {
            "vcpu_count": self.vcpu_count,
            "mem_size_mib": self.mem_size_mib,
            "smt": self.smt,
            "track_dirty_pages": self.track_dirty_pages,
        }
        if self.cpu_template is not None:
            body["cpu_template"] = self.cpu_template
        return body


@dataclass
class BootSource:
    """``PUT /boot-source`` body.

    Attributes:
        kernel_image_path (`str`):
            Host-side path to the uncompressed ELF kernel image.
        boot_args (`str`):
            Kernel command line.  Defaults to
            :data:`DEFAULT_BOOT_ARGS` (console=ttyS0 + clean CtrlAltDel
            shutdown flags).
        initrd_path (`str | None`):
            Optional host-side path to an initrd image.
    """

    kernel_image_path: str
    boot_args: str = DEFAULT_BOOT_ARGS
    initrd_path: str | None = None

    def to_json(self) -> dict[str, Any]:
        """Serialise to the Firecracker JSON body shape."""
        body: dict[str, Any] = {
            "kernel_image_path": self.kernel_image_path,
            "boot_args": self.boot_args,
        }
        if self.initrd_path is not None:
            body["initrd_path"] = self.initrd_path
        return body


@dataclass
class Drive:
    """``PUT /drives/{id}`` body.

    Attributes:
        drive_id (`str`):
            Stable identifier for this drive; appears in the URL path.
        path_on_host (`str`):
            Host-side path to the backing file (ext4 image, raw block
            device, ...).
        is_root_device (`bool`):
            Whether this drive is the root filesystem.
        is_read_only (`bool`):
            Read-only flag.
        cache_type (`str`):
            Cache strategy (``Unsafe``, ``Writeback``, ``None``).
    """

    drive_id: str
    path_on_host: str
    is_root_device: bool = False
    is_read_only: bool = False
    cache_type: str = "Unsafe"

    def to_json(self) -> dict[str, Any]:
        """Serialise to the Firecracker JSON body shape."""
        return {
            "drive_id": self.drive_id,
            "path_on_host": self.path_on_host,
            "is_root_device": self.is_root_device,
            "is_read_only": self.is_read_only,
            "cache_type": self.cache_type,
        }


@dataclass
class NetworkInterface:
    """``PUT /network-interfaces/{id}`` body.

    Attributes:
        iface_id (`str`):
            Stable identifier for this interface; appears in the URL
            path.
        host_dev_name (`str`):
            Name of the host-side TAP device the virtio-net is backed
            by.  Must already exist when the VM starts.
        guest_mac (`str | None`):
            Optional MAC address presented to the guest.
    """

    iface_id: str
    host_dev_name: str
    guest_mac: str | None = None

    def to_json(self) -> dict[str, Any]:
        """Serialise to the Firecracker JSON body shape."""
        body: dict[str, Any] = {
            "iface_id": self.iface_id,
            "host_dev_name": self.host_dev_name,
        }
        if self.guest_mac is not None:
            body["guest_mac"] = self.guest_mac
        return body


@dataclass
class Vsock:
    """``PUT /vsock`` body.

    Attributes:
        vsock_id (`str`):
            Stable identifier for this vsock device.
        guest_cid (`int`):
            Guest Context Identifier.  Must be >= 3 (2 is reserved
            for the host).
        uds_path (`str`):
            Host-side Unix-domain socket path Firecracker binds to
            bridge vsock connections from the host to the guest.
    """

    vsock_id: str
    guest_cid: int
    uds_path: str

    def to_json(self) -> dict[str, Any]:
        """Serialise to the Firecracker JSON body shape."""
        return {
            "vsock_id": self.vsock_id,
            "guest_cid": self.guest_cid,
            "uds_path": self.uds_path,
        }


@dataclass
class InstanceInfo:
    """Parsed body of ``GET /``.

    Attributes:
        state (`str`):
            ``"Not started"`` or ``"Running"``.
        vmm_version (`str`):
            Firecracker VMM version string.
    """

    state: str
    vmm_version: str


@dataclass
class FirecrackerProcessHandle:
    """Bookkeeping for a spawned firecracker process.

    Attributes:
        process (`asyncio.subprocess.Process`):
            The firecracker subprocess.
        api_socket (`str`):
            Path to the API Unix socket the process is listening on.
        vsock_uds (`str | None`):
            Path to the vsock Unix socket (if a vsock was attached).
        stdout_task (`asyncio.Task | None`):
            Background task draining the process's stdout (kernel log)
            into a buffer for diagnostics.
        stdout_buffer (`bytearray`):
            Captured stdout so far (last 64 KiB).
    """

    process: asyncio.subprocess.Process
    api_socket: str
    vsock_uds: str | None = None
    stdout_task: asyncio.Task | None = None
    stdout_buffer: bytearray = field(default_factory=bytearray)

    def tail_stdout(self, n: int = 4096) -> str:
        """Return the last ``n`` bytes of stdout as text (replace errors)."""
        if not self.stdout_buffer:
            return ""
        tail = bytes(self.stdout_buffer[-n:])
        return tail.decode("utf-8", errors="replace")


class FirecrackerApi:
    """Async client for the Firecracker REST API.

    All HTTP traffic is sent over a Unix-domain socket via
    :mod:`httpx`'s UDS transport — there is no TCP surface on the
    wire.

    Args:
        api_socket (`str`):
            Filesystem path to the Firecracker API Unix socket.
        request_timeout (`float`, defaults to ``5.0``):
            Per-request timeout in seconds.
    """

    def __init__(
        self,
        api_socket: str,
        *,
        request_timeout: float = 5.0,
    ) -> None:
        """Bind socket path and timeout."""
        self._api_socket = api_socket
        self._request_timeout = request_timeout
        # ``httpx.AsyncClient(uds=...)`` mounts an HTTP/1.1 transport
        # bound to the Unix socket — every request reuses the same
        # underlying connection pool.
        self._client: httpx.AsyncClient | None = None

    # ── lifecycle ────────────────────────────────────────────────

    async def __aenter__(self) -> "FirecrackerApi":
        """Open the httpx client (lazy connect on first request)."""
        self._ensure_client()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Close the underlying httpx client."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying httpx client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _ensure_client(self) -> httpx.AsyncClient:
        """Lazily create the httpx client bound to the Unix socket."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=self._api_socket),
                base_url="http://localhost",
                timeout=self._request_timeout,
            )
        return self._client

    # ── raw request helper ───────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """Send one HTTP request and return the response.

        Non-2xx responses raise :class:`FirecrackerApiError` with the
        parsed ``fault_message`` if available.

        Args:
            method (`str`):
                HTTP method (``GET`` / ``PUT`` / ``PATCH``).
            path (`str`):
                Path component of the URL, starting with ``/``.
            json (`Any | None`, optional):
                JSON-serialisable body for PUT/PATCH.
            timeout (`float | None`, optional):
                Override the default request timeout.

        Returns:
            `httpx.Response`:
                The raw httpx response.

        Raises:
            FirecrackerApiError: On any non-2xx response.
        """
        client = self._ensure_client()
        request_timeout = (
            self._request_timeout if timeout is None else timeout
        )
        response = await client.request(
            method,
            path,
            json=json,
            timeout=request_timeout,
        )
        if 200 <= response.status_code < 300:
            return response
        fault = "unknown"
        try:
            body = response.json()
            if isinstance(body, dict):
                fault = str(
                    body.get("fault_message")
                    or body.get("error")
                    or body,
                )
        except Exception:
            fault = response.text or fault
        raise FirecrackerApiError(response.status_code, fault)

    # ── endpoints ─────────────────────────────────────────────────

    async def describe(self) -> InstanceInfo:
        """``GET /`` — instance info."""
        resp = await self._request("GET", "/")
        body = resp.json()
        return InstanceInfo(
            state=str(body.get("state", "Unknown")),
            vmm_version=str(body.get("vmm_version", "unknown")),
        )

    async def get_vm_state(self) -> str:
        """``GET /vm`` — VM state (``Running`` / ``Paused``)."""
        resp = await self._request("GET", "/vm")
        return str(resp.json().get("state", "Unknown"))

    async def pause(self) -> None:
        """``PATCH /vm`` — pause the VM."""
        await self._request("PATCH", "/vm", json={"state": "Paused"})

    async def resume(self) -> None:
        """``PATCH /vm`` — resume the VM."""
        await self._request("PATCH", "/vm", json={"state": "Resumed"})

    async def get_vm_config(self) -> dict[str, Any]:
        """``GET /vm/config`` — full machine configuration."""
        resp = await self._request("GET", "/vm/config")
        return resp.json()

    async def put_boot_source(self, boot: BootSource) -> None:
        """``PUT /boot-source`` — configure kernel + initrd + cmdline."""
        await self._request("PUT", "/boot-source", json=boot.to_json())

    async def put_machine_config(self, cfg: MachineConfig) -> None:
        """``PUT /machine-config`` — configure vCPU / mem / SMT."""
        await self._request("PUT", "/machine-config", json=cfg.to_json())

    async def put_drive(self, drive: Drive) -> None:
        """``PUT /drives/{id}`` — attach a block device."""
        await self._request(
            "PUT",
            f"/drives/{drive.drive_id}",
            json=drive.to_json(),
        )

    async def patch_drive(self, drive_id: str, *, path_on_host: str) -> None:
        """``PATCH /drives/{id}`` — notify host file changed.

        Only ``path_on_host`` is updatable post-boot; size is re-stat'd
        by Firecracker.
        """
        await self._request(
            "PATCH",
            f"/drives/{drive_id}",
            json={"path_on_host": path_on_host},
        )

    async def put_network_interface(self, iface: NetworkInterface) -> None:
        """``PUT /network-interfaces/{id}`` — attach a virtio-net."""
        await self._request(
            "PUT",
            f"/network-interfaces/{iface.iface_id}",
            json=iface.to_json(),
        )

    async def put_vsock(self, vsock: Vsock) -> None:
        """``PUT /vsock`` — attach a virtio-vsock device."""
        await self._request("PUT", "/vsock", json=vsock.to_json())

    async def start_instance(self) -> None:
        """``PUT /actions`` — boot the microVM (``InstanceStart``)."""
        await self._request(
            "PUT",
            "/actions",
            json={"action_type": "InstanceStart"},
        )

    async def send_ctrl_alt_del(self) -> None:
        """``PUT /actions`` — clean shutdown via ``SendCtrlAltDel``.

        Triggers an i8042/AT-keyboard reset; the guest's init performs
        an orderly shutdown and Firecracker exits on CPU reset.
        Requires the guest kernel to be built with
        ``CONFIG_SERIO_I8042`` + ``CONFIG_KEYBOARD_ATKBD``.
        """
        await self._request(
            "PUT",
            "/actions",
            json={"action_type": "SendCtrlAltDel"},
        )

    async def flush_metrics(self) -> None:
        """``PUT /actions`` — flush metrics to the metrics FIFO."""
        await self._request(
            "PUT",
            "/actions",
            json={"action_type": "FlushMetrics"},
        )

    async def put_logger(
        self,
        *,
        log_path: str,
        level: str = "Info",
        show_level: bool = False,
        show_log_origin: bool = False,
    ) -> None:
        """``PUT /logger`` — configure log file / level."""
        body: dict[str, Any] = {
            "log_path": log_path,
            "level": level,
            "show_level": show_level,
            "show_log_origin": show_log_origin,
        }
        await self._request("PUT", "/logger", json=body)


# ── process spawning helpers ──────────────────────────────────────


async def wait_for_socket(
    socket_path: str,
    *,
    timeout: float = DEFAULT_API_READY_TIMEOUT,
) -> None:
    """Wait until the API Unix socket exists and is connectable.

    Firecracker creates the socket file before binding the listener;
    we retry ``connect()`` until it succeeds (or ``timeout`` elapses)
    rather than relying on ``os.path.exists``.

    Raises:
        TimeoutError: If the socket is not connectable within ``timeout``.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    last_err: Exception | None = None
    while asyncio.get_event_loop().time() < deadline:
        # ``socket.socket(AF_UNIX).connect_ex`` is non-blocking-safe and
        # returns 0 on success — no exception to clean up on failure.
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            err = sock.connect_ex(socket_path)
            if err == 0:
                return
            last_err = OSError(err, os.strerror(err))
        finally:
            sock.close()
        await asyncio.sleep(0.05)
    msg = f"Firecracker API socket {socket_path!r} not ready "
    if last_err is not None:
        msg += f"(last error: {last_err})"
    raise TimeoutError(msg)


async def spawn_firecracker(
    *,
    api_socket: str,
    firecracker_bin: str,
    log_path: str | None = None,
    log_level: str = "Info",
    seccomp_level: int = 2,
    extra_args: list[str] | None = None,
) -> FirecrackerProcessHandle:
    """Spawn a ``firecracker`` process bound to ``api_socket``.

    The process is spawned with its stdin closed and stdout captured
    (the guest kernel log goes there when ``console=ttyS0`` is in the
    boot args).  A background task drains stdout into a 64 KiB rolling
    buffer so it can be surfaced in error messages.

    Args:
        api_socket (`str`):
            Filesystem path for the API Unix socket.  Removed first
            if it already exists.
        firecracker_bin (`str`):
            Path to (or name of) the firecracker binary.
        log_path (`str | None`, optional):
            Path for the firecracker process's own log file.
        log_level (`str`, defaults to ``"Info"``):
            Firecracker log level (``Error`` / ``Warning`` / ``Info``
            / ``Debug``).
        seccomp_level (`int`, defaults to ``2``):
            Seccomp filter level (0 = off, 1 = basic, 2 = advanced).
        extra_args (`list[str] | None`, optional):
            Extra CLI args appended after the standard flags.

    Returns:
        `FirecrackerProcessHandle`:
            Bookkeeping for the spawned process.
    """
    # Remove any stale socket so Firecracker can bind cleanly.
    try:
        os.unlink(api_socket)
    except FileNotFoundError:
        pass

    # Make sure the parent dir exists.
    parent = os.path.dirname(api_socket) or "."
    os.makedirs(parent, exist_ok=True)

    cmd: list[str] = [
        firecracker_bin,
        "--api-sock",
        api_socket,
        "--seccomp-level",
        str(seccomp_level),
        "--level",
        log_level,
    ]
    if log_path is not None:
        cmd.extend(["--log-path", log_path])
    if extra_args:
        cmd.extend(extra_args)

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    handle = FirecrackerProcessHandle(
        process=process,
        api_socket=api_socket,
    )

    # Background drain of stdout into a rolling 64 KiB buffer — used
    # only for diagnostics when boot fails.
    async def _drain() -> None:
        try:
            while True:
                chunk = await process.stdout.read(4096)
                if not chunk:
                    break
                handle.stdout_buffer.extend(chunk)
                # Trim to the last 64 KiB to bound memory.
                if len(handle.stdout_buffer) > 65536:
                    del handle.stdout_buffer[: len(handle.stdout_buffer) - 65536]
        except Exception:
            pass

    handle.stdout_task = asyncio.create_task(_drain())
    return handle


async def reap_firecracker(
    handle: FirecrackerProcessHandle,
    *,
    shutdown_timeout: float,
) -> None:
    """Best-effort teardown: SIGTERM, wait, then SIGKILL.

    Caller is expected to have already issued ``SendCtrlAltDel`` (or
    decided not to).  This helper only reaps the process and the
    stdout-drain task.
    """
    if handle.stdout_task is not None:
        handle.stdout_task.cancel()
        try:
            await handle.stdout_task
        except (asyncio.CancelledError, Exception):
            pass
        handle.stdout_task = None

    if handle.process.returncode is None:
        try:
            handle.process.terminate()
            try:
                await asyncio.wait_for(
                    handle.process.wait(),
                    timeout=shutdown_timeout,
                )
            except asyncio.TimeoutError:
                handle.process.kill()
                await handle.process.wait()
        except ProcessLookupError:
            pass
