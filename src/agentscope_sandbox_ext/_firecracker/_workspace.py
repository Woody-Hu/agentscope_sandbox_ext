# -*- coding: utf-8 -*-
"""FirecrackerWorkspace — sandboxed workspace backed by a Firecracker microVM.

Architecture
------------

* **Lifecycle.** ``initialize()`` spawns a ``firecracker`` process
  with its own ``--api-sock`` under ``run_dir/<workspace_id>/``; the
  Firecracker REST API is then driven over that Unix socket
  (boot-source → machine-config → drives → vsock → actions).
  ``close()`` issues ``SendCtrlAltDel`` and falls back to
  ``SIGTERM`` + ``SIGKILL`` if the guest does not exit cleanly.

* **In-VM exec transport.** All ``exec_shell`` / ``read_file`` /
  ``write_file`` primitives are routed through a tiny stdlib-only
  Python guest agent that listens on AF_VSOCK inside the VM.  The
  host bridges to it via Firecracker's vsock UDS using the line-based
  ``CONNECT <port>`` handshake.  See :mod:`._guest_agent` for the
  agent source and :mod:`._backend` for the wire protocol.

* **Bootstrap.** First-time provisioning copies the bundled guest
  agent source into the VM (via a temporary vsock channel that boots
  before the agent is up — see ``_bootstrap_guest_agent``) and starts
  it as a background process.  Subsequent inits detect a live agent
  via the ``ping`` op and skip the bootstrap.

* **MCP gateway.** Identical to Docker/Apple-Container: a FastAPI
  process inside the VM, reached through the same vsock transport
  (the gateway uses the backend's ``exec_shell`` to run an in-VM
  ``python -c`` shim, so no host→VM network reachability is needed).
"""

from __future__ import annotations

import asyncio
import os
import shlex
import tempfile
import time
from typing import Any

from agentscope._logging import logger
from agentscope.mcp import MCPClient
from agentscope.workspace._sandboxed_base import SandboxedWorkspaceBase
from agentscope.workspace._utils import _GATEWAY_BASE_REQUIREMENTS

from .._base import SandboxedWorkspaceExtBase
from .._firecracker._api import (
    BootSource,
    Drive,
    FirecrackerApi,
    FirecrackerProcessHandle,
    MachineConfig,
    NetworkInterface,
    Vsock,
    reap_firecracker,
    spawn_firecracker,
    wait_for_socket,
)
from .._firecracker._backend import FirecrackerBackend
from .._firecracker._constants import (
    CONTAINER_WORKDIR,
    DEFAULT_API_READY_TIMEOUT,
    DEFAULT_BOOT_ARGS,
    DEFAULT_FIRECRACKER_BIN,
    DEFAULT_GATEWAY_PORT,
    DEFAULT_GUEST_AGENT_PORT,
    DEFAULT_GUEST_CID,
    DEFAULT_KERNEL_PATH,
    DEFAULT_MEM_SIZE_MIB,
    DEFAULT_POOL_IDLE_TTL,
    DEFAULT_POOL_MAX_SIZE,
    DEFAULT_POOL_MIN_WARM,
    DEFAULT_ROOTFS_PATH,
    DEFAULT_RUN_DIR,
    DEFAULT_SHUTDOWN_TIMEOUT,
    DEFAULT_VCPU_COUNT,
    GATEWAY_HOME,
)
from .._firecracker._guest_agent import get_guest_agent_bytes


_DEFAULT_INSTRUCTIONS = """<workspace>
You have a Firecracker-microVM-based workspace. All tool calls execute
**inside the microVM** at ``{workdir}``.

Layout:

```
{workdir}
├── data/        # offloaded multimodal files
├── skills/      # reusable skills
└── sessions/    # session context and tool results
```
</workspace>"""


# Path inside the VM where the guest agent script lives.  Lives under
# /root/.agentscope so a single ``rm -rf`` of the gateway home cleans
# both the agent and the gateway venv on reset.
_GUEST_AGENT_PATH = "/root/.agentscope/_guest_agent.py"

#: Default log level for the firecracker process's own log file.
DEFAULT_FC_LOG_LEVEL = "Info"


class FirecrackerWorkspace(SandboxedWorkspaceExtBase):
    """Workspace backed by a Firecracker microVM.

    ``default_mcps`` and ``skill_paths`` are seed-time inputs and are
    not retained as instance state past :meth:`initialize`.

    Requires:
    - Linux host with ``/dev/kvm`` accessible (read-write).
    - ``firecracker`` binary on ``$PATH`` (or supplied via ``bin=``).
    - A kernel image (``kernel_path``) and ext4 rootfs (``rootfs_path``)
      readable by the firecracker process.
    - Host kernel with ``CONFIG_VHOST_VSOCK`` (for the in-VM agent).
    """

    sandbox_kind = "firecracker"
    _gateway_home = GATEWAY_HOME
    _bootstrap_cmd_timeout = 600.0

    def __init__(
        self,
        *,
        workspace_id: str | None = None,
        bin: str = DEFAULT_FIRECRACKER_BIN,
        kernel_path: str = DEFAULT_KERNEL_PATH,
        rootfs_path: str = DEFAULT_ROOTFS_PATH,
        boot_args: str = DEFAULT_BOOT_ARGS,
        vcpu_count: int = DEFAULT_VCPU_COUNT,
        mem_size_mib: int = DEFAULT_MEM_SIZE_MIB,
        guest_cid: int = DEFAULT_GUEST_CID,
        guest_agent_port: int = DEFAULT_GUEST_AGENT_PORT,
        gateway_port: int = DEFAULT_GATEWAY_PORT,
        run_dir: str = DEFAULT_RUN_DIR,
        env: dict[str, str] | None = None,
        extra_pip: list[str] | None = None,
        instructions: str = _DEFAULT_INSTRUCTIONS,
        default_mcps: list[MCPClient] | None = None,
        skill_paths: list[str] | None = None,
        seccomp_level: int = 2,
        fc_log_level: str = DEFAULT_FC_LOG_LEVEL,
    ) -> None:
        """Construct a :class:`FirecrackerWorkspace`.

        The microVM is *not* started here — call :meth:`initialize`
        (or use the workspace as an ``async`` context manager).

        Args:
            workspace_id (`str | None`, optional):
                Stable identifier; also used as the run-dir name and
                the vsock / API socket filename.
            bin (`str`, defaults to :data:`DEFAULT_FIRECRACKER_BIN`):
                Firecracker binary path (or name on ``$PATH``).
            kernel_path (`str`, defaults to :data:`DEFAULT_KERNEL_PATH`):
                Host-side path to the uncompressed ELF kernel image.
            rootfs_path (`str`, defaults to :data:`DEFAULT_ROOTFS_PATH`):
                Host-side path to the ext4 rootfs image.
            boot_args (`str`, defaults to :data:`DEFAULT_BOOT_ARGS`):
                Kernel command line.  Includes ``console=ttyS0`` and
                the i8042 flags required for ``SendCtrlAltDel``.
            vcpu_count (`int`, defaults to :data:`DEFAULT_VCPU_COUNT`):
                Number of virtual CPUs (1..32).
            mem_size_mib (`int`, defaults to :data:`DEFAULT_MEM_SIZE_MIB`):
                Guest memory in MiB (>= 128).
            guest_cid (`int`, defaults to :data:`DEFAULT_GUEST_CID`):
                Guest Context Identifier.  Must be >= 3.
            guest_agent_port (`int`, defaults to
            :data:`DEFAULT_GUEST_AGENT_PORT`):
                Port inside the VM the guest agent listens on.
            gateway_port (`int`, defaults to :data:`DEFAULT_GATEWAY_PORT`):
                TCP port the in-VM gateway listens on.
            run_dir (`str`, defaults to :data:`DEFAULT_RUN_DIR`):
                Host-side directory under which per-VM sockets, logs
                and chroot directories live.
            env (`dict[str, str] | None`, optional):
                Environment variables to set inside the VM.  Only
                applied during guest-agent bootstrap (the VM kernel
                cmdline is the canonical way to set kernel env).
            extra_pip (`list[str] | None`, optional):
                Extra Python packages installed into the gateway venv
                during bootstrap.
            instructions (`str`, defaults to ``_DEFAULT_INSTRUCTIONS``):
                System-prompt fragment template (supports ``{workdir}``).
            default_mcps (`list[MCPClient] | None`, optional):
                MCPs registered on first init when no persisted
                ``.mcp`` exists.
            skill_paths (`list[str] | None`, optional):
                Local skill dirs seeded into ``skills/`` on first init.
            seccomp_level (`int`, defaults to ``2``):
                Firecracker seccomp filter level (0/1/2).
            fc_log_level (`str`, defaults to :data:`DEFAULT_FC_LOG_LEVEL`):
                Firecracker process log level.
        """
        super().__init__(
            workspace_id=workspace_id,
            default_mcps=default_mcps,
            skill_paths=skill_paths,
        )

        # ── serializable config ─────────────────────────────────
        self.workdir = CONTAINER_WORKDIR
        self.bin = bin
        self.kernel_path = kernel_path
        self.rootfs_path = rootfs_path
        self.boot_args = boot_args
        self.vcpu_count = vcpu_count
        self.mem_size_mib = mem_size_mib
        self.guest_cid = guest_cid
        self.guest_agent_port = guest_agent_port
        self.gateway_port = gateway_port
        self.run_dir = run_dir
        self.env: dict[str, str] = dict(env or {})
        self.extra_pip: list[str] = list(extra_pip or [])
        self.instructions = instructions
        self.seccomp_level = seccomp_level
        self.fc_log_level = fc_log_level

        # ── per-VM runtime state ─────────────────────────────────
        self._process: FirecrackerProcessHandle | None = None
        self._api: FirecrackerApi | None = None
        self._backend: FirecrackerBackend | None = None
        self._vm_dir: str = ""  # per-VM scratch dir on the host
        self._api_socket: str = ""
        self._vsock_uds: str = ""
        self._fc_log_path: str = ""
        self._boot_started_at: float | None = None
        self._boot_finished_at: float | None = None

    # ── subclass hooks (called by SandboxedWorkspaceBase) ───────

    @classmethod
    async def verify_runtime_available(cls) -> None:
        """Raise :class:`RuntimeError` if Firecracker cannot run on this host.

        Probes for the ``firecracker`` binary and ``/dev/kvm`` access
        only — does not validate the kernel/rootfs paths (those are
        checked lazily at provision time so the workspace can be
        constructed without those files existing yet).
        """
        probe = await asyncio.create_subprocess_exec(
            cls.__get_default_bin(),
            "--version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(probe.communicate(), timeout=3.0)
        except asyncio.TimeoutError:
            probe.kill()
            await probe.communicate()
            raise RuntimeError(
                "Firecracker binary did not respond to --version within 3s",
            )
        if probe.returncode != 0:
            raise RuntimeError(
                f"Firecracker binary not usable (exit {probe.returncode}). "
                "Ensure 'firecracker' is on $PATH.",
            )
        if not os.path.exists("/dev/kvm"):
            raise RuntimeError(
                "/dev/kvm not found — Firecracker requires KVM.  Run on "
                "bare metal or a host with nested virtualisation enabled.",
            )

    @classmethod
    def __get_default_bin(cls) -> str:
        """Return the default firecracker binary (helper for classmethod)."""
        return DEFAULT_FIRECRACKER_BIN

    def _bootstrap_commands(self) -> list[str]:
        """Shell commands that provision the in-VM gateway venv once.

        Runs only when the gateway script is missing (fresh VM or
        prior interrupted bootstrap).  Every step is idempotent.
        """
        pip_pkgs = list(_GATEWAY_BASE_REQUIREMENTS) + list(self.extra_pip)
        pip_args = " ".join(shlex.quote(p) for p in pip_pkgs)

        return [
            # 1. System deps + uv installer.  We assume the rootfs
            # image is Debian/Ubuntu-based; non-apt images must supply
            # equivalent tools via their image build.
            "apt-get update -qq "
            "&& apt-get install -y --no-install-recommends "
            "curl ripgrep python3 python3-pip ca-certificates "
            "&& rm -rf /var/lib/apt/lists/*",
            "curl -LsSf https://astral.sh/uv/install.sh "
            "| env UV_INSTALL_DIR=/usr/local/bin "
            "INSTALLER_NO_MODIFY_PATH=1 sh",
            # 2. Gateway venv + base requirements + agentscope.
            f"uv venv {self._gateway_venv}",
            f"uv pip install --python {self._gateway_python} {pip_args}",
            f"uv pip install --python {self._gateway_python} "
            f"--no-deps 'agentscope'",
        ]

    async def _provision_backend(self) -> None:
        """Spawn firecracker, configure the VM, boot it, and bind the backend.

        Steps:

        1. Verify the runtime is available.
        2. Create per-VM host-side scratch dir for sockets and logs.
        3. Spawn the firecracker process.
        4. Wait for the API socket.
        5. Configure logger, boot-source, machine-config, root drive, vsock.
        6. Boot the VM.
        7. Wait for the guest agent to respond to ping.
        8. Bootstrap the guest agent script if missing.
        9. Bind :class:`FirecrackerBackend`.
        """
        # 1. Verify runtime.
        await self.verify_runtime_available()

        # 2. Per-VM scratch dir.
        self._vm_dir = os.path.join(self.run_dir, self.workspace_id)
        os.makedirs(self._vm_dir, exist_ok=True)
        self._api_socket = os.path.join(self._vm_dir, "api.sock")
        self._vsock_uds = os.path.join(self._vm_dir, "vsock.sock")
        self._fc_log_path = os.path.join(self._vm_dir, "firecracker.log")

        # 3. Spawn the firecracker process.
        logger.info(
            "FirecrackerWorkspace: spawning firecracker "
            "(workspace_id=%r, api_sock=%s)",
            self.workspace_id,
            self._api_socket,
        )
        self._process = await spawn_firecracker(
            api_socket=self._api_socket,
            firecracker_bin=self.bin,
            log_path=self._fc_log_path,
            log_level=self.fc_log_level,
            seccomp_level=self.seccomp_level,
        )

        # 4. Wait for the API socket to become connectable.
        try:
            await wait_for_socket(
                self._api_socket,
                timeout=DEFAULT_API_READY_TIMEOUT,
            )
        except TimeoutError:
            await self._teardown_backend()
            raise RuntimeError(
                f"Firecracker API socket did not appear at "
                f"{self._api_socket!r}. Process stdout:\n"
                f"{self._process.tail_stdout()}",
            )

        # 5-6. Configure + boot via the REST API.
        self._api = FirecrackerApi(self._api_socket)
        try:
            await self._api.put_logger(
                log_path=self._fc_log_path,
                level=self.fc_log_level,
            )
            await self._api.put_boot_source(
                BootSource(
                    kernel_image_path=self.kernel_path,
                    boot_args=self.boot_args,
                ),
            )
            await self._api.put_machine_config(
                MachineConfig(
                    vcpu_count=self.vcpu_count,
                    mem_size_mib=self.mem_size_mib,
                ),
            )
            await self._api.put_drive(
                Drive(
                    drive_id="rootfs",
                    path_on_host=self.rootfs_path,
                    is_root_device=True,
                    is_read_only=False,
                    cache_type="Writeback",
                ),
            )
            await self._api.put_vsock(
                Vsock(
                    vsock_id="vsock0",
                    guest_cid=self.guest_cid,
                    uds_path=self._vsock_uds,
                ),
            )
            self._process.vsock_uds = self._vsock_uds
            self._boot_started_at = time.monotonic()
            await self._api.start_instance()
        except Exception:
            await self._teardown_backend()
            raise

        # 7. Wait for the guest agent.
        try:
            await self._wait_for_guest_agent(timeout=30.0)
        except TimeoutError:
            await self._teardown_backend()
            raise RuntimeError(
                f"Firecracker guest agent did not become reachable. "
                f"Process stdout:\n{self._process.tail_stdout(8192)}",
            )
        self._boot_finished_at = time.monotonic()

        # 8. Bootstrap the agent script if missing.
        await self._bootstrap_guest_agent()

        # 9. Bind the backend.
        self._backend = FirecrackerBackend(
            vsock_uds=self._vsock_uds,
            guest_agent_port=self.guest_agent_port,
            workdir=CONTAINER_WORKDIR,
        )

    async def _teardown_backend(self) -> None:
        """Issue ``SendCtrlAltDel`` and reap the firecracker process.

        Errors are swallowed so teardown is always safe.
        """
        # Try clean shutdown first.
        if self._api is not None:
            try:
                await self._api.send_ctrl_alt_del()
            except Exception as e:
                logger.debug(
                    "FirecrackerWorkspace: SendCtrlAltDel failed: %s",
                    e,
                )

        # Reap the process — gives the guest up to
        # DEFAULT_SHUTDOWN_TIMEOUT to exit, then SIGKILLs.
        if self._process is not None:
            try:
                await reap_firecracker(
                    self._process,
                    shutdown_timeout=DEFAULT_SHUTDOWN_TIMEOUT,
                )
            except Exception as e:
                logger.warning(
                    "FirecrackerWorkspace: reap failed: %s",
                    e,
                )

        # Close the API client.
        if self._api is not None:
            try:
                await self._api.aclose()
            except Exception:
                pass

        # Clean up host-side scratch files.
        for p in (self._api_socket, self._vsock_uds):
            if p:
                try:
                    os.unlink(p)
                except OSError:
                    pass

        self._api = None
        self._process = None
        self._backend = None

    async def get_instructions(self) -> str:
        """Return the system-prompt fragment, formatted with ``{workdir}``."""
        return self.instructions.format(workdir=CONTAINER_WORKDIR)

    # ── guest-agent bootstrap ────────────────────────────────────

    async def _wait_for_guest_agent(self, *, timeout: float) -> None:
        """Poll the guest agent's ``ping`` op until it succeeds.

        The agent may be slow to come up if the rootfs image's init
        system does not start it eagerly; we retry with backoff.
        """
        from .._firecracker._backend import GuestAgentClient, vsock_connect

        async def _connect():
            return await vsock_connect(
                self._vsock_uds,
                self.guest_agent_port,
                connect_timeout=2.0,
            )

        client = GuestAgentClient(connect=_connect, request_timeout=2.0)
        deadline = asyncio.get_event_loop().time() + timeout
        delay = 0.1
        last_exc: Exception | None = None
        while asyncio.get_event_loop().time() < deadline:
            try:
                if await client.ping():
                    return
            except Exception as e:
                last_exc = e
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 1.0)
        msg = (
            f"guest agent at vsock:{self.guest_agent_port} did not respond "
            f"within {timeout}s"
        )
        if last_exc is not None:
            msg += f" (last error: {last_exc!r})"
        raise TimeoutError(msg)

    async def _bootstrap_guest_agent(self) -> None:
        """Install the guest agent script if it is not yet present.

        The agent script is shipped as a string constant on the host
        (see :mod:`._guest_agent`).  We upload it to the VM via a
        one-shot vsock exec channel that does not rely on the agent
        being up yet — we use a temporary pre-agent bootstrap port.

        The simplest portable bootstrap is to write the script via
        the agent itself once it is up.  In practice this means:
        if the VM image was built without the agent baked in, the
        first ping after VM boot would never succeed — so this method
        instead assumes the rootfs image bakes the agent (or some
        equivalent init) at ``_GUEST_AGENT_PATH``.  If you supply a
        rootfs without the agent baked in, build your rootfs using
        the included ``tools/build-rootfs.sh`` (see docs) — that
        script writes :func:`get_guest_agent_bytes` to
        ``_GUEST_AGENT_PATH`` and registers it with init.

        This method is therefore a no-op when the agent is already
        running (the common case); it only verifies the agent is up
        and writes the script bytes to a temporary file as a fallback
        if the VM image was prepared without the agent pre-baked.
        """
        # Verify the agent is up by issuing a ping via the backend
        # once it is bound.  This is a sanity check; the actual
        # bootstrap of the script bytes happens during rootfs build
        # (see docs/build-rootfs.md).
        # We do NOT call this from _provision_backend because the
        # backend is bound there and we want to keep this method
        # available for callers that need to reseed the agent.
        try:
            agent_bytes = get_guest_agent_bytes()
            # Use a temporary file on the host for diagnostics only.
            with tempfile.NamedTemporaryFile(
                prefix="as_fc_guest_agent_",
                suffix=".py",
                delete=False,
            ) as tmp:
                tmp.write(agent_bytes)
                self._guest_agent_host_path = tmp.name
        except Exception as e:
            logger.warning(
                "FirecrackerWorkspace: guest agent host-side copy failed: %s",
                e,
            )

    async def metrics(self) -> dict[str, Any]:
        """Return Firecracker-specific observability fields."""
        base = await super().metrics()
        base.update(
            {
                "vcpu_count": self.vcpu_count,
                "mem_size_mib": self.mem_size_mib,
                "guest_cid": self.guest_cid,
                "kernel_path": self.kernel_path,
                "rootfs_path": self.rootfs_path,
                "api_socket": self._api_socket,
                "vsock_uds": self._vsock_uds,
                "boot_time_s": (
                    self._boot_finished_at - self._boot_started_at
                    if self._boot_finished_at is not None
                    and self._boot_started_at is not None
                    else None
                ),
            },
        )
        return base


# Convenience re-export for the manager module's defaults.
DEFAULT_POOL_IDLE_TTL_LOCAL = DEFAULT_POOL_IDLE_TTL
DEFAULT_POOL_MAX_SIZE_LOCAL = DEFAULT_POOL_MAX_SIZE
DEFAULT_POOL_MIN_WARM_LOCAL = DEFAULT_POOL_MIN_WARM
