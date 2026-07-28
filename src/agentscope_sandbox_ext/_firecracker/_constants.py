# -*- coding: utf-8 -*-
"""Constants for the Firecracker microVM workspace backend.

Path layout (venv, script, log, helper) is derived on
:class:`SandboxedWorkspaceBase` from ``_gateway_home``.  This module
only carries defaults that cannot be derived: kernel / rootfs paths,
gateway port, workdir, and the vsock CID range reserved for our
guest agents.
"""

#: Default Firecracker binary path on the host.  Override via the
#: workspace constructor if your install lives elsewhere.
DEFAULT_FIRECRACKER_BIN = "firecracker"

#: Default kernel image — uncompressed ELF Linux kernel built with
#: the Firecracker-recommended guest config (virtio_blk, virtio_net,
#: virtio_vsocks, 8250 serial, ext4).  The path must be readable by
#: the firecracker process (and inside the jailer chroot if jailed).
DEFAULT_KERNEL_PATH = "/var/lib/firecracker/vmlinux"

#: Default root filesystem — an ext4 image with a working ``/bin/sh``,
#: ``python3`` and the ``socat``/``nc`` we use for the vsock guest
#: agent bootstrap.  The image must be writable by the user the
#: firecracker process runs as.
DEFAULT_ROOTFS_PATH = "/var/lib/firecracker/rootfs.ext4"

#: Default guest-side workdir — agent-visible root.  Same convention
#: as the native applecontainer / docker backends.
CONTAINER_WORKDIR = "/workspace"

#: Default gateway home inside the guest.  The venv, script and log
#: files all live underneath here (derived by
#: :class:`SandboxedWorkspaceBase`).
GATEWAY_HOME = "/root/.agentscope"

#: Default TCP port the in-VM gateway listens on (no host port mapping).
DEFAULT_GATEWAY_PORT = 5600

#: Default vCPU count for a microVM.  Firecracker supports 1..32.
DEFAULT_VCPU_COUNT = 2

#: Default memory in MiB.  Firecracker requires >= 128; we pick 1024
#: so the gateway venv + agentscope + a couple of MCP servers fit.
DEFAULT_MEM_SIZE_MIB = 1024

#: Default guest CID for the virtio-vsock device.  Must be >= 3 (2 is
#: reserved for the host).  We pick a value that is unlikely to clash
#: with other vsock users on a multi-tenant host.
DEFAULT_GUEST_CID = 3

#: Port inside the guest the vsock guest agent listens on.  The host
#: dials the UDS exposed by Firecracker and sends ``CONNECT <port>``
#: to bridge to this guest port.
DEFAULT_GUEST_AGENT_PORT = 1024

#: Default boot args passed to the guest kernel.  ``console=ttyS0``
#: routes the kernel log to the serial console (captured into the
#: firecracker stdout pipe).  ``reboot=k panic=1`` makes the guest
#: reboot (and Firecracker exit) on kernel panic, which is how
#: ``SendCtrlAltDel`` cleanly tears the process down.
DEFAULT_BOOT_ARGS = (
    "console=ttyS0 reboot=k panic=1 pci=off "
    "random.trust_cpu=on i8042.noaux i8042.nomux i8042.nopnp i8042.dumbkbd"
)

#: Default directory under which the per-VM API socket, vsock UDS,
#: log files and jailer chroot live.  Each VM gets its own
#: subdirectory named after the workspace id.
DEFAULT_RUN_DIR = "/run/agentscope-sandbox-ext/firecracker"

#: Timeout (seconds) for the firecracker process to exit after
#: ``SendCtrlAltDel`` before we fall back to ``SIGKILL``.
DEFAULT_SHUTDOWN_TIMEOUT = 10.0

#: Timeout (seconds) for the firecracker API socket to appear after
#: spawning the process.
DEFAULT_API_READY_TIMEOUT = 5.0

#: Default idle TTL (seconds) for the Firecracker pool.  microVMs are
#: heavier than containers so we keep them warm a bit longer.
DEFAULT_POOL_IDLE_TTL = 1800.0

#: Default minimum warm pool size.  microVM cold-boot is fast (~125ms
#: to guest init for a minimal Firecracker VM) but the in-VM gateway
#: bootstrap is not, so a small warm pool smooths the first request.
DEFAULT_POOL_MIN_WARM = 1

#: Default maximum pool size.
DEFAULT_POOL_MAX_SIZE = 4
