# Firecracker backend

English | [简体中文](firecracker_zh.md)

This document covers how to prepare a host to run the Firecracker backend, how the in-VM guest agent is wired up, and how the wire protocol is exercised by the test suite.

## Host prerequisites

```bash
# 1. KVM device node must be present and writable by the firecracker user.
test -c /dev/kvm && test -w /dev/kvm && echo OK

# 2. firecracker binary on $PATH (or set DEFAULT_FIRECRACKER_BIN).
firecracker --version

# 3. Kernel image — uncompressed ELF Linux kernel built with the
#    Firecracker-recommended guest config (virtio_blk, virtio_net,
#    virtio_vsocks, 8250 serial, ext4).
ls -l /var/lib/firecracker/vmlinux

# 4. Root filesystem — ext4 image with /bin/sh, python3 and socat/nc.
ls -l /var/lib/firecracker/rootfs.ext4
```

If any of the above is missing, `FirecrackerWorkspace.verify_runtime_available()` raises a descriptive `RuntimeError` and the manager refuses to provision.

## Building the rootfs

The rootfs must contain:

- A working `/bin/sh` and `/usr/bin/python3`.
- `socat` or `nc` for the vsock guest-agent bootstrap (optional if the agent is baked in at build time).
- The agentscope guest agent at `/root/.agentscope/_guest_agent.py`, started by init.

Use the helper script:

```bash
sudo tools/build-rootfs.sh /var/lib/firecracker/rootfs.ext4
```

The script:

1. Creates a 512 MiB ext4 image.
2. Bootstraps a minimal Debian into it via `debootstrap`.
3. Installs `python3` and `socat`.
4. Copies `_guest_agent.py` from this package into `/root/.agentscope/`.
5. Adds an init entry (`/etc/rc.local`) that starts the guest agent on boot.

## Wire protocol

Host → guest requests are framed as `[4-byte big-endian length][json]` over the virtio-vsock device. The guest agent listens on `AF_VSOCK` at port `DEFAULT_GUEST_AGENT_PORT` (1024 by default). Firecracker exposes the vsock device on the host as a Unix-domain socket; the host dials it and sends `CONNECT <port>\n` to bridge to the guest port.

### Operations

| `op` | Request fields | Response fields |
|---|---|---|
| `ping` | — | `ok`, `pong` |
| `exec` | `argv: list[str]`, `timeout: float` | `ok`, `exit_code`, `stdout` (base64), `stderr` (base64) |
| `read_file` | `path: str` | `ok`, `data` (base64) — or `ok=false, error` |
| `write_file` | `path: str`, `data` (base64) | `ok` — or `ok=false, error` |

The agent is intentionally tiny: stdlib only, no third-party imports, so it runs on any image that ships `python3`.

## Lifecycle

```
FirecrackerWorkspace.initialize()
    │
    ├── verify_runtime_available()       # probe host
    ├── spawn firecracker --api-socket …  # async subprocess
    ├── wait for API socket to appear     # 5s timeout
    │
    ├── PUT /boot-source                 # kernel_image_path + boot_args
    ├── PUT /machine-config              # vcpu_count, mem_size_mib
    ├── PUT /drives/rootfs               # rootfs path, read/write
    ├── PUT /vsock                       # guest CID
    ├── POST /actions instance.start     # boot the VM
    │
    ├── wait for guest agent             # ping loop, 30s timeout
    └── SandboxedWorkspaceBase.bootstrap_gateway()
            │
            └── via vsock exec / file ops:
                install agentscope into gateway venv,
                write gateway script,
                start gateway,
                poll gateway /healthz
```

## Pool tuning

Firecracker microVMs are heavier than containers, so the default pool is conservative:

| Knob | Default | Effect |
|---|---|---|
| `max_size` | 4 | Hard cap on warm microVMs |
| `min_warm` | 1 | Pre-warm one microVM at startup |
| `idle_ttl` | 1800 s | Recycle idle microVMs after 30 min |

For a multi-tenant host with bursty traffic, raise `min_warm` to 2-3. For a single-user dev box, set `min_warm=0` to disable pre-warming entirely.

## CI testing

CI cannot run a real Firecracker microVM (no `/dev/kvm`), so the test suite exercises the wire protocol against a real Unix-socket server that loads the same `GUEST_AGENT_SOURCE` string the workspace ships into the rootfs:

```python
from agentscope_sandbox_ext._firecracker._backend import GuestAgentClient
from _helpers.guest_agent_server import GuestAgentUnixServer

# _make_unix_connect returns an async callable that opens a real
# Unix-socket connection (see tests/test_firecracker_guest_agent.py).
with GuestAgentUnixServer() as srv:
    client = GuestAgentClient(connect=_make_unix_connect(srv.path))
    result = await client.exec_shell(["echo", "hello"])
    assert result.exit_code == 0
    assert result.stdout.strip() == b"hello"
```

This verifies the protocol end-to-end without needing a VM, and means any drift between the host client and the guest handler is caught immediately.
