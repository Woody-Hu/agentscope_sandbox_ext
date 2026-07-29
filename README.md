# agentscope-sandbox-ext

English | [简体中文](README_zh.md)

Extension sandbox backends for the [agentScope](https://github.com/agentscope-ai/agentscope) framework — **Firecracker microVM**, **gVisor (runsc)**, **Kata Containers**, **Sysbox**, and the new **VFS / agentfs** zero-runtime backend.

This package adds five new sandboxed-workspace backends **without modifying any agentscope native code**. Everything is composed by inheritance from agentscope's own (documented-subclassing) abstractions:

- [`agentscope.workspace._sandboxed_base.SandboxedWorkspaceBase`](https://github.com/agentscope-ai/agentscope/blob/main/src/agentscope/workspace/_sandboxed_base.py) — the in-sandbox MCP-gateway template-method lifecycle.
- [`agentscope.app.workspace_manager._base.WorkspaceManagerBase`](https://github.com/agentscope-ai/agentscope/blob/main/src/agentscope/app/workspace_manager/_base.py) — the manager interface `agentscope.app` calls into.

Each workspace inherits from a single `SandboxedWorkspaceExtBase` so a management plane has a uniform `isinstance` discriminator and `metrics()` hook, while keeping the full sandboxed-workspace lifecycle (gateway bootstrap, MCP persistence, skill seeding, offload) inherited from `SandboxedWorkspaceBase`.

## Backends

| Backend | `sandbox_kind` | Isolation | Cold boot | Best for |
|---|---|---|---|---|
| **Firecracker** | `firecracker` | KVM-grade microVM, separate kernel | ~125 ms to guest init | Strongest isolation, multi-tenant, untrusted code |
| **gVisor (runsc)** | `gvisor` | Application-level kernel (Sentry intercepts syscalls) | Container-fast | Strong isolation without VM overhead |
| **Kata Containers** | `kata` | Hardware-VM-backed container (Firecracker/QEMU hypervisor) | ~1 s | VM-grade isolation with container ergonomics |
| **Sysbox** | `sysbox` | Docker runtime with per-container user/mount namespaces, virtualised `/proc` `/sys`, Docker-in-Docker without `--privileged` | Container-fast | Nested container workloads, stronger-than-runc isolation without a VM |
| **VFS / agentfs** | `agentfs` | None — translates `BackendBase` primitives to host I/O + subprocess confined to a per-workspace dir | ~microseconds | CI / dev / benchmark baseline; trusted code that only needs a clean workdir |

agentScope already ships Docker, E2B, K8s, Apple Container, Bubblewrap, Daytona and OpenSandbox backends — this package fills the gaps with the four mainstream VM/sandbox runtimes the framework does not yet cover, starting from the lightest and most popular (Firecracker).

## Highlights

- **No native code modification.** Only imports from agentscope. Drops into any agentscope `>=2.0.5` install.
- **Unified extension interface.** `SandboxedWorkspaceExtBase` + `SandboxExtManagerBase` give a single `isinstance` discriminator, a `metrics()` hook, and a `verify_runtime_available()` probe that every backend implements.
- **Pooling optimisation.** `SandboxPool` keeps `min_warm` sandboxes hot, evicts idle ones past `idle_ttl`, and caps the warm pool at `max_size`. Each manager can compose it in front of its cache. Tunable `acquire_strategy` (`fifo`/`lifo`), optional `health_check` probe on acquire, and `max_concurrent_provisions` semaphore to prevent thundering-herd boots. See [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) for the full analysis and [`docs/VFS.md`](docs/VFS.md) for the benchmark results that quantify each knob.
- **VFS translation layer.** `VFSBackendBase` / `VFSWorkspaceBase` give a ~50-line path to a new backend that translates the three `BackendBase` primitives to anything (host I/O, in-memory tree, 9p, FUSE, ...). The shipped `agentfs` reference impl is the natural "theoretical upper bound" baseline for benchmarks and a zero-runtime CI / dev backend.
- **Real tests, no mocking.** The guest-agent wire protocol is exercised end-to-end against the exact handler source string that ships into the microVM (run on a Unix socket in CI since AF_VSOCK is unavailable). The Firecracker REST API client is driven against a real Unix-socket HTTP server. 150+ tests, 0 mocks.

## Quickstart

```bash
pip install agentscope-sandbox-ext[docker-runtime]
```

```python
import asyncio
from agentscope_sandbox_ext import (
    GVisorWorkspaceManager,
    IsolationPolicy,
)

async def main():
    async with GVisorWorkspaceManager(
        basedir="/tmp/as-workspaces",
        isolation=IsolationPolicy.PER_AGENT,
    ) as mgr:
        ws = await mgr.get_workspace(
            user_id="alice",
            agent_id="bash",
            session_id="s1",
        )
        result = await ws.get_backend().exec_shell(
            ["sh", "-c", "echo hello from gVisor"],
        )
        print(result.stdout.decode())

asyncio.run(main())
```

## Architecture

```
                 ┌─────────────────────────────────────────────┐
                 │           agentscope.app (host)              │
                 │  WorkspaceManagerBase  ← SandboxExtManagerBase
                 └───────────────┬─────────────────────────────┘
                                 │  get_workspace / close / close_all
   ┌─────────────────────────────┼─────────────────────────────┐
   ▼                             ▼                             ▼
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ FirecrackerMgr   │    │  GVisorMgr       │    │   KataMgr        │    │  SysboxMgr       │
│  + SandboxPool    │    │  + SandboxPool   │    │   + SandboxPool   │    │  + SandboxPool   │
└────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
         │                       │                       │                       │
         ▼                       ▼                       ▼                       ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│FirecrackerWorkspace│  │ GVisorWorkspace  │   │  KataWorkspace    │   │  SysboxWorkspace │
│  (microVM + vsock) │   │  (Docker + runsc)│   │  (Docker + kata)  │   │  (Docker + sysbox)│
└────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
         │                       │                       │                       │
         ▼                       ▼                       ▼                       ▼
   SandboxedWorkspaceExtBase  (sandbox_kind + metrics + verify_runtime_available)
         │
         ▼
   SandboxedWorkspaceBase  ← agentscope native (gateway bootstrap, MCP, skills)
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design.

## Firecracker prerequisites

Firecracker needs a Linux host with KVM, a kernel image and an ext4 rootfs:

```bash
# /dev/kvm must be accessible
test -w /dev/kvm && echo OK

# firecracker binary on $PATH
firecracker --version

# Kernel + rootfs (build with tools/build-rootfs.sh, see docs/firecracker.md)
ls /var/lib/firecracker/vmlinux /var/lib/firecracker/rootfs.ext4
```

The in-VM guest agent (`src/agentscope_sandbox_ext/_firecracker/_guest_agent.py`) must be baked into the rootfs at `/root/.agentscope/_guest_agent.py` and started by init. The `tools/build-rootfs.sh` helper does this.

## gVisor / Kata / Sysbox prerequisites

gVisor, Kata and Sysbox are Docker *runtimes* — register them in `/etc/docker/daemon.json`:

```json
{
  "runtimes": {
    "runsc": { "path": "/usr/bin/runsc" },
    "kata-fc": { "path": "/usr/bin/kata-fc" },
    "sysbox-runc": { "path": "/usr/bin/sysbox-runc" }
  }
}
```

Restart the daemon, then verify:

```bash
docker info --format '{{json .Runtimes}}'
```

## Testing

```bash
pip install -e ".[test]"
pytest -q
```

Tests never mock — they spin up real protocol peers (a Unix-socket guest-agent server using the exact handler source string, a Unix-socket HTTP server for the Firecracker API, a real vsock-bridge handshake server). Runtime-probe tests that need Docker/Firecracker installed are skipped automatically when the binary is absent.

## License

MIT
