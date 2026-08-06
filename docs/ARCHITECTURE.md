# Architecture

English | [简体中文](ARCHITECTURE_zh.md)

This document describes the design of `agentscope-sandbox-ext` — how the five sandbox backends (Firecracker, gVisor, Kata Containers, Sysbox, and the zero-runtime VFS / `agentfs`) plug into agentScope without modifying any native code, and how the pooling layer composes in front of them.

## Design constraints

The package was designed against four hard constraints from the task brief:

1. **No native code modification.** Only `import` from agentscope.
2. **A single inherited interface** so a future management plane can route by `isinstance` rather than backend-specific branching.
3. **Pooling** as an opt-in optimisation layer, composable in front of any manager.
4. **Real tests, no mocking.** The wire protocols are exercised end-to-end against real protocol peers.

## Layered composition

```
            ┌──────────────────────────────────────────────────────────────────┐
            │           agentscope.app  (host)                                 │
            │                                                                  │
            │   WorkspaceManagerBase          SandboxedWorkspaceBase           │
            │   (abstract manager)            (template-method lifecycle)      │
            │          ▲                              ▲                       │
            │          │ subclass                    │ subclass               │
            └──────────┼──────────────────────────────┼───────────────────────┘
                       │                              │
            ┌──────────┴──────────────┐    ┌──────────┴──────────────────────┐
            │  SandboxExtManagerBase   │    │  SandboxedWorkspaceExtBase      │
            │   + backend_kind         │    │   + sandbox_kind                │
            │   + manager_metrics()    │    │   + metrics()                   │
            │   (this package)         │    │   + verify_runtime_available()  │
            └─────┬──────┬─────┬───────┘    └──┬──────┬─────┬─────┬───────────┘
                  │      │     │               │      │     │     │
            ┌─────┴─┐ ┌──┴──┐ ┌─┴───┐ ┌──────┐ ┌┴────┐ ┌─┴──┐ ┌─┴──┐ ┌─┴────┐
            │ FC Mgr│ │GVis │ │Kata │ │Sysbox│ │FC WS│ │GVis│ │Kata│ │Sysbox│
            │       │ │ Mgr │ │ Mgr │ │ Mgr  │ │     │ │ WS │ │ WS │ │  WS  │
            └───┬───┘ └──┬──┘ └──┬──┘ └──┬───┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬───┘
                │        │       │      │       │       │      │      │
                └────────┴───────┴──────┴───────┴───────┴──────┴──────┘
                                  │
                          (optional) SandboxPool
                                  │
                          ┌───────┴────────┐
                          │  pre-warm loop │
                          │  idle sweeper  │
                          │  capacity cap  │
                          └────────────────┘

        Note: `AgentFSWorkspace` (the VFS backend) inherits directly from
        `SandboxedWorkspaceExtBase` without a manager — it is a
        workspace-only, zero-runtime baseline (no pool layer).
```

### Why two base classes instead of one?

agentScope splits workspace *lifecycle* (`SandboxedWorkspaceBase` — gateway bootstrap, MCP, skills) from workspace *ownership* (`WorkspaceManagerBase` — cache, TTL, isolation). The extension package mirrors that split so the inheritance graph stays shallow and there is no "god class" mixing concerns.

### The `verify_runtime_available` probe

Every backend implements a `classmethod async verify_runtime_available()` that raises `RuntimeError` with a human-readable message if the host cannot run it. Managers call it before attempting to provision so the error surfaces synchronously rather than halfway through VM boot.

| Backend | Probe |
|---|---|
| Firecracker | `firecracker --version` on `$PATH`, `/dev/kvm` writable, kernel + rootfs present |
| gVisor | `runsc --version` on `$PATH`, Docker daemon responds to `/v1.41/info` |
| Kata | `kata-runtime --version` on `$PATH`, Docker daemon responds |
| Sysbox | Docker daemon reports one of `sysbox-runc` / `sysbox` in `docker info --format '{{json .Runtimes}}'` |
| VFS (`agentfs`) | Always succeeds — pure Python, no host runtime needed |

## Firecracker backend

```
┌──────────── host ─────────────────────────────┐
│  FirecrackerWorkspace                         │
│    │                                          │
│    ├── spawn `firecracker --api-socket …`     │
│    ├── FirecrackerApi (Unix-socket HTTP)      │
│    │     PUT /boot-source                     │
│    │     PUT /machine-config                   │
│    │     PUT /drives/rootfs                    │
│    │     PUT /vsock  (CID = host-allocated)     │
│    │     POST /actions instance.start           │
│    │                                          │
│    └── GuestAgentClient (vsock → Unix socket)  │
│          exec_shell  / read_file / write_file  │
└──────────────────┬─────────────────────────────┘
                   │ virtio-vsock
┌──────────────────┴──────────── guest ──────────┐
│  init → python3 /root/.agentscope/_guest_agent.py
│     AF_VSOCK listening socket                   │
│     length-prefixed JSON protocol               │
│     exec_shell / read_file / write_file / ping  │
└──────────────────────────────────────────────────┘
```

The guest agent is a pure-stdlib Python script shipped inside the rootfs. It listens on `AF_VSOCK` and speaks a length-prefixed JSON protocol. The same source string is exercised by the test suite against a Unix-socket transport, so the wire format is verified end-to-end without needing a running microVM in CI.

## gVisor / Kata / Sysbox backends

These three reuse `agentscope.workspace.DockerWorkspace`'s entire flow (image build, bind-mount, gateway bootstrap) and only override `HostConfig.Runtime`:

```python
config = {
    "Image": self._image_tag,
    "HostConfig": {
        "Runtime": self._runtime,   # "runsc" | "kata-fc" | "sysbox-runc"
        "Binds": [...],
    },
    ...
}
```

This is the smallest possible delta — every other concern (gateway port, MCP persistence, skill seeding) is inherited unchanged.

The runtime-specific delta:

- **gVisor** (`runsc`) — application-level kernel; the Sentry intercepts syscalls in userspace, giving strong isolation without a VM.
- **Kata** (`kata-fc` / `kata-qemu` / ...) — each container runs inside a lightweight VM backed by a hardware hypervisor, combining VM-grade isolation with container ergonomics.
- **Sysbox** (`sysbox-runc` / `sysbox`) — per-container user/mount namespaces, virtualised `/proc` and `/sys`, and Docker-in-Docker without `--privileged`; lighter than a full VM.

## VFS / `agentfs` backend

`AgentFSWorkspace` is the zero-runtime baseline: it translates the `BackendBase` primitives (`exec_shell` / `read_file` / `write_file`) into host I/O + `asyncio.subprocess` confined to a per-workspace directory.

- **No Docker / Firecracker / Kata runtime to verify** — `verify_runtime_available()` always succeeds.
- **No image build, no container start, no guest agent** — provisioning is a single `os.makedirs` plus a Python object construction (microsecond-cheap).
- **No manager, no `SandboxPool`** — `AgentFSWorkspace` is constructed directly. The "always warm, always cheap" property makes pooling pointless.

This makes `agentfs` the natural CI / dev / benchmark baseline: it exercises the full sandboxed-workspace lifecycle (provision → initialize → exec → teardown) with real I/O and real subprocesses, but needs no host runtime. See `docs/VFS.md` for the design and `docs/PERFORMANCE.md` for its role as the "theoretical upper bound" in cold-boot benchmarks.

## Pooling layer

`SandboxPool` is a standalone building block — it does not itself implement `WorkspaceManagerBase`. A manager wires it in front of its cache:

```python
class FirecrackerWorkspaceManager(SandboxExtManagerBase):
    def __init__(self, ..., pool: SandboxPool | None = None):
        ...
        self._pool = pool or SandboxPool(factory=self._make, max_size=8, min_warm=2)
```

### Eviction paths

1. **Idle eviction** — a background sweeper closes any pooled sandbox whose `last_returned` timestamp is older than `idle_ttl`.
2. **Capacity cap** — `max_size` bounds the warm pool; excess returns are torn down immediately rather than queued.
3. **Wait path** — `acquire` blocks (with timeout) when no warm sandbox is available and the pool is already at `max_size`. This is the intended back-pressure path for a loaded manager.

### Concurrency

- Every public method is a coroutine and safe under concurrency; an `asyncio.Lock` guards mutation of the free list.
- The pre-warm task is best-effort: it tries to maintain `min_warm` ready sandboxes in the background, retrying with exponential back-off. Failures are logged and swallowed so a transient provision error never tears the pool down.
- `acquire` uses an `asyncio.Condition` so blocked callers wake promptly when a sandbox is returned.

## Testing strategy

Tests never mock. They spin up real protocol peers:

- The guest-agent wire protocol is exercised end-to-end against the exact handler source string that ships into the microVM. CI runs it on a Unix socket since `AF_VSOCK` is unavailable outside a VM.
- The Firecracker REST API client is driven against a real Unix-socket HTTP server built with `asyncio.start_server` — the same socket type Firecracker itself uses.
- The pool tests use a real (cheap) sandbox subclass that actually flips an `is_alive` flag and records every provision / close, so pre-warm, idle eviction, and capacity cap are all observed without mocks.
- Runtime-probe tests that need Docker / Firecracker installed are skipped automatically when the binary is absent (marked with `@pytest.mark.integration`).

## Why not just one backend?

The five backends cover distinct points in the isolation / overhead trade-off:

- **Firecracker** — strongest isolation (separate kernel), sub-second cold boot, but needs KVM and a rootfs.
- **gVisor** — container-fast, application-level kernel, no VM overhead, but Sentry is not a complete kernel.
- **Kata** — VM-grade isolation with container ergonomics, but heavier than gVisor.
- **Sysbox** — per-container user/mount namespaces and virtualised `/proc` / `/sys` (Docker-in-Docker without `--privileged`); sits between `runc` and Kata on the isolation axis.
- **VFS (`agentfs`)** — zero isolation, zero runtime, microsecond provisioning. The CI / dev / benchmark baseline — represents the "theoretical upper bound" of how fast a workspace can be when there is nothing to isolate.

A management plane can offer any subset and let the per-agent isolation policy pick — the unified `SandboxedWorkspaceExtBase` discriminator makes routing trivial.
