# Sandbox coverage and performance optimization opportunities

English | [简体中文](PERFORMANCE_zh.md)

This document analyses (1) which sandbox backends the package currently supports, (2) which mainstream candidates are still missing and worth adding, and (3) where the pooling / caching / scheduling layer can be tightened for performance.

## 1. Currently supported sandboxes

### 1.1 This package (`agentscope-sandbox-ext`)

| Backend | `sandbox_kind` | Class | Isolation class | Cold boot | Status |
|---|---|---|---|---|---|
| Firecracker microVM | `firecracker` | `FirecrackerWorkspace` | KVM-grade VM, separate kernel | ~125 ms | implemented |
| gVisor (runsc) | `gvisor` | `GVisorWorkspace` | application-kernel (Sentry) | container-fast | implemented |
| Kata Containers | `kata` | `KataWorkspace` | VM-backed container | ~1 s | implemented |
| Sysbox | `sysbox` | `SysboxWorkspace` | Docker runtime (stronger isolation than runc) | container-fast | implemented (this branch) |

All four inherit from the unified `SandboxedWorkspaceExtBase` so a management plane can route by `isinstance` and call `verify_runtime_available()` before provisioning.

### 1.2 agentScope native backends (for reference)

These are *not* reimplemented here because agentScope already ships them:

| Backend | Reason for not re-implementing |
|---|---|
| Docker (`DockerWorkspace`) | The gVisor / Kata / Sysbox backends **inherit** this class and only override `HostConfig.Runtime`. |
| E2B | Cloud sandbox, no host runtime needed. |
| Kubernetes (`K8sWorkspace`) | Cluster-scope, orthogonal to host-runtime backends. |
| Apple Container | macOS only. |
| Bubblewrap | Linux namespace jailer — covers a similar niche to nsjail. |
| Daytona / OpenSandbox | Cloud sandboxes. |

## 2. Candidates for future backends

Each candidate is graded on isolation strength, cold-boot cost, fit with the `SandboxedWorkspaceBase` contract (POSIX shell + Python venv + MCP gateway), and implementation effort.

### 2.1 Cloud Hypervisor (microVM, recommended next)

- **Isolation:** KVM-grade VM (same class as Firecracker).
- **Cold boot:** ~150 ms — comparable to Firecracker.
- **Fit:** Excellent — same vsock / guest-agent transport as Firecracker, full POSIX guest.
- **Differentiator vs Firecracker:** device hotplug, live migration, virtiofs support, more flexible block layout. Worth ~80 % code reuse from the Firecracker backend.
- **Effort:** Medium — spawn `cloud-hypervisor` instead of `firecracker`, drive its `--api-socket` REST API (similar shape), reuse the vsock guest agent verbatim.

### 2.2 WASM/WASI (Wasmtime) — *not recommended for the workspace contract*

- **Isolation:** Strong (capability-based, no syscalls by default).
- **Cold boot:** <10 ms.
- **Fit:** **Poor.** `SandboxedWorkspaceBase` assumes a POSIX shell, a Python venv and an MCP gateway process. WASI has neither a shell nor a Python runtime. Fitting the workspace contract would require a custom WASI host that emulates `/bin/sh`, file I/O and a Python interpreter — well outside the scope of "add a backend".
- **Verdict:** Useful as a future *side-channel* executor for short-lived tool calls, but not as a `SandboxedWorkspaceBase` subclass. Document and skip.

### 2.3 nsjail (Linux jailer)

- **Isolation:** Linux namespaces + seccomp — same class as Bubblewrap.
- **Fit:** Good (POSIX guest).
- **Verdict:** agentScope already ships Bubblewrap, which covers the same niche. **Skip** unless nsjail's per-call resource limits are required.

### 2.4 Podman (rootless container engine)

- **Isolation:** Container (runc by default, can pair with runsc/kata).
- **Fit:** Good, but Podman does not expose a Docker-compatible API by default (`podman.sock` differs enough that `aiodocker` cannot drive it without a compat shim).
- **Effort:** High — would need a new `PodmanWorkspace` that bypasses `DockerWorkspace`.
- **Verdict:** Document as future work; do not implement now.

### 2.5 QEMU direct (full-system emulation)

- **Isolation:** VM.
- **Fit:** Good but heavy.
- **Verdict:** Kata already covers the QEMU-backed case via `kata-qemu`. **Skip.**

### 2.6 gVisor + Kata coverage already complete

gVisor (`runsc`) and Kata (`kata-fc`, `kata-qemu`, `kata-clh`) are the two mainstream VM/SCS Docker runtimes. No further Docker-runtime backend is needed.

## 3. Performance optimization opportunities

This section identifies concrete, non-breaking optimisations in the pooling / caching / scheduling layer. Items marked **(implemented in this branch)** are already done; the rest are documented as a roadmap.

### 3.1 Pool acquisition strategy — **(implemented)**

The pool popped sandboxes from the left of a `deque` (FIFO). FIFO ages every pooled sandbox evenly, so under low traffic every sandbox hits the idle TTL at roughly the same time and the pre-warmer has to replace all of them at once.

**Implemented:** `acquire_strategy` parameter accepting `"fifo"` (default, age-out evenly) or `"lifo"` (newest-first, keeps the warmest sandbox hot). LIFO trades even-ageing for cache-locality — the most recently returned sandbox is the most likely to still have a warm page cache and live gateway connection.

### 3.2 Liveness probe on acquire — **(implemented)**

Previously `acquire` returned any sandbox whose `is_alive` flag was `True` at the time it was returned. A sandbox that crashed *while pooled* (gateway OOM, microVM panic) would be handed to the next caller, who would then have to detect the failure and call `release(broken=True)` themselves.

**Implemented:** optional `health_check` callback on the pool. When set, `acquire` invokes it on each candidate before returning; failed candidates are torn down and the pool tries the next. This makes the "broken pooled sandbox" case invisible to callers.

### 3.3 Concurrent provision limiter — **(implemented)**

When `N` coroutines all missed the pool simultaneously (e.g. a burst of session creations at startup), the original code started `N` parallel provisions. For Firecracker that meant `N` microVM boots at once — KVM scheduler thrash and rootfs I/O contention.

**Implemented:** `max_concurrent_provisions` parameter backed by an `asyncio.Semaphore`. Default `0` (unlimited, preserves old behaviour); set to `1`–`N` to serialise / bound concurrent boots.

### 3.4 Stale-on-acquire eviction (roadmap)

A pooled sandbox that has been idle for `idle_ttl / 2` is still "fresh" enough to serve, but one that has been idle for almost `idle_ttl` is risky (long-idle microVMs sometimes have stale clock / dropped gateway connections). A `max_age` cap that evicts *before* returning would let the pool serve only fresh sandboxes. Not yet implemented — left as a roadmap item because the liveness probe (3.2) covers the safety side, and the sweeper already bounds the age.

### 3.5 Per-user / per-agent pool partitioning (roadmap)

The pool is currently global — a single free list shared across all tenants. On a multi-tenant host a noisy tenant can drain the pool and starve others. Partitioning the free list by `user_id` (or by a sharding key) with per-shard `max_size` would give every tenant a guaranteed floor of warm sandboxes. Not implemented here because it requires API changes to the manager (`get_workspace` would need to pass the partition key to `acquire`).

### 3.6 Image-build cache sharing (already handled by agentscope)

`DockerWorkspace` already content-hashes the Dockerfile + COPY files and skips rebuilds on tag hit, so gVisor / Kata / Sysbox inherit image-build caching for free. No action needed.

### 3.7 Manager-level cache eviction policy (roadmap)

The manager's `_cache` is a plain `dict` with TTL-based eviction. Under memory pressure a strict-LRU eviction (evict least-recently-used first when above a hard cap) would be better than the current "evict only when expired" policy. Low priority — the TTL sweeper already bounds the cache in practice.

### 3.8 Pre-warm burst prediction (roadmap)

The pre-warmer is reactive (it tops up to `min_warm` whenever it ticks). A predictive variant that observes acquire rate and pre-warms *ahead* of a predicted burst would smooth startup spikes on bursty workloads. Tricky to tune; left as a roadmap item.

### 3.9 Pool metrics already in place

`SandboxPool.metrics()` reports `warm`, `in_use`, `total`, `max_size`, `min_warm`, `closed`. `manager_metrics()` extends this with `backend_kind` and `cache_size`. No further instrumentation needed for basic observability; richer metrics (provision latency histogram, acquire-wait histogram) would require a stats library and are out of scope.

## 4. Summary

| Area | Status |
|---|---|
| Backends | Firecracker, gVisor, Kata (existing) + Sysbox (this branch). Cloud Hypervisor is the recommended next backend; WASM and nsjail are not a good fit; Podman is a future project. |
| Pooling | LIFO/FIFO strategy, liveness probe, concurrent provision limiter (this branch). Partitioning and predictive pre-warm are roadmap items. |
| Caching | TTL sweeper + content-hashed image build (already in place). Strict-LRU is a roadmap item. |
| Scheduling | Condition-based acquire back-pressure with timeout (already in place). Per-tenant partitioning is a roadmap item. |
