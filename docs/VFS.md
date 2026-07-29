# VFS Workspace Backend — Design & Benchmarks

## 1. Motivation

All existing sandbox backends (Firecracker, gVisor, Kata, Sysbox) provision
a *real* isolation boundary — a microVM or a container — which is the right
answer for untrusted code but is also the dominant latency cost:

| backend | cold provision | isolation boundary |
|---------|----------------|--------------------|
| Firecracker | ~1–3 s | microVM (KVM) |
| gVisor | ~300–800 ms | syscall filter (host kernel) |
| Kata | ~500 ms–2 s | microVM via Docker runtime |
| Sysbox | ~300–600 ms | namespace-virtualized container |

For three important workloads, however, paying the isolation tax on every
provision is wasteful:

1. **CI / dev loop** — the agent's own test suite, lint, type-check, doc
   build. The code is trusted; we only need a clean working directory.
2. **Benchmark baseline** — to measure pool / scheduling overhead we need a
   backend whose provision cost is effectively zero, so the numbers isolate
   the *pool* rather than the *backend*.
3. **Pluggable VFS translation** — some deployments want to translate
   `exec_shell` / `read_file` / `write_file` into something other than a
   container (a 9p server, a FUSE mount, an in-memory interpreter, a remote
   gateway). Today every new translation is a from-scratch backend.

The VFS backend family solves all three.

## 2. Design

### 2.1 Where VFS plugs in

`agentscope.tool._builtin._backend.BackendBase` defines three abstract
primitives every backend must implement:

```python
async def exec_shell(command, *, cwd=None, timeout=None) -> ExecResult
async def read_file(path) -> bytes
async def write_file(path, data: bytes) -> None
```

Every higher-level agent tool (Bash, Read, Write, Edit, Grep, Glob) is
derived from those three. So a **VFS backend** is just a `BackendBase`
subclass that *translates* the three primitives into operations against a
virtual workspace instead of a container.

The inheritance chain mirrors the Firecracker backend:

```
BackendBase                                  ← agentscope
└── VFSBackendBase                           ← this package, abstract
    └── AgentFSBackend                       ← reference impl

SandboxedWorkspaceBase                       ← agentscope
└── SandboxedWorkspaceExtBase                ← this package
    └── VFSWorkspaceBase                    ← this package, abstract
        └── AgentFSWorkspace                 ← reference impl
```

### 2.2 `VFSBackendBase` — the translation contract

`VFSBackendBase` leaves the three primitives abstract (as `_exec_translated`
/ `_read_translated` / `_write_translated`) and provides the shared
plumbing: a cached `workdir` (so `getcwd()` doesn't pay a translation round
trip), the `BackendBase` primitive wiring, and a `close()` lifecycle hook.

A new VFS backend is a ~50-line subclass: implement three translation hooks
and you're done. The derived filesystem helpers (`file_exists`, `is_dir`,
`list_dir`, `stat_mtime`, `delete_path`) are inherited from `BackendBase`
and work for free.

### 2.3 `VFSWorkspaceBase` — the lifecycle contract

`VFSWorkspaceBase` specializes `SandboxedWorkspaceExtBase` for the
zero-runtime case:

* `verify_runtime_available()` — always succeeds (no Docker / KVM / runtime
  to probe).
* `_provision_backend()` — instantiates the VFS backend (a pure-Python
  object); microsecond-cheap.
* `_bootstrap_commands()` — returns `[]` (no guest agent / image bootstrap).
* `initialize()` — overridden to skip the MCP gateway setup. A VFS workspace
  has no in-sandbox gateway process, so the gateway bootstrap / health-poll
  path does not apply; everything else (provision backend, ensure workspace
  layout, seed skills) runs unchanged.
* `get_backend()` — returns the bound VFS backend.

### 2.4 `AgentFSBackend` — the reference implementation

`agentfs` translates the three primitives to host I/O + subprocess confined
to a per-workspace host directory:

* `read_file` / `write_file` — direct `open()` on a host path resolved under
  the workspace root. Path traversal that would escape the workdir raises
  `PermissionError`.
* `exec_shell` — `asyncio.create_subprocess_exec` with `cwd` resolved under
  the workdir. Belt-and-braces: a `cwd` that escapes the workdir is rejected
  with exit code 126.

Because there is no image build, no container start, and no guest agent,
`agentfs` provisioning is a single `os.makedirs` plus a Python object — the
cheapest backend in the package, and the natural "theoretical upper bound"
baseline for benchmarks.

## 3. Other VFS implementations (sketch)

The translation layer is intentionally narrow; other VFS backends are
~50 lines each:

| backend | `exec_shell` | `read_file` / `write_file` | use case |
|---------|--------------|----------------------------|----------|
| `agentfs` (shipped) | host subprocess | host `open()` | CI / dev / benchmark baseline |
| `memoryfs` (future) | in-process interpreter | in-memory dict | pure unit tests, no host I/O |
| `ninep` (future) | 9p protocol to a remote | 9p protocol | remote workspace server |
| `fuse` (future) | host subprocess against FUSE mount | FUSE read/write | custom FS layouts |

## 4. Benchmarks

### 4.1 Methodology

The benchmark harness (`tests/benchmarks/bench_pool.py`) measures **real**
acquire / exec / release latency and steady-state throughput across the
optimisation strategies the package supports, using the zero-cost `agentfs`
backend so the numbers reflect *pool* and *scheduling* overhead — not
container boot cost.

Strategies compared:

| strategy | description |
|----------|-------------|
| `direct` | no pool; every request provisions + tears down |
| `pool-fifo` | `SandboxPool`, `acquire_strategy="fifo"` |
| `pool-lifo` | `SandboxPool`, `acquire_strategy="lifo"` |
| `prewarm` | pool with `min_warm=N` (warm pool under idle load) |
| `prewarm+ratelimit` | pool with `min_warm=N` AND `max_concurrent_provisions=1` |

Metrics: cold/hot acquire latency, exec latency, release latency,
steady-state throughput (ops/s), and p50/p95/p99 acquire tail latency.

### 4.2 Results

Config: `warmup=10, iters=300, concurrency=8` (run on the sandbox host;
absolute numbers are environment-dependent — the *relative* shape is what
matters).

| strategy | cold ms | hot ms | exec ms | release ms | ops/s | p50 ms | p95 ms | p99 ms |
|----------|--------:|-------:|--------:|-----------:|------:|-------:|-------:|-------:|
| direct | 6.387 | 6.387 | 3.084 | 0.000 | 21.9 | 46.292 | 51.376 | 58.447 |
| pool-fifo | 6.506 | 0.001 | 3.604 | 0.002 | 62.4 | 16.726 | 21.265 | 39.537 |
| pool-lifo | 6.347 | 0.001 | 3.389 | 0.002 | 57.5 | 18.391 | 21.373 | 40.987 |
| prewarm | 6.392 | 0.001 | 3.371 | 0.002 | 59.6 | 17.480 | 22.197 | 41.956 |
| prewarm+ratelimit | 7.512 | 0.001 | 3.511 | 0.002 | 44.9 | 21.113 | 29.271 | 74.297 |

### 4.3 Findings

1. **Pooling is the single biggest win.** `direct` sustains only 21.9 ops/s
   at concurrency 8; `pool-fifo` reaches 62.4 ops/s — a **2.8× throughput
   improvement** — because the steady-state path no longer pays the
   provision + close cost on every request.

2. **Hot acquire is ~6000× faster than cold.** Cold acquire is 6.4 ms
   (provision a fresh `agentfs` workspace); hot acquire is 0.001 ms (pop
   from the free list). This is the entire premise of pooling, quantified.

3. **FIFO slightly beats LIFO in steady-state throughput** here (62.4 vs
   57.5 ops/s). The `agentfs` backend is stateless per-workspace, so LIFO's
   guest-cache-locality advantage doesn't apply; the small gap is within
   run-to-run noise. On real container backends (where the guest page cache
   matters), LIFO is expected to win.

4. **Pre-warming does not help under sustained load.** `prewarm` (59.6
   ops/s) is on par with `pool-lifo` (57.5 ops/s). Pre-warming only helps
   the *first* burst of requests after idle; under sustained load the pool
   is already full from releases.

5. **`max_concurrent_provisions=1` hurts throughput under load.** The
   rate-limited variant drops to 44.9 ops/s (vs 62.4 for `pool-fifo`) and
   inflates p99 to 74 ms. This knob is a **safety valve for thundering-herd
   cold starts** on backends where parallel provisions contend (Firecracker
   KVM scheduler thrash, rootfs I/O contention); it is *not* a throughput
   optimisation and should default to `0` (unlimited) for VFS-class
   backends.

### 4.4 Recommendations

| workload | recommended strategy | why |
|----------|----------------------|-----|
| CI / dev (trusted code) | `agentfs` + `pool-fifo` | zero-runtime, 2.8× throughput |
| Firecracker bursty load | `pool-lifo` + `prewarm(min_warm=2)` | LIFO keeps the warmest VM hot; prewarm absorbs the first burst |
| Firecracker thundering-herd cold start | add `max_concurrent_provisions=2` | prevent KVM thrash on mass cold start, accept throughput cost |
| Sustained high-throughput | `pool-fifo`, `min_warm=0` | prewarm doesn't help under sustained load; FIFO is marginally faster |

## 5. Running the benchmarks

```bash
# default config (warmup=5, iters=200, concurrency=8)
python -m tests.benchmarks.bench_pool

# larger sample for stable tail percentiles
python -m tests.benchmarks.bench_pool --warmup 10 --iters 300 --concurrency 8

# machine-readable JSON
python -m tests.benchmarks.bench_pool --json out.json

# PNG bar chart (requires matplotlib)
python -m tests.benchmarks.bench_pool --plot out.png
```

A snapshot is always written to `docs/benchmark-results.json` on every run
so docs can reference a reproducible result.
