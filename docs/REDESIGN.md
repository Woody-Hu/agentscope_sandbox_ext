# Redesign: Modular Actor-Worker Runtime & Control-Plane Contract

English | [简体中文](REDESIGN_zh.md)

This document specifies the redesign of `agentscope-sandbox-ext` into a
**modular, layered runtime** whose public contract lets it flexibly
interoperate with an external control-plane system (a high-density,
Kubernetes-style agent runtime that multiplexes many logical "actors"
onto a smaller pool of pre-warmed "workers").

The redesign is motivated by a deep comparison with such a reference
system. Names used here are neutral (`actor`, `worker`, `template`,
`checkpoint`); the reference system's private identifiers are not
reused anywhere in code, docs, or identifiers.

---

## 1. Goals & non-goals

### Goals
1. **Flexible integration.** Define a stable, transport-agnostic
   control-plane contract (data model + RPCs) so this runtime can be
   *driven by* an external control plane **or** *drive* external
   workers. Either side is replaceable.
2. **Borrow proven mechanisms.** Adopt — and adapt to a single-process
   async Python context — five mechanisms from the reference system:
   - **Actor–worker isolation** (virtual-actor model: many logical
     instances ↔ few pre-warmed execution units, time-multiplexed).
   - **Singleflight** (per-key in-process call deduplication).
   - **Templates** (immutable definitions + a one-time *golden*
     snapshot that new instances clone-restore from).
   - **Worker pool** with constraint-based scheduling and locality.
   - **Snapshot & checkpoint** with a two-level (pause/suspend) model
     and golden/last snapshot semantics.
3. **Modularity.** Each concern is an independently importable module
   with a narrow interface; backends, pool, actor registry, scheduler,
   checkpoint store and control plane compose rather than nest.
4. **No regressions.** Existing backends (Firecracker / gVisor / Kata /
   Sysbox / VFS) and `SandboxPool` / `TieredSnapshotStore` keep working;
   new layers wrap them.

### Non-goals
- Not reimplementing Kubernetes control-plane semantics in Python.
- Not shipping a distributed lock / Redis registry as the default; the
  default registry & lock are in-process, with pluggable distributed
  backends for multi-replica deployments.
- Not building a network proxy / Envoy data plane; routing is an
  optional, pluggable resolver abstraction.

---

## 2. Deep comparison: current system vs reference system

| Dimension | Current system (`agentscope-sandbox-ext`) | Reference system | Gap / decision |
|---|---|---|---|
| **Logical vs physical unit** | A *workspace* ≡ one sandbox instance; no separation between "who the agent is" and "where it runs" | `Actor` (logical, sparse, idle) ↔ `Worker` (physical, pre-warmed, dense); one worker hosts ≤1 actor at a time; actors migrate across workers | **Introduce `Actor` + `Worker` split** (virtual-actor model). Time-multiplex workers across actors to raise density. |
| **Identity / addressing** | `(user_id, agent_id, session_id)` hashed into a workspace id; no global namespace | `(namespace, name)` global addressing; namespace is an isolation boundary distinct from K8s ns | Adopt `(namespace, name)` actor addressing; keep workspace-id as the *physical* handle. |
| **Pooling** | `SandboxPool`: pre-warm, idle eviction, cap, fifo/lifo, health-check, `max_concurrent_provisions` semaphore (thundering-herd *cap*, not *dedup*) | `WorkerPool` (CRD→Deployment); random-spreading scheduler with `RequiredNodes` locality for paused snapshots | **Extend pool → `WorkerPool`** with actor assignment + constraint scheduler; keep existing knobs. |
| **Request dedup** | Semaphore caps *concurrency* of provisions but N callers still each boot | `singleflight` collapses per-image/per-layer downloads; distributed lock collapses per-actor workflows across replicas | **Add `Singleflight`** for per-key dedup (provision, checkpoint, image/materialise). In-process by default; distributed lock is the cross-replica equivalent (pluggable). |
| **Definition of a workload class** | None — each workspace is provisioned ad-hoc from a backend + env | `ActorTemplate` (immutable; new version ⇒ new template); creation triggers a *golden snapshot* | **Add `ActorTemplate`** (immutable spec) + **golden snapshot** baking; new actors clone-restore from golden. |
| **Snapshot model** | Per-workspace `snapshot(tag)`/`restore(tag)`; VFS does deep-copy with atomic rename | `Full` (mem+rootfs delta+durable) vs `Data` (durable only); **Pause** (node-local, locality-priority resume) vs **Suspend** (remote upload); **Golden** (shared, per-template) vs **Last** (per-actor) | **Add two-level Pause/Suspend** + scope (Full/Data) + golden/last semantics on top of existing `SnapshotStore`. |
| **Snapshot storage** | `TieredSnapshotStore` (Local/PG/MinIO) with LRU/TTL eviction + promotion | Object storage (GCS/S3) + node-local cache; content-addressed | **Reuse `TieredSnapshotStore`** as the durable tier; add a node-local "pause" tier + content-addressing for golden snapshots. |
| **State store / concurrency** | In-process dict cache + `asyncio.Lock`/`Condition` | Redis/Valkey for high-freq records; optimistic version + distributed lock (auto-renew, ctx-cancel on loss) | Keep in-process default; **abstract `ActorRegistry` + `LockProvider`** so a Redis/Valkey impl can drop in for multi-replica. Optimistic version on actor records. |
| **Multi-backend abstraction** | `SandboxedWorkspaceExtBase` + `sandbox_kind` discriminator; 5 backends | `SandboxClass` (gvisor/microvm) + `SandboxConfig` (cluster assets); snapshots never cross class | Introduce **`SandboxClass`** as a first-class scheduling constraint; bind class to template & worker; snapshots are class-scoped. |
| **Lifecycle states** | `is_alive` bool + `initialize`/`close` | `SUSPENDED→RESUMING→RUNNING→SUSPENDING→SUSPENDED` state machine | **Add `ActorStatus` state machine**; map workspace `is_alive` to `RUNNING`. |
| **Integration surface** | Python `async with Manager` only | gRPC control plane + K8s CRDs + node-level gRPC | **Define a `ControlPlane` contract** (data model + RPCs) with an in-process default and pluggable transport (HTTP/gRPC). This is the interoperability boundary. |
| **On-demand activation** | Eager `get_workspace` | Location-transparent DNS + proxy ext_proc triggers `ResumeActor` on first request | **Add optional `ActorResolver`** hook (on-resume trigger); no built-in proxy. |

### Mechanism-by-mechanism verdict
- **Actor–worker isolation**: high value, currently absent → **adopt**.
  Adaptation: a worker wraps an existing `SandboxedWorkspaceExtBase`;
  "suspend" = snapshot + release worker back to pool; "resume" = acquire
  worker + restore. This is the single biggest density win.
- **Singleflight**: high value, small, composable → **adopt** as a
  generic utility used by provision/checkpoint/materialise. The existing
  `max_concurrent_provisions` *cap* stays (it bounds resource pressure);
  singleflight *dedups* identical in-flight work — they compose.
- **Template + golden snapshot**: high value for cold-start elimination
  → **adopt**. A template bakes a golden snapshot once; new actors
  clone-restore (sub-second) instead of cold-booting.
- **Worker pool + scheduling**: extend existing `SandboxPool` → **adopt**
  constraint scheduler (sandbox-class, template selector, locality).
- **Snapshot & checkpoint two-level**: high value for fast pause +
  cross-node migrate → **adopt** Pause (node-local) / Suspend (remote)
  on top of `TieredSnapshotStore`.

---

## 3. Modular architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Control Plane Contract  (controlplane/)                              │
│  ControlPlane ABC + messages (data model) + in-process default        │
│  RPCs: CreateActor/Resume/Suspend/Pause/Delete/Get/List,             │
│        CreateTemplate, ListWorkers, GetActorSnapshot ...              │
│  Pluggable transport: in-process (default) | HTTP | gRPC              │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ drives
┌──────────────────────────────▼───────────────────────────────────────┐
│  Actor Lifecycle  (actor/)                                            │
│  ActorRegistry ABC (+in-process)  ActorLifecycle (state machine)      │
│  Optimistic version + LockProvider (+in-process keyed lock)           │
└──────────┬───────────────────────────────────┬───────────────────────┘
           │ schedules                         │ checkpoints
┌──────────▼─────────────────┐  ┌─────────────▼─────────────────────────┐
│  Scheduler  (actor/)       │  │  Checkpoint Manager  (checkpoint/)     │
│  Constraints → Worker      │  │  Pause (node-local) / Suspend (remote) │
│  random-spreading + locality│  │  Golden / Last  | scope Full/Data     │
│  Singleflight (provision)  │  │  Singleflight (per-actor checkpoint)   │
└──────────┬─────────────────┘  └─────────────┬─────────────────────────┘
           │ acquires/releases worker           │ put/get snapshot
┌──────────▼───────────────────────────────────▼─────────────────────────┐
│  Worker Pool  (worker/)                                                 │
│  WorkerPool (extends SandboxPool) + Worker record + SandboxClass        │
│  SandboxRuntime adapter: wraps SandboxedWorkspaceExtBase backends       │
└──────────┬─────────────────────────────────────────────────────────────┘
           │ provisions / execs / snapshots
┌──────────▼─────────────────────────────────────────────────────────────┐
│  Backends  (existing, unchanged)                                        │
│  Firecracker · gVisor · Kata · Sysbox · VFS/agentfs                     │
└──────────┬─────────────────────────────────────────────────────────────┘
           │ snapshot artifacts
┌──────────▼─────────────────────────────────────────────────────────────┐
│  Snapshot Storage  (existing _runtime/, extended)                       │
│  SnapshotStore ABC · Local · Postgres · MinIO · TieredSnapshotStore     │
│  + node-local Pause tier + content-addressed golden snapshot refs       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Modules (new)
```
src/agentscope_sandbox_ext/
  _actor/            # Actor, ActorTemplate, ActorSnapshot, Namespace, registry, lifecycle, scheduler
    _types.py        # data model (frozen dataclasses): ActorRef, Constraints, ActorRecord, ActorSnapshotRef, SandboxClass
    _registry.py     # ActorRegistry ABC + InProcessActorRegistry + InProcessLockProvider
    _lifecycle.py    # ActorLifecycle: state machine + resume/suspend/pause orchestration
    _scheduler.py    # Scheduler: Constraints + random-spreading + locality
  _worker/
    _types.py        # Worker, WorkerFactory, SandboxRuntime protocol
    _pool.py         # WorkerPool: pre-warmed pool with constraint-based scheduling
    _runtime.py      # (reserved) SandboxRuntime adapter for SandboxedWorkspaceExtBase
  _checkpoint/
    _types.py        # CheckpointConfig, PauseSnapshotNotLocal
    _singleflight.py # Singleflight generic utility
    _manager.py      # CheckpointManager: pause/suspend/resume/bake_golden over SnapshotStore
  _template/
    _template.py     # ActorTemplateRecord, TemplateRegistry, TemplateBaker (golden-snapshot baking)
```

> **Note:** The `_controlplane/` module (ControlPlane ABC + messages +
> in-process default) is specified in §4 as the interop boundary. In the
> current implementation, `ActorLifecycle` *is* the in-process
> realisation of that contract — its public methods
> (`create_actor`/`resume_actor`/`suspend_actor`/`pause_actor`/
> `delete_actor`/`get_actor`/`list_actors`) map 1:1 to the RPC surface.
> A thin `InProcessControlPlane` adapter that delegates to
> `ActorLifecycle` + `WorkerPool` + `CheckpointManager` can be added
> when a transport (HTTP/gRPC) is needed; the data model in §4.1 is
> already implemented in `_actor/_types.py` and `_template/_template.py`.

Existing modules (`_base`, `_pool`, `_vfs`, `_firecracker/...`,
`_runtime/...`) are reused as-is or wrapped; no breaking changes.

### Composition rules
- A **backend** produces a `SandboxedWorkspaceExtBase`. The
  `SandboxRuntime` adapter exposes a uniform `provision/exec/snapshot/
  restore/suspend/resume/close` surface over any backend.
- A **`WorkerPool`** owns pre-warmed `Worker`s (each wrapping a runtime
  adapter). It extends `SandboxPool` (keeps pre-warm/idle-eviction/cap)
  and adds actor-assignment bookkeeping + a scheduler hook.
- **`ActorLifecycle`** owns the state machine and orchestrates
  resume/suspend/pause by *borrowing* a worker from the pool, calling
  the checkpoint manager, and updating the registry. It never touches
  backends directly.
- **`CheckpointManager`** sits above `SnapshotStore` and the node-local
  pause tier; it implements Pause (local) / Suspend (remote) and
  golden/last selection. `Singleflight` dedups per-actor checkpoints.
- **`ControlPlane`** is the only outward-facing surface: it translates
  `messages` into `ActorLifecycle` / `WorkerPool` / `CheckpointManager`
  calls. External drivers talk to it; the in-process default wires
  everything together.

---

## 4. Interface specification (control-plane contract)

The contract is **transport-agnostic**: the same `messages` data model
serialises to JSON (HTTP) or protobuf (gRPC). The default transport is
in-process. Every RPC is `async`.

### 4.1 Data model (`_controlplane/_messages.py`)

```python
@dataclass(frozen=True)
class ActorRef:
    namespace: str          # isolation boundary; DNS-1123
    name: str               # DNS-1123; actor identity = (namespace, name)

@dataclass(frozen=True)
class TemplateRef:
    name: str               # immutable template name
    version: int            # immutable; bump ⇒ new template

@dataclass(frozen=True)
class SandboxClass:         # enum-like
    value: str              # "firecracker" | "gvisor" | "kata" | "sysbox" | "vfs"

@dataclass(frozen=True)
class Constraints:
    sandbox_class: SandboxClass
    template_selector: dict[str, str]   # labels matched on worker
    actor_selector: dict[str, str]      # labels matched on worker
    required_nodes: tuple[str, ...] = ()# locality: prefer these nodes (pause locality)

@dataclass(frozen=True)
class CreateActorRequest:
    ref: ActorRef
    template: TemplateRef
    constraints: Constraints
    tags: dict[str, str] = field(default_factory=dict)

@dataclass(frozen=True)
class ResumeActorRequest:
    ref: ActorRef
    prefer_node: str | None = None       # hint for locality
    timeout: float = 60.0

@dataclass(frozen=True)
class SuspendActorRequest:
    ref: ActorRef
    scope: str = "full"                  # "full" | "data"

@dataclass(frozen=True)
class PauseActorRequest:
    ref: ActorRef
    scope: str = "full"                  # pause is node-local

@dataclass(frozen=True)
class ActorSnapshotRef:
    snapshot_id: str                     # content-addressed or unified ref
    kind: str                            # "golden" | "last" | "pause"
    scope: str                           # "full" | "data"
    node: str | None = None              # node-local for pause

@dataclass
class ActorRecord:
    ref: ActorRef
    template: TemplateRef
    status: str                          # SUSPENDED|RESUMING|RUNNING|SUSPENDING
    version: int                         # optimistic concurrency
    worker_id: str | None = None         # current assignment
    last_snapshot: ActorSnapshotRef | None = None
    created_at: float = 0.0
    updated_at: float = 0.0

@dataclass
class WorkerRecord:
    worker_id: str
    pool: str
    sandbox_class: SandboxClass
    status: str                          # ACTIVE|BUSY|DRAINING
    node: str
    labels: dict[str, str]
    assigned_actor: ActorRef | None = None

@dataclass
class TemplateRecord:
    ref: TemplateRef
    sandbox_class: SandboxClass
    golden_snapshot: ActorSnapshotRef | None
    phase: str                           # Initial|Baking|Ready|Failed
    spec: dict                           # backend-specific provision spec
```

### 4.2 RPC surface (`_controlplane/_contract.py`)

```python
class ControlPlane(abc.ABC):
    # ── templates ──
    @abstractmethod
    async def create_template(self, req: "CreateTemplateRequest") -> "TemplateRecord": ...
    @abstractmethod
    async def get_template(self, ref: "TemplateRef") -> "TemplateRecord": ...

    # ── actors ──
    @abstractmethod
    async def create_actor(self, req: CreateActorRequest) -> ActorRecord: ...
    @abstractmethod
    async def resume_actor(self, req: ResumeActorRequest) -> ActorRecord: ...
    @abstractmethod
    async def suspend_actor(self, req: SuspendActorRequest) -> ActorRecord: ...
    @abstractmethod
    async def pause_actor(self, req: PauseActorRequest) -> ActorRecord: ...
    @abstractmethod
    async def delete_actor(self, ref: ActorRef) -> None: ...
    @abstractmethod
    async def get_actor(self, ref: ActorRef) -> ActorRecord: ...
    @abstractmethod
    async def list_actors(self, namespace: str) -> list[ActorRecord]: ...

    # ── workers ──
    @abstractmethod
    async def list_workers(self, pool: str | None = None) -> list[WorkerRecord]: ...

    # ── snapshots ──
    @abstractmethod
    async def list_snapshots(self, ref: ActorRef) -> list[ActorSnapshotRef]: ...
```

Error model: `KeyError` (not found), `ValueError` (bad request),
`asyncio.TimeoutError` (resume timeout), `RuntimeError` (version
conflict / precondition). These map cleanly to HTTP 404/400/408/409.

### 4.3 State machine

```
                 create
        ┌─────────────────────────────┐
        ▼                              │
   SUSPENDED ──resume──▶ RESUMING ──ok──▶ RUNNING
        ▲                    │              │
        │                  fail/timeout     │ suspend
        │                    ▼              ▼
        └──────────────  SUSPENDING ◀──────┘
        (suspend)            │
                             │ ok
                             ▼
                         SUSPENDED
   Pause: RUNNING ──pause──▶ (node-local snapshot) ──▶ SUSPENDED
          (resume prefers the pause node via required_nodes)
```

### 4.4 Interoperability scenarios
1. **External control plane drives this runtime.** An external scheduler
   speaks the contract over HTTP/gRPC; `InProcessControlPlane` is
   replaced by a server adapter. The runtime is a pure data plane.
2. **This runtime drives external workers.** `WorkerPool` is given a
   `RemoteWorkerProvider` that calls an external node agent
   (`Run`/`Restore`/`Checkpoint` gRPC, mirroring the reference's
   node-level herder). Local backends are one provider among many.
3. **Side-by-side.** Two runtimes coexist; each owns its namespaces;
   the contract's `ActorRef.namespace` is the isolation boundary.

---

## 5. Detailed mechanism designs

### 5.1 Actor–worker isolation
- `Worker` = `(worker_id, SandboxRuntime adapter, SandboxClass, node,
  labels, status, assigned_actor)`. Wraps a `SandboxedWorkspaceExtBase`.
- `WorkerPool.acquire(constraints) -> Worker` delegates to a
  `Scheduler.pick(workers, constraints)`; on hit, marks the worker
  `BUSY` with `assigned_actor`. `release(worker)` clears assignment and
  returns the worker to the free list (or tears it down if broken).
- **Density gain** comes from time-multiplexing: actor A suspends → its
  worker returns to the pool → actor B resumes onto the same worker.
  Oversubscription ratio = (active actors) / (pool `max_size`).
- A worker hosts **≤1 actor at a time** (matches the reference invariant;
  keeps snapshot/restore semantics simple and crash-safe).

### 5.2 Singleflight
```python
class Singleflight:
    async def run(self, key: str, factory: Callable[[], Awaitable[T]]) -> T
```
- Per-key dedup: concurrent calls with the same `key` share one
  in-flight coroutine; result (or exception) is broadcast to all waiters.
- Used by: provision (key=`provision:<class>:<template>`), checkpoint
  (key=`checkpoint:<actor>`), golden-snapshot materialise
  (key=`golden:<template>`).
- **Composes with** `max_concurrent_provisions` (cap) and the keyed lock
  (cross-workflow serialisation). Singleflight collapses *identical*
  in-flight work; the lock *serialises* contended workflows; the cap
  *bounds* total resource pressure. Three distinct concerns.

### 5.3 Templates & golden snapshot
- `ActorTemplate` is **immutable**: `(name, version, sandbox_class,
  provision_spec, snapshot_config)`. Bumping any field ⇒ new version
  ⇒ new template record.
- On `create_template`, `TemplateRecord.phase` goes
  `Initial → Baking → Ready` (or `Failed`): the baker provisions one
  worker, runs `readyz` (the backend's `verify_runtime_available` +
  an optional ready probe), snapshots → **golden snapshot**
  (`ActorSnapshotRef(kind="golden")`), releases the worker.
- `create_actor` records the actor `SUSPENDED` with `last_snapshot`
  pointing at the template's golden snapshot (no boot yet).
- `resume_actor` (first time) restores from golden → sub-second vs cold
  boot. Subsequent resumes use the actor's `last_snapshot`.

### 5.4 Worker pool & scheduler
- `WorkerPool` extends `SandboxPool` (reuses pre-warm, idle eviction,
  capacity cap, fifo/lifo, health_check, max_concurrent_provisions).
- Adds: `SandboxClass` per worker, `node` label, `labels` dict,
  `assigned_actor` bookkeeping, and a pluggable `Scheduler`.
- `Scheduler.pick(candidates, constraints)`:
  1. filter: `sandbox_class` match, `status==ACTIVE`, `assigned_actor
     is None`, label selectors match, `required_nodes` empty or
     contains `worker.node`;
  2. choose: **random spreading** among candidates (simple, stateless,
     avoids hotspots — matches the reference); if `prefer_node` given
     and a candidate is on that node, prefer it (pause locality).

### 5.5 Snapshot & checkpoint (two-level)
- **Scope**: `Full` = rootfs delta + durable dir (+ mem on backends that
  support it); `Data` = durable dir only (cheap; resume cold-boots from
  image + restores durable). `on_suspend` ⊆ `on_pause` invariant.
- **Kind**: `Golden` (per-template, shared, immutable),
  `Last` (per-actor, overwritten each suspend), `Pause` (node-local,
  short-lived, drives locality).
- **Pause** = snapshot to the **node-local tier** (a `LocalSnapshotStore`
  under a per-node dir), keep worker→free, set `last_snapshot` =
  `Pause(node=...)`. Resume adds that node to `required_nodes`.
- **Suspend** = snapshot to the **durable tier** (`TieredSnapshotStore`),
  release worker, set `last_snapshot` = `Last`.
- `CheckpointManager` uses `Singleflight` keyed by actor so concurrent
  suspend/pause on the same actor collapse to one snapshot.
- Content-addressed refs for golden snapshots (`sha256` of the artifact)
  enable de-dup across actors of the same template.

---

## 6. Data model summary (entity relationships)

```
Namespace 1──* Actor *──1 ActorTemplate 1──1 GoldenSnapshot
                  │                 │
                  │ last_snapshot   │ golden_snapshot
                  ▼                 ▼
              ActorSnapshot ──◀ SnapshotStore tiers (Local/PG/MinIO)
                  │
                  │ assigned to (≤1 at a time)
                  ▼
              Worker *──1 WorkerPool ── SandboxClass
                  │
                  │ wraps
                  ▼
         SandboxedWorkspaceExtBase (backend)
```

Primary keys: `ActorRef=(namespace,name)`, `TemplateRef=(name,version)`,
`worker_id`, `snapshot_id` (content-addressed / unified ref).

---

## 7. Testing & benchmark plan

### 7.1 Unit tests (no mocks — real on-disk I/O)
All tests use `FakeSandboxRuntime` (real `shutil.copytree` snapshots,
real tempdir I/O — no mocks) so they exercise the actual snapshot /
restore / stage paths.

| Test file | Coverage |
|---|---|
| `test_actor_types.py` | Data-model round-trips, phase/status constants, version-conflict type. |
| `test_actor_registry.py` | Create/get/update/delete, optimistic-version conflict, keyed-lock serialisation, lock refcounting, namespace filtering. |
| `test_actor_scheduler.py` | Constraint filtering (class, labels, required_nodes), random spreading, prefer-node locality, no-capacity error. |
| `test_actor_lifecycle.py` | Full state machine, resume-from-golden, resume-from-last, pause-locality, concurrent-resume serialisation, density (many actors on small pool). |
| `test_checkpoint_singleflight.py` | Leader/follower dedup, exception broadcast, entry lifecycle, per-key isolation, slow-factory non-blocking. |
| `test_checkpoint_manager.py` | Tag sanitisation, scope invariants, pause→local, suspend→durable, bake-golden, resume-from-pause (local + cross-node fallback), concurrent dedup, list-snapshots. |
| `test_template.py` | Record round-trip, registry CRUD, bake phase transitions (Initial→Baking→Ready/Failed), idempotent bake, bake closes worker. |
| `test_worker_pool.py` | Acquire/release, broken-worker teardown, capacity timeout, prewarm, idle eviction, constraint matching, prefer-node, metrics, many-actors time-multiplex. |

**Result: 139 tests pass.**

### 7.2 Benchmarks (`tests/benchmarks/`)
- `bench_actor_runtime.py` — the new modular-runtime benchmark (see
  §7.3 below for results).
- `bench_pool.py` — the existing VFS + pool-optimisation benchmark
  (unchanged; compares direct / pool-fifo / pool-lifo / prewarm /
  prewarm+ratelimit / snapshot-restore strategies).

### 7.3 Benchmark results (`bench_actor_runtime.py`)

Three design claims, measured with real on-disk I/O
(`FakeSandboxRuntime` + `LocalSnapshotStore`):

#### Claim 1: Actor density — N actors on M workers (M ≪ N)

| Benchmark | Actors | Workers | Density | Conc | ops/s | p50 ms | p99 ms |
|---|---|---|---|---|---|---|---|
| density-10a-2w | 10 | 2 | 5:1 | 10 | 750 | 1.32 | 1.68 |
| density-50a-4w | 50 | 4 | 12.5:1 | 16 | 747 | 1.30 | 1.90 |
| density-100a-4w | 100 | 4 | 25:1 | 16 | 776 | 1.28 | 1.61 |

**Verdict:** Throughput is bounded by pool size, not actor count.
100 actors on 4 workers (25:1 oversubscription) sustain the same
throughput as 10 actors on 2 workers (5:1). The actor–worker isolation
model delivers density without latency penalty.

#### Claim 2: Two-level checkpoint — pause (local) vs suspend (durable) vs golden

| Benchmark | ops/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|
| pause-latency (node-local capture) | 10240 | 0.092 | 0.122 | 0.129 |
| suspend-latency (capture + durable upload) | 1368 | 0.729 | 0.778 | 0.873 |
| golden-restore (download + stage + restore) | 1744 | 0.572 | 0.623 | 0.678 |

**Verdict:** Pause is **~8× faster** than suspend (0.092 ms vs 0.729 ms),
justifying the two-level design: pause first (cheap, node-local), suspend
on eviction (durable, slower). Golden clone-restore (0.572 ms) is
sub-millisecond — new actors cold-start from golden in <1 ms instead of
booting from scratch.

#### Claim 3: Singleflight dedup — N concurrent callers → 1 execution

| Benchmark | Callers | Execs/iter | ops/s | p50 ms | p99 ms |
|---|---|---|---|---|---|
| singleflight-on-10c | 10 | **1.0** | 167 | 5.96 | 6.26 |
| singleflight-off-10c | 10 | **10.0** | 93 | 10.02 | 17.56 |

**Verdict:** Singleflight collapses 10 concurrent `checkpoint.suspend`
calls to **1 execution** (1.0 vs 10.0 executions per iter). Wall-clock
is **1.7× faster** (5.96 ms vs 10.02 ms p50) because the dedup path
does 1/10th the I/O work. The 10 baseline executions serialise on the
store's lock, inflating p99 to 17.6 ms.

### 7.4 Correctness invariants validated
- A worker never hosts >1 actor concurrently (enforced by `WORKER_BUSY`
  + `assigned_actor` in `WorkerPool.acquire`).
- `on_suspend` scope ⊆ `on_pause` scope (enforced in `CheckpointConfig`).
- Golden snapshots are immutable & shared (content-addressed via
  `template:<name>:<version>` actor_id in the durable store).
- Optimistic version rejects stale updates (`VersionConflict` in
  `ActorRegistry.update`).
- Pause-locality resume prefers the pause node when free (scheduler
  `prefer_node` hint; falls back to golden on cross-node miss).

---

## 8. Migration & compatibility
- Existing `*WorkspaceManager` / `SandboxPool` / `SnapshotStore` APIs
  are **unchanged**; the new layers are additive.
- `InProcessControlPlane` can wrap a legacy manager to expose the new
  contract, so callers migrate at their own pace.
- No new hard dependencies; distributed registry/lock/gRPC transport
  are optional extras (declared but not required by default).

---

## 9. Implementation status

| Component | Module | Status |
|---|---|---|
| Data model (`ActorRef`, `Constraints`, `ActorRecord`, `ActorSnapshotRef`, `SandboxClass`) | `_actor/_types.py` | ✅ done |
| Actor registry + optimistic versioning + keyed locks | `_actor/_registry.py` | ✅ done |
| Constraint scheduler (class/labels/required_nodes + locality) | `_actor/_scheduler.py` | ✅ done |
| Actor lifecycle state machine (resume/suspend/pause/delete) | `_actor/_lifecycle.py` | ✅ done |
| Worker pool (pre-warm, idle eviction, constraint scheduling) | `_worker/_pool.py` | ✅ done |
| Worker + SandboxRuntime protocol | `_worker/_types.py` | ✅ done |
| Singleflight (per-key call dedup) | `_checkpoint/_singleflight.py` | ✅ done |
| Checkpoint manager (pause/suspend/resume/bake_golden) | `_checkpoint/_manager.py` | ✅ done |
| Template registry + golden-snapshot baker | `_template/_template.py` | ✅ done |
| ControlPlane ABC + transport adapters | `_controlplane/` | ⏳ spec only (§4); `ActorLifecycle` is the in-process impl |
| Distributed registry/lock (Redis/Valkey) | — | ⏳ pluggable via `ActorRegistry`/`LockProvider` ABCs |
| HTTP/gRPC transport | — | ⏳ pluggable via the §4 contract |

### How to run

```bash
# Full test suite (139 tests)
python -m pytest tests/test_actor_*.py tests/test_checkpoint_*.py \
                 tests/test_template.py tests/test_worker_pool.py -v

# Actor-runtime benchmark (density + checkpoint + singleflight)
python -m tests.benchmarks.bench_actor_runtime --iters 100

# Existing pool benchmark (unchanged)
python -m tests.benchmarks.bench_pool --iters 100
```

Benchmark results are written to `docs/actor-benchmark-results.json`
and `docs/benchmark-results.json` respectively for reproducible
comparisons.
