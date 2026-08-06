# Agent Orchestration Runtime — Design & Integration Specification

English | [简体中文](ORCHESTRATION_zh.md)

This document specifies a **modular orchestration layer** that sits on top
of the existing per-workspace sandbox backends (Firecracker / gVisor /
Kata / Sysbox / VFS) and lets the package interoperate with a
large-scale agent control plane. It is the result of surveying a
production agent runtime ("the reference runtime" below) that maps many
idle "actors" onto a smaller set of warm "workers", and distilling the
patterns worth borrowing into composable, opt-in modules.

The layer is **additive**: nothing in the existing
`SandboxedWorkspaceExtBase` / `SandboxExtManagerBase` / `SandboxPool`
surface changes. Callers that do not need orchestration keep using the
per-workspace API exactly as before.

---

## 1. Why an orchestration layer?

The current package is built around the **per-workspace** model: every
`(user_id, agent_id, session_id)` triple maps to one sandboxed workspace
that is provisioned on demand and cached by the manager. That model is
simple and correct, but it has three structural costs at scale:

| Cost | Symptom today |
|---|---|
| **Idle resource burn** | A long-lived agent keeps a microVM/container alive for the whole session even though agent-like workloads are idle most of the time. |
| **Cold boot on every miss** | A cache miss pays the full provision cost (Firecracker ~1–3 s, Kata ~500 ms–2 s). There is no "resume a previously-suspended agent" path. |
| **No request coalescing** | Two concurrent `get_workspace` calls for the same logical agent race to provision two sandboxes; the loser is torn down. |

The reference runtime solves exactly these three problems with an
**actor/worker split** plus a small set of supporting primitives. This
document specifies how to bring those primitives into this package
without a hard dependency on Kubernetes or Redis — they are
**in-process, async, pluggable**, and degrade gracefully to the existing
per-workspace behaviour when disabled.

---

## 2. Survey of the reference runtime

The reference runtime is a Kubernetes-native agent control plane. Its
hot path bypasses etcd entirely (actors / workers / snapshots live in
Redis because "the Kubernetes API server is not designed to handle
millions of resources"); K8s is used only for low-frequency infra
(standby worker pods, autoscaling, RBAC, network policy).

Five patterns from that runtime are directly borrowable. Each is graded
below on (a) what it buys us, (b) how it differs from what we have, and
(c) what we keep vs. change when porting it into an in-process Python
package.

### 2.1 Actor–worker isolation

**Reference.** Many `Actor` records (cheap rows in Redis) are
multiplexed onto a smaller set of `Worker` pods. A worker hosts **at
most one actor at a time**; multiplexing is time-sliced via
suspend/resume, not concurrent co-tenancy. An idle actor is just a
record plus a snapshot — zero compute. Worker assignment is an
optimistic-concurrency claim (Redis `WATCH/MULTI`, `version` field);
contended claims retry.

**What it buys us.** Decouples "logical agent identity" from "live
sandbox". An agent that is idle stops burning a sandbox; the same
sandbox is reused for the next actor that needs it.

**Differs from today.** Today `workspace_id` *is* the sandbox; there is
no notion of a suspended agent that owns no compute.

**Port.** We keep the per-workspace sandbox as the "worker" unit (a
`SandboxedWorkspaceExtBase` instance *is* a worker). We add an `Actor`
layer above it: an `Actor` is a cheap dataclass that, when resumed,
binds to one worker for the duration of its activation. The CAS
assignment primitive is ported as an in-memory `version`-guarded
`WorkerStore.claim()`; the contract is identical so a Redis-backed
store can drop in later.

### 2.2 Singleflight

**Reference.** Two distinct uses: (1) the ingress router deduplicates
concurrent `ResumeActor` calls for the same actor onto one in-flight
control-plane call (`golang.org/x/sync/singleflight`, keyed by actor
ref). The leader's context is **detached from any single caller** — if
caller 1 disconnects, callers 2/3 still get the result. Late joiners
inherit the *remaining* budget. (2) Concurrent image-layer pulls are
collapsed so simultaneous actor starts never duplicate work. A related
"request parking" mechanism holds requests when the pool is saturated
and retries with backoff instead of returning 503.

**What it buys us.** Eliminates the "two concurrent `get_workspace`
calls provision two sandboxes" race, and prevents thundering-herd
provision bursts.

**Differs from today.** Today `SandboxPool` has `max_concurrent_provisions`
(a semaphore that *bounds* concurrency) but no *deduplication*. Two
callers asking for the same logical agent still get two separate
sandboxes.

**Port.** We ship a self-contained `Singleflight` class with
per-flight budget detached from caller context, and wire it into the
orchestrator's `resume_actor` so concurrent resumes of the same actor
collapse onto one provision. We also use it to dedup template
provisioning (the equivalent of the image-pull singleflight).

### 2.3 Templates (immutable spec + golden snapshot)

**Reference.** An immutable `ActorTemplate` (image pinned by digest,
sandbox class, snapshot scope). On creation a one-time "golden" boot is
checkpointed; the first actor of that template **restores from the
shared golden snapshot** instead of cold-booting. A node-local
content-addressed image cache composes rootfs via a single overlayfs
mount (milliseconds) instead of untarring.

**What it buys us.** Fast first-boot of a new agent: boot becomes a
restore, not a cold start.

**Differs from today.** Today every provision cold-boots and re-seeds
skills; there is no shared "known-good starting point" across actors of
the same kind.

**Port.** We add an `ActorTemplate` (immutable spec: backend kind,
provision config, skill paths, MCPs, snapshot scope). The first
materialisation of a template takes a `snapshot("golden")`; subsequent
actors of the same template `restore("golden")` instead of cold-booting
(only on backends whose `snapshot()` is implemented — otherwise they
fall back to cold boot). We reuse the existing `SandboxedWorkspaceExtBase.
snapshot/restore` API and the `TieredSnapshotStore` for durable,
cross-worker golden snapshots.

### 2.4 Worker pool (standby workers + scheduler)

**Reference.** A `WorkerPool` CRD materialises a `Deployment` of standby
pods ("workers waiting for assignments"). HPA on an external metric
scales the deployment. A scheduler lists idle workers matching
`Constraints` (sandbox class, selectors, node locality for paused
snapshots) and picks one **uniformly at random** — no bin-packing.
Workers have `ACTIVE`/`DRAINING` states; on termination the herder
forwards SIGTERM to the actor so it can save state.

**What it buys us.** Warm capacity that is shared across all actors,
not per-manager. An idle actor does not consume a warm slot — only an
*active* actor does.

**Differs from today.** Today `SandboxPool` keeps warm sandboxes but a
checked-out sandbox stays bound to one workspace for its whole session
(only returned on `close`). There is no "suspend this actor and free
the worker for another actor" path.

**Port.** We compose the existing `SandboxPool` as the standby
mechanism (it already has pre-warm, idle eviction, capacity cap,
`max_concurrent_provisions`). On top we add a `WorkerPool` orchestrator
that owns the assignment contract: `acquire_worker(constraints)` →
returns a worker with an optimistic `version`; `release_worker` either
returns it to standby or drains it. The `SandboxPool` stays the
building block; `WorkerPool` adds the actor-multiplexing contract.

### 2.5 Snapshot & checkpoint (suspend/resume)

**Reference.** Sub-second suspend/resume via Cloud-Hypervisor snapshot
+ `userfaultfd` OnDemand restore (~75 ms vs ~1.8 s eager). Two scopes:
`Full` (memory + rootfs delta + durable dir) and `Data` (durable dir
only; guest discarded → cold-boot on resume). Diff-snapshot merge:
an OnDemand-restored actor's next checkpoint is sparse (only faulted
pages), merged onto the restore source to rebuild a self-contained
snapshot.

**What it buys us.** An idle agent can be **suspended** (snapshot
taken, worker freed) and **resumed** later in sub-second, instead of
being torn down and cold-rebooted.

**Differs from today.** Today `snapshot/restore` is **filesystem-only**
(deep-copy of the workdir) and the workspace **stays alive** across
restore — there is no "free the worker, keep the snapshot" suspend
path, and no memory checkpoint.

**Port.** We add a `CheckpointScope` enum (`FULL` / `DATA`) and an
`ActorCheckpoint` bridge that ties an `Actor` to the existing
`SnapshotStore`/`TieredSnapshotStore`. `suspend_actor(scope=FULL)`
snapshots and frees the worker; `resume_actor` restores. Memory
checkpoint is **not** portable into a Python in-process package (it
needs a VMM with snapshot support); we expose the scope enum and the
contract, ship a `DATA`-scope implementation today (FS snapshot via the
existing VFS path + durable `SnapshotStore`), and leave `FULL` scope as
an opt-in hook that a future Firecracker/Cloud-Hypervisor backend
overrides with a real memory snapshot. Diff-merge is left as a backend
hook (`_merge_delta_into_base`).

### 2.6 What we deliberately do NOT port

- **K8s/Redis hard dependency.** The orchestration layer is in-process
  and async. The `ActorStore`/`WorkerStore` ABCs have an in-memory
  default; a Redis backend is a drop-in for multi-node deployments.
- **Envoy + ext_proc traffic routing.** Shipped as an abstract `Router`
  interface with an in-memory default; a real ingress is the deployer's
  concern.
- **CRIU / memory checkpoint.** Not portable; `FULL` scope is a hook.
- **gRPC control plane.** The orchestration API is async-Python; a gRPC
  façade can be generated from the data models later.

---

## 3. Architecture

```
                ┌──────────────────────────────────────────────────┐
                │              Orchestrator (facade)                │
                │  create_actor / resume_actor / suspend_actor /    │
                │  pause_actor / delete_actor  (+ Singleflight)     │
                └───────┬──────────┬──────────┬──────────┬─────────┘
                        │          │          │          │
              ┌─────────▼──┐ ┌─────▼────┐ ┌───▼────┐ ┌───▼──────┐
              │ ActorStore │ │WorkerPool│ │Router  │ │Checkpoint│
              │ (CAS)      │ │+Scheduler│ │(abstract)│ │ Bridge  │
              └─────────┬──┘ └─────┬────┘ └───┬────┘ └───┬──────┘
                        │          │          │          │
                        │          │          │          ▼
                        │          │          │   SnapshotStore /
                        │          │          │   TieredSnapshotStore
                        │          │          │   (existing — Local/Minio/PG)
                        │          │          │
              ┌─────────▼──────────▼──────────▼─────────┐
              │           WorkerPool (standby)           │
              │   backed by SandboxPool (existing)       │
              │   factory = a SandboxedWorkspaceExtBase  │
              └─────────────────────┬────────────────────┘
                                    │
                                    ▼
              SandboxedWorkspaceExtBase  (Firecracker / gVisor /
                  Kata / Sysbox / VFS)   ← unchanged
```

### 3.1 Module layout

All new code lives under `agentscope_sandbox_ext/_orchestration/`:

| Module | Responsibility |
|---|---|
| `singleflight.py` | `Singleflight` — dedup concurrent calls with same key; per-flight budget detached from caller context. |
| `model.py` | Core data models: `Actor`, `ActorStatus`, `ActorRef`, `Worker`, `WorkerState`, `WorkerAssignment`, `ActorTemplate`, `SandboxClass`, `CheckpointScope`, `Constraints`. |
| `store.py` | `ActorStore` / `WorkerStore` ABCs + `InMemoryActorStore` / `InMemoryWorkerStore` with optimistic-CAS (`version`) claim/release. |
| `worker_pool.py` | `WorkerPool` — standby worker management + `Scheduler` (random-among-eligible with `Constraints`). Composes existing `SandboxPool`. |
| `checkpoint.py` | `CheckpointBridge` — ties `Actor` to `SnapshotStore`; `FULL`/`DATA` scope; durable across worker free. |
| `lifecycle.py` | `Workflow` engine + idempotent `Step`s for resume (Load → ClaimWorker → AttachSnapshot → Restore → Finalize). |
| `router.py` | `Router` ABC + `InMemoryRouter` — resolve `ActorRef` → active worker address. |
| `orchestrator.py` | `Orchestrator` façade — ties everything together; `Singleflight` around `resume_actor`. |

### 3.2 Composability / opt-in

Every module is usable in isolation:

- Need **only** request dedup on an existing manager? Use `Singleflight`
  directly around `get_workspace`.
- Need **only** the actor/worker model? Use `WorkerPool` + `ActorStore`
  with a `SandboxPool` factory.
- Need the **full** suspend/resume lifecycle? Use `Orchestrator`.

Nothing is mandatory. The existing per-workspace API is unchanged and
remains the recommended path for single-user / dev / CI.

---

## 4. Data model

### 4.1 Actor

```python
@dataclass
class Actor:
    actor_id: str                 # unique within namespace
    namespace: str                # tenant / atespace equivalent (identity = namespace + actor_id)
    template_id: str              # ActorTemplate this actor was created from
    status: ActorStatus           # see state machine below
    version: int                  # optimistic-concurrency token (CAS)
    worker_assignment: WorkerAssignment | None   # set while RUNNING/RESUMING
    latest_snapshot_ref: str | None              # last suspend/pause snapshot
    worker_selector: dict[str, str]              # label selector for worker affinity
    created_at: float
    updated_at: float
```

**State machine** (mirrors the reference runtime; transitions are
CAS-guarded):

```
                 create
        ┌─────────────────────────────┐
        ▼                              │
   SUSPENDED ──resume──▶ RESUMING ──▶ RUNNING
        ▲                   │           │
        │                   │           │ suspend
        │                   │           ▼
        │                   │       SUSPENDING
        │                   │           │
        │                   ▼           ▼
        │                 (CRASHED) ◀── SUSPENDED
        │                               │
        │                               │ pause
        │                               ▼
        │                            PAUSING
        │                               │
        │                               ▼
        │                            PAUSED
        │                               │
        └───────── delete ──────────────┘
                    │
                    ▼
                DELETING
```

- `resume` from `PAUSED` prefers a worker on the same node (snapshot is
  node-local — locality constraint).
- `CRASHED` is reachable from `RESUMING`/`RUNNING`/`SUSPENDING` on
  worker loss; resume from `CRASHED` re-binds a fresh worker.
- `delete` is only valid from `SUSPENDED`/`CRASHED`/`PAUSED`.

### 4.2 Worker

```python
@dataclass
class Worker:
    worker_id: str
    state: WorkerState            # ACTIVE | DRAINING
    sandbox_class: SandboxClass   # CONTAINER | MICROVM | VFS
    assignment: WorkerAssignment | None   # the actor currently bound
    version: int                  # CAS token
    labels: dict[str, str]        # for selector matching
    node: str | None              # locality hint
    sandbox: SandboxedWorkspaceExtBase | None  # the live workspace (in-process)
```

**Invariant:** `assignment is not None` ⟺ the worker is currently
hosting an actor. `claim(actor)` is a CAS transition
`assignment: None → WorkerAssignment(actor_id, ...)`; `release()` is
the inverse. A `DRAINING` worker rejects new claims.

### 4.3 ActorTemplate

```python
@dataclass(frozen=True)
class ActorTemplate:
    template_id: str
    sandbox_class: SandboxClass
    backend_kind: str             # "firecracker" | "gvisor" | "kata" | "sysbox" | "agentfs"
    provision_config: dict        # backend-specific kwargs (image, vcpu, ...)
    skill_paths: list[str]
    default_mcps: list            # MCP clients
    snapshot_scope_on_suspend: CheckpointScope   # FULL | DATA
    snapshot_scope_on_pause: CheckpointScope     # DATA (node-local)
    version: int = 1              # immutable: bump = new template
```

**Immutability:** like the reference runtime, a template is immutable;
changing the provision config produces a new `template_id`. This is
required because **changing the image invalidates snapshots** — a
snapshot taken from template v1 is not restorable into a v2 worker.

**Golden snapshot is tracked off the template.** The one-time "golden"
snapshot of a fresh materialisation is tracked out-of-band by the
:class:`Orchestrator` (its `golden_snapshots` registry) rather than as
a field on the frozen template: the first actor of a template
cold-boots and (if the backend supports snapshots) captures a golden
snapshot; subsequent actors of the same template `restore("golden")`
instead of cold-booting — boot becomes a restore, not a cold start.
Keeping the golden ref off the immutable spec means the spec never has
to mutate once the first actor has materialised.

### 4.4 Constraints & scheduling

```python
@dataclass
class Constraints:
    sandbox_class: SandboxClass            # hard gate (snapshots not portable across classes)
    template_selector: dict[str, str]      # label selector AND'd
    actor_selector: dict[str, str]         # label selector AND'd
    required_node: str | None              # locality for paused snapshots
```

The scheduler lists workers with `assignment is None`, `state == ACTIVE`,
matching `Constraints`, then picks one **uniformly at random** (no
bin-packing — matches the reference runtime's deliberate simplicity).

### 4.5 CheckpointScope

```python
class CheckpointScope(Enum):
    FULL = "full"   # memory + FS delta + durable (backend hook; not portable)
    DATA = "data"   # durable FS only; guest discarded → cold-boot on resume
```

---

## 5. Interface specification

### 5.1 Singleflight

```python
class Singleflight:
    async def run(
        self,
        key: str,
        fn: Callable[[], Awaitable[T]],
        *,
        budget: float | None = None,
    ) -> T:
        """Dedup concurrent calls with the same *key* onto one in-flight *fn*.

        The leader's *budget* is detached from any single caller's
        context — if caller 1 cancels, callers 2/3 still get the result.
        Late joiners inherit the *remaining* budget.  Returns the
        leader's result to every caller (or raises the leader's
        exception to every caller).
        """
```

### 5.2 ActorStore / WorkerStore (CAS)

```python
class ActorStore(ABC):
    # Identity is the composite (namespace, actor_id) — the same actor_id
    # in two namespaces is two distinct actors (mirrors ActorRef).
    @abstractmethod
    async def get(self, actor_id: str, *, namespace: str = "default") -> Actor: ...
    @abstractmethod
    async def put(self, actor: Actor, *, expected_version: int | None = None) -> Actor: ...
    @abstractmethod
    async def delete(self, actor_id: str, *, namespace: str = "default") -> None: ...
    @abstractmethod
    async def list(self, namespace: str | None = None) -> list[Actor]: ...

class WorkerStore(ABC):
    @abstractmethod
    async def get(self, worker_id: str) -> Worker: ...
    @abstractmethod
    async def claim(
        self, worker_id: str, assignment: WorkerAssignment, *, expected_version: int,
    ) -> Worker:
        """CAS: assignment None → assignment.  Raises VersionConflict on contention."""
    @abstractmethod
    async def release(self, worker_id: str, *, expected_version: int) -> Worker: ...
    @abstractmethod
    async def list_idle(self, constraints: Constraints) -> list[Worker]: ...
    @abstractmethod
    async def set_state(self, worker_id: str, state: WorkerState, *, expected_version: int) -> Worker: ...
```

### 5.3 WorkerPool + Scheduler

```python
class Scheduler:
    async def pick(self, workers: list[Worker], constraints: Constraints) -> Worker | None:
        """Filter by constraints, pick one uniformly at random."""

class WorkerPool:
    def __init__(self, store: WorkerStore, sandbox_pool: SandboxPool, scheduler: Scheduler): ...
    async def acquire_worker(self, constraints: Constraints) -> Worker:
        """Acquire a standby worker (from SandboxPool) and register it."""
    async def release_worker(self, worker: Worker, *, drain: bool = False) -> None:
        """Release the actor binding; return worker to standby (or drain)."""
```

### 5.4 CheckpointBridge

```python
class CheckpointBridge:
    def __init__(self, store: SnapshotStore | TieredSnapshotStore): ...
    async def checkpoint(
        self, actor: Actor, worker: Worker, scope: CheckpointScope,
    ) -> str:
        """Snapshot the worker's FS into the durable store; return snapshot_ref.
        FULL scope delegates to worker.sandbox.snapshot() if the backend
        supports it, else degrades to DATA with a warning."""
    async def restore(
        self, actor: Actor, worker: Worker, snapshot_ref: str,
    ) -> None:
        """Restore the snapshot into the worker's sandbox."""
```

### 5.5 Router

```python
class Router(ABC):
    @abstractmethod
    async def resolve(self, actor: ActorRef) -> WorkerAssignment | None: ...
    @abstractmethod
    async def bind(self, actor: ActorRef, assignment: WorkerAssignment) -> None: ...
    @abstractmethod
    async def unbind(self, actor: ActorRef) -> None: ...

class InMemoryRouter(Router): ...
```

### 5.6 Orchestrator (façade)

```python
class Orchestrator:
    def __init__(
        self,
        *,
        actor_store: ActorStore,
        worker_pool: WorkerPool,
        checkpoint: CheckpointBridge,
        router: Router,
        templates: dict[str, ActorTemplate],
        singleflight: Singleflight | None = None,
    ): ...

    async def create_actor(
        self, actor_id: str, template_id: str, *, namespace: str = "default",
    ) -> Actor: ...

    async def resume_actor(
        self, actor_id: str, *, namespace: str = "default", budget: float = 30.0,
    ) -> WorkerAssignment:
        """Idempotent resume wrapped in Singleflight keyed by (namespace, actor_id).
        Workflow: LoadActor → ClaimWorker → (Restore|GoldenBoot) → Finalize."""

    async def suspend_actor(
        self, actor_id: str, *, namespace: str = "default",
        scope: CheckpointScope | None = None,
    ) -> str:
        """Snapshot + free the worker.  Returns the snapshot_ref."""

    async def pause_actor(self, actor_id: str, *, namespace: str = "default") -> str:
        """Node-local snapshot (DATA scope); worker freed but snapshot stays node-local."""

    async def delete_actor(self, actor_id: str, *, namespace: str = "default") -> None: ...
```

---

## 6. Integration with the existing system

The orchestration layer composes on top of the existing backends; it
does **not** replace them. The integration point is the `SandboxPool`
factory:

```python
from agentscope_sandbox_ext import FirecrackerWorkspaceManager, SandboxPool
from agentscope_sandbox_ext._orchestration import (
    Orchestrator, InMemoryActorStore, InMemoryWorkerStore,
    WorkerPool, Scheduler, CheckpointBridge, InMemoryRouter,
    Singleflight, ActorTemplate, SandboxClass, CheckpointScope,
)
from agentscope_sandbox_ext._runtime import LocalSnapshotStore

mgr = FirecrackerWorkspaceManager(basedir="/tmp/as", enable_pool=True)

template = ActorTemplate(
    template_id="bash-fc-v1",
    sandbox_class=SandboxClass.MICROVM,
    backend_kind="firecracker",
    provision_config={...},
    skill_paths=["./skills"],
    snapshot_scope_on_suspend=CheckpointScope.DATA,
    snapshot_scope_on_pause=CheckpointScope.DATA,
)

orch = Orchestrator(
    actor_store=InMemoryActorStore(),
    worker_pool=WorkerPool(InMemoryWorkerStore(), mgr._pool, Scheduler()),
    checkpoint=CheckpointBridge(LocalSnapshotStore("/tmp/snaps")),
    router=InMemoryRouter(),
    templates={template.template_id: template},
    singleflight=Singleflight(),
)
```

**Backwards compatibility:** callers that do not touch `_orchestration`
see zero change. The existing `get_workspace` / `close` / `close_all`
manager API and the `SandboxPool` API are untouched.

---

## 7. Testing strategy

Mirrors the package's "real tests, no mocking" rule:

- **Singleflight**: N concurrent `run()` calls with the same key → the
  wrapped function executes exactly once; all N callers receive the
  same result. Leader-cancels-early → joiners still complete. Budget
  exhaustion → late joiners raise `TimeoutError`.
- **CAS stores**: concurrent `claim()` on the same worker → exactly one
  succeeds, the rest raise `VersionConflict`. `release()` is the
  inverse.
- **Scheduler**: filters by `Constraints` correctly; random pick is
  uniform over eligible workers (statistical test over many runs).
- **WorkerPool**: `acquire_worker`/`release_worker` round-trip; a
  `DRAINING` worker is never handed out.
- **CheckpointBridge**: `DATA`-scope snapshot → restore round-trip is
  verified by a real `diff -r` (reuses the VFS snapshot test oracle).
  `FULL` scope on a backend without memory snapshot degrades to `DATA`.
- **Orchestrator**: `resume_actor` wrapped in `Singleflight` collapses
  concurrent resumes onto one provision; `suspend` + `resume` preserves
  FS state across worker free (the durability contract); `pause` then
  `resume` prefers the same node (locality).
- **Lifecycle workflow**: a crashed step is re-runnable and converges
  (idempotency).

Integration tests that need a real microVM/container are marked
`@pytest.mark.integration` and skipped when the binary is absent.

---

## 8. Benchmarks

`tests/benchmarks/bench_orchestration.py` ships four strategies that
isolate the orchestration overhead using the zero-cost `agentfs`
backend.  All strategies run **serially** (concurrency=1) so the
per-activation percentiles are directly comparable — the pool-level
throughput-under-load numbers live in `bench_pool.py`.

| strategy | description |
|---|---|
| `direct` | baseline: raw sandbox provision + exec + close per activation, no orchestration. |
| `cold-resume` | `resume_actor` with no golden + no warm-idle worker (`max_warm_idle=0`); every activation materialises a fresh worker and cold-boots (capturing the golden). The "scale-up from zero" path. |
| `warm-idle-resume` | `resume_actor` reusing a warm-idle worker (returned by the previous suspend) + restoring the template's golden snapshot. The delta vs. cold-resume is the sandbox-pool acquire cost saved. |
| `snapshot-restore` | `resume_actor` restoring the actor's *own* durable snapshot (suspend → resume round-trip), reusing a warm-idle worker. The delta vs. warm-idle-resume is the checkpoint + restore cost of actor-private state. |

Metrics: `resume_ms` (one-shot), `suspend_ms`, serial `ops/s`,
`p50/p95/p99_resume_ms`.  Results are written to
`docs/orchestration-benchmark-results.json` on every run so docs
reference a reproducible result.  On the VFS backend the provision cost
is near-zero, so cold-resume ≈ warm-idle-resume; on a real
container/microVM backend the delta widens to the boot cost the
warm-idle pool saves.
