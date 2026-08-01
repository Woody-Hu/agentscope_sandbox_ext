# Session Log — Snapshot/Restore Feature Investigation

English | [简体中文](SESSION_LOG_zh.md)

This log records the investigation, decision, and verification trail
for the `snapshot()` / `restore()` API shipped on
[`VFSWorkspaceBase`](../src/agentscope_sandbox_ext/_vfs/_base.py).
It is the audit trail required by the open-ended task brief: a real
survey of open-source agent-sandbox / pluggable-workspace systems, a
strict logical closure for what was borrowed and what was not, and
real (no-mock) test + benchmark evidence.

The feature design and benchmark analysis live in
[`SNAPSHOT.md`](SNAPSHOT.md); this log is the *process* artifact —
what was looked at, what was decided, what was verified, in what
order.

## 1. Task

> 对于这个系统做一个开放性的任务，这种基于智能体的 sandbox 与可插拔
> workspace 的工作应该也有很多开源的系统，进行调研，看看有没有可参考的
> 思路，尝试做一些测试与验证如果真的有意义的话则引入当前系统，但是你
> 需要完成严格的逻辑闭环论证，同时注意需要更新 session-log、文档等内容
> （注意与当前项目风格匹配）测试时不能 mock 作弊。

Constraints extracted from the brief:

1. Survey open-source agent-sandbox / pluggable-workspace systems.
2. Identify referenceable patterns.
3. Test & validate; introduce into the current system *only if*
   meaningful — strict logical closure required.
4. Update session-log + docs in the project style.
5. **No mock cheating in tests.**

## 2. Investigation

### 2.1 Systems surveyed

Sixteen systems were surveyed, grouped by the pattern they contribute
to the snapshot/restore design space.

| # | system | pattern of interest | relevance |
|---|---|---|---|
| 1 | **E2B** | cloud sandbox `createSnapshot` / `connect(snapshotId)`; sandbox ID is the snapshot handle | direct — durability contract, API shape |
| 2 | **Firecracker** | microVM `PUT /snapshot/create` (pause → diff-dump mem + disk → resume); `PUT /snapshot/load` (create VM from memfile + diff-disk) | direct — atomic publish, snapshot independent of running VM |
| 3 | **gVisor** (`runsc checkpoint` / `runsc restore`) | CRIU-style process checkpoint; restore re-hydrates Sentry + app | direct — "restore keeps workspace usable end-to-end" |
| 4 | **containerd** snapshotter (`Prepare` / `Commit` / `Active` / `Remove`) | overlayfs-style lower/upper dir state machine | direct — per-workspace namespacing, tag independence |
| 5 | **k8s agent-sandbox** (snapshot/restore controller pattern) | PVC snapshot + new pod from snapshot | direct — durability across `close()` restated for k8s |
| 6 | **CRIU** | process checkpoint/restore (memory + fds + registers) | rejected — VFS workspace has no long-running process |
| 7 | **overlayfs** (Linux) | kernel-enforced CoW lower + writable upper | rejected for VFS — no enforcement; deferred via `_snapshot_to` hook |
| 8 | **btrfs reflink** | CoW file copy | rejected for VFS — filesystem-specific; deferred via `_snapshot_to` hook |
| 9 | **ZFS snapshots** | filesystem-level snapshot | rejected for VFS — filesystem-specific; deferred via `_snapshot_to` hook |
| 10 | **OpenSandbox** (agentscope native) | cloud sandbox, snapshot semantics not documented at the time of survey | noted — no borrowable pattern |
| 11 | **Daytona** (agentscope native) | cloud dev environment | noted — orthogonal (no snapshot primitive exposed) |
| 12 | **Apple Container** (agentscope native) | macOS container runtime | noted — no snapshot primitive exposed |
| 13 | **Bubblewrap** (agentscope native) | Linux namespace jailer | noted — no snapshot primitive exposed |
| 14 | **DockerWorkspace** (agentscope native) | bind-mount + image build | noted — no snapshot primitive exposed; the natural place to add a `tar`-based snapshot later |
| 15 | **Chandy-Lamport** distributed snapshot | consistent distributed cut | rejected — VFS workspace is single-writer; no coordination needed |
| 16 | **Git** (content-addressed tree snapshot) | tree hash → immutable snapshot | noted — too heavy for the rollback use case (we want a tree copy, not a content-addressed store) |

### 2.2 Invariants extracted from the survey

Three invariants emerged from every system that has a real snapshot
primitive (rows 1–5):

1. **The snapshot outlives the workspace that created it.** Closing
   the workspace must not delete snapshots; otherwise
   "snapshot → work → crash → restore" is impossible.
2. **Restore does not re-provision.** The workspace stays alive across
   restore — no re-bootstrap, no re-seed. Restore is the *cheap*
   rollback path.
3. **Snapshots are namespaced per workspace.** Two workspaces using
   the same tag must not collide.

These became the acceptance criteria for the design.

### 2.3 What the current package was missing

Before this work, the only rollback primitive the package offered was
"close the workspace and provision a fresh one" — which on a real
backend means paying the cold-boot + layout + skill-seed cost again on
every iteration. The survey showed every comparable system exposes a
cheaper rollback primitive; the package should too.

## 3. Decision

### 3.1 What was shipped

1. `SandboxedWorkspaceExtBase.snapshot(tag) -> str` and
   `SandboxedWorkspaceExtBase.restore(tag) -> None` — abstract API on
   the common base, default `NotImplementedError` so the API is
   uniform across every backend.
2. `VFSWorkspaceBase.snapshot` / `VFSWorkspaceBase.restore` — real
   implementation translating snapshot/restore into host-side tree
   copies against `_host_workdir`.
3. `_snapshot_to` / `_restore_from` subclass hooks — the extension
   point for future VFS backends that can guarantee CoW (overlayfs,
   btrfs reflink, 9p server-side snapshot).
4. `_cleanup_stale_snapshot_dirs` — crash recovery for half-written
   temp dirs from a previous crash.
5. `snapshot_count` added to `VFSWorkspaceBase.metrics()`.

### 3.2 Why snapshot/restore and not something else from the survey

The survey surfaced three candidate features:

| candidate | source | verdict |
|---|---|---|
| snapshot/restore | E2B, Firecracker, gVisor, containerd, k8s | **shipped** — universal across the survey; directly addresses the iterative-agent-rollback use case; quantifiable win |
| memory checkpoint (CRIU) | gVisor, Firecracker | rejected — VFS workspace has no long-running process; would add a hard runtime dependency for zero benefit |
| CoW tree (overlayfs / reflink) | containerd, btrfs, ZFS | deferred — requires filesystem-specific support; the `_snapshot_to` hook leaves the door open without paying the cost now |

Snapshot/restore was the only candidate that (a) every surveyed
system agreed on, (b) was implementable without a new hard runtime
dependency, and (c) had a quantifiable win on the cheapest baseline
(`agentfs`) — which understates the win on real backends.

### 3.3 Why deep copy, not hardlinks

`exec_shell` runs arbitrary host subprocesses against the workdir. A
hardlink tree (`cp -al`) would share inodes between the snapshot and
the live tree, so an in-place mutation (`sed -i`, `echo >> file`,
`dd conv=notrunc`) on the live tree would silently corrupt the
snapshot. containerd's overlayfs snapshotter avoids this because the
kernel enforces the lower dir's read-only-ness; a VFS workspace has
no such enforcement. We pay the deep-copy cost for correctness.

This is the logical closure: the survey gave us the *idea* (CoW
snapshot) and the *correctness constraint* (snapshot isolation); the
*implementation* (deep copy) follows from the constraint plus the VFS
workspace's lack of kernel-enforced CoW.

### 3.4 Atomic publish + crash recovery

Both `snapshot` and `restore` write to a temp sibling first and only
rename into place once the copy is complete (borrowed from
Firecracker's `PUT /snapshot/create` "write to temp path, rename into
place" pattern). `_cleanup_stale_snapshot_dirs` removes stale
`<tag>.tmp.*` / `<tag>.obsolete.*` dirs from a previous crash before
writing the new snapshot, so a prior crash does not wedge the next
snapshot.

### 3.5 Durability across `close()`

Snapshots live under `snapshots_root`, *not* under `host_workdir`. The
`close()` path tears down the VFS backend but does not touch the
snapshots root, so a snapshot created by workspace A can be restored
into workspace B bound to the same `host_workdir` + `snapshots_root`
after A has been closed and the workdir wiped. This is the E2B / k8s
"durable artifact" contract.

## 4. Verification

### 4.1 Correctness — real, no mocking

`tests/test_vfs_snapshot.py` — 18 tests, 0 mocks. The correctness
oracle is a real `diff -r` subprocess: after `mutate → restore`, the
live tree must be byte-identical to the snapshot.

Coverage summary (full table in [`SNAPSHOT.md`](SNAPSHOT.md) §4):

- API contract: `NotImplementedError` on the base; real impl on
  `VFSWorkspaceBase`.
- Lifecycle guards: `snapshot` / `restore` before provision raise
  `RuntimeError`.
- Tag validation: empty / `.` / `..` / `a/b` / `../escape` rejected.
- Round-trip correctness: real `diff -r` is empty after
  `snapshot → mutate (write_file + exec_shell) → restore`.
- Snapshot isolation: in-place mutation after snapshot does not leak
  into the snapshot.
- Tag namespacing: two tags independent; restoring one does not affect
  the other.
- Atomic replace: re-snapshotting a tag replaces it atomically.
- Metadata preservation: exec bit + empty dirs preserved (real
  `stat`).
- Durability across `close()`: snapshot survives `close()` + workdir
  wipe; restorable into a fresh workspace.
- Skill-tree preservation: seeded `skills/` tree preserved across
  restore without re-seed.
- Workspace isolation: two workspaces with the same tag do not
  collide.
- Metrics: `snapshot_count` reflects snapshot creates; atomic replace
  does not double-count.
- End-to-end usability: after `restore`, a real `exec_shell` succeeds
  against the restored tree.

The tests deliberately exercise both translation hooks (`write_file`
*and* `exec_shell`) on the mutate path, because the snapshot must be
correct regardless of which primitive the agent used to mutate the
tree.

### 4.2 Performance — real, no mocking

`tests/benchmarks/bench_pool.py` ships two new strategies that model
the iterative agent workflow:

- `reprovision-rollback` — baseline: every iteration closes the
  workspace and provisions + reseeds a fresh one. The rollback cost
  *without* snapshot/restore.
- `snapshot-restore` — new: one `snapshot("rollback")` after seeding,
  then every iteration is `mutate → restore("rollback")`. The
  workspace stays alive.

Canonical config (`warmup=10, iters=300, concurrency=8`):

| strategy | ops/s | p50 ms | p99 ms |
|---|---:|---:|---:|
| reprovision-rollback | 14.8 | 64.645 | 168.157 |
| snapshot-restore | 134.1 | 7.160 | 9.351 |

**9.1× throughput improvement, 17.9× p99 latency reduction.** The
full table is in [`SNAPSHOT.md`](SNAPSHOT.md) §5 and
`docs/benchmark-results.json`.

Reproducibility: two independent runs at
`warmup=5, iters=100, concurrency=4` produced 4.1× and 3.9×
throughput ratios — stable within ~3%.

### 4.3 No-regression — full pytest suite

```
$ python -m pytest -q
.............................sss........................................ [ 43%]
........................................................................ [ 86%]
.......................                                                  [100%]
164 passed, 3 skipped in 23.49s
```

The 3 skips are runtime-probe tests for gVisor / Kata / Sysbox that
auto-skip when the binaries are absent — by design, not mock cheating.

## 5. Logical closure

The brief required a strict logical closure for "if meaningful, then
introduce". The closure is:

1. **Survey → invariant.** Every surveyed system with a real snapshot
   primitive (5 of 16) agrees on three invariants (§2.2). The
   invariant set is not a design choice — it is a necessary condition
   for any snapshot/restore implementation to be correct.

2. **Invariant → API.** The three invariants plus "uniform API across
   backends" (already a package convention via
   `verify_runtime_available`) force the API shape: `snapshot(tag)` /
   `restore(tag)` on `SandboxedWorkspaceExtBase`, default
   `NotImplementedError`, real impl on `VFSWorkspaceBase`.

3. **VFS translation constraint → deep copy.** The VFS workspace has
   no kernel-enforced CoW (unlike containerd overlayfs), so the
   snapshot must be a deep copy to satisfy invariant #1 (snapshot
   isolation) under arbitrary `exec_shell` mutations.

4. **Deep copy → atomic publish + crash recovery.** A deep copy can
   be interrupted; the survey's atomic-publish pattern (Firecracker)
   + a stale-temp-dir sweeper make the copy crash-safe.

5. **Real test → correctness.** 18 no-mock tests with `diff -r` as
   the oracle prove the implementation satisfies the invariants.

6. **Real bench → meaningful.** 9.1× throughput / 17.9× p99 on the
   cheapest baseline (which understates the win on real backends).
   The feature is meaningfully better than the status quo.

∴ The feature is meaningful (6) and correct (5), the implementation
follows necessarily from the invariants (3, 4), the invariants follow
from the survey (1, 2). **Ship it.**

## 6. Changes shipped

| file | change |
|---|---|
| `src/agentscope_sandbox_ext/_base.py` | added `snapshot` / `restore` abstract API on `SandboxedWorkspaceExtBase` |
| `src/agentscope_sandbox_ext/_vfs/_base.py` | implemented `snapshot` / `restore` / `_snapshot_to` / `_restore_from` / `_cleanup_stale_snapshot_dirs` on `VFSWorkspaceBase`; added `snapshots_root` ctor kwarg; added `snapshot_count` to `metrics()` |
| `tests/test_vfs_snapshot.py` | new — 18 no-mock tests |
| `tests/benchmarks/bench_pool.py` | added `bench_reprovision_rollback` + `bench_snapshot_restore` strategies; added `_seed_rollback_workspace` / `_mutate` helpers; added `itertools` import |
| `docs/SNAPSHOT.md` | new — survey + design + bench analysis (English) |
| `docs/SNAPSHOT_zh.md` | new — survey + design + bench analysis (Chinese) |
| `docs/SESSION_LOG.md` | new — this log (English) |
| `docs/SESSION_LOG_zh.md` | new — this log (Chinese) |
| `docs/benchmark-results.json` | updated — canonical-config run with the two new rows |
| `README.md` | updated — link to `SNAPSHOT.md` |
| `README_zh.md` | updated — link to `SNAPSHOT_zh.md` |

## 7. Follow-ups (not in scope)

- Native `snapshot` / `restore` on `FirecrackerWorkspace` — the
  Firecracker REST API already exposes `PUT /snapshot/create` /
  `PUT /snapshot/load`; the in-VM guest agent would need a
  `pause` / `resume` op. The API shape on
  `SandboxedWorkspaceExtBase` is ready for this.
- `tar`-based snapshot on `DockerWorkspace` — `docker commit` +
  `docker save` is the natural translation; same API shape.
- overlayfs-backed `_snapshot_to` for a future `overlayfs` VFS backend
  — the hook is in place; the CoW guarantee would let us drop the
  deep-copy cost.
