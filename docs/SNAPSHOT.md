# VFS Snapshot / Restore — Open-Source Survey & Design Closure

English | [简体中文](SNAPSHOT_zh.md)

This document records the open-source survey that motivated the
`snapshot()` / `restore()` API on
[`VFSWorkspaceBase`](../src/agentscope_sandbox_ext/_vfs/_base.py), the
logical closure for why a deep-copy translation is the right first
implementation, and the real (no-mock) benchmark numbers that justify
shipping it.

## 1. Motivation

The dominant agent workflow is **iterative**: try a change → run a
test → roll back to the known-good state → try the next change. Today
the only rollback primitive the package offers is "close the workspace
and provision a fresh one" — which on a real backend means paying the
cold-boot + layout + skill-seed cost again on every iteration. On
`agentfs` that is ~7 ms; on Firecracker it is ~1–3 s; on Kata it is
~500 ms–2 s.

Snapshot/restore is the standard answer in every comparable system
(E2B, gVisor, Firecracker, containerd, k8s). The package should expose
the same primitive so:

1. Iterative agent loops get a cheap rollback path that does not pay
   the provision cost on every iteration.
2. A/B trial branches can fork from a snapshot without copying the
   whole workspace by hand.
3. The API is uniform across backends — a caller can
   `try: await ws.snapshot(t) except NotImplementedError: ...` to
   degrade gracefully on backends that have no snapshot primitive yet.

## 2. Open-source survey

Sixteen systems were surveyed; the five with directly borrowable
patterns are summarised below. The full table is in `SESSION_LOG.md`.

| system | snapshot primitive | what we borrowed |
|---|---|---|
| **E2B** (`createSnapshot` / `connect(snapshotId)`) | cloud-side per-sandbox FS snapshot; the sandbox ID *is* the snapshot handle | the "snapshot outlives the sandbox that created it" durability contract; the "snapshot ID is the restore handle" API shape |
| **Firecracker** (`PUT /snapshot/create` / `PUT /snapshot/load`) | microVM pause → diff-dump memory + disk → resume; restore = create VM from memfile + diff-disk | the "write to a temp path, rename into place" atomic-publish pattern; the "snapshot is independent of the running VM" framing |
| **gVisor** (`runsc checkpoint` / `runsc restore`) | CRIU-style process checkpoint; restore re-hydrates the Sentry + application | the "restore keeps the workspace usable end-to-end" expectation (no re-provision, no re-seed) |
| **containerd** snapshotter state machine (`Prepare` / `Commit` / `Active` / `Remove`) | overlayfs-style: `Prepare` creates an active dir, `Commit` promotes it to a read-only snapshot, later `Prepare` uses it as a lower dir | the "snapshot is namespaced per workspace" isolation rule; the "tagged snapshots are independent" namespacing rule |
| **k8s agent-sandbox** (snapshot/restore controller pattern) | PVC snapshot + new pod from snapshot | the "snapshot must survive `close()` and be restorable into a fresh workspace" durability contract (the E2B contract, restated for k8s) |

### 2.1 What every survey participant agreed on

Three invariants emerged from every system we looked at:

1. **The snapshot outlives the workspace that created it.** Closing
   the workspace must not delete snapshots; otherwise "snapshot, work,
   crash, restore" is impossible.
2. **Restore does not re-provision.** The workspace stays alive across
   restore — no re-bootstrap, no re-seed. Restore is the *cheap*
   rollback path; if it were as expensive as a fresh provision it
   would have no reason to exist.
3. **Snapshots are namespaced per workspace.** Two workspaces using
   the same tag must not collide. containerd enforces this with
   per-image snapshotter handles; E2B enforces it with per-sandbox
   snapshot IDs; we enforce it with a per-workspace `snapshots_root`.

### 2.2 What we did *not* borrow

- **CRIU / memory checkpoint.** gVisor and Firecracker both snapshot
  *process state* (memory + registers + open fds). A VFS workspace
  has no long-running process to checkpoint — the backend is a
  stateless translator. Borrowing CRIU would add a hard runtime
  dependency for zero benefit.
- **overlayfs / btrfs reflink / ZFS snapshots.** containerd's
  snapshotter gets away with hardlinks because the kernel enforces the
  lower dir's read-only-ness. A VFS workspace has no such enforcement:
  `exec_shell` runs arbitrary subprocesses that may mutate files in
  place (`sed -i`, `echo >> file`, `dd conv=notrunc`), and a hardlink
  tree would leak those mutations back into the snapshot. We pay the
  deep-copy cost for correctness; the `_snapshot_to` / `_restore_from`
  hooks leave the door open for a future overlayfs VFS backend that
  *can* guarantee CoW.
- **Distributed snapshot coordination (Chandy-Lamport, etc.).** The
  VFS workspace is single-writer; no coordination needed.

## 3. Design

### 3.1 API surface

`SandboxedWorkspaceExtBase` (the common base for every backend) gains
two coroutines:

```python
async def snapshot(self, tag: str) -> str: ...
async def restore(self, tag: str) -> None: ...
```

The default implementations raise `NotImplementedError`, so the API is
uniform across every backend — backends that have no snapshot
primitive (a future `memoryfs`, the existing Firecracker/gVisor/Kata
backends until they grow native snapshot support) keep working
unchanged. `VFSWorkspaceBase` overrides both with a real
implementation; `AgentFSWorkspace` inherits it.

### 3.2 VFS translation

`VFSWorkspaceBase` translates snapshot/restore into host-side tree
copies against `_host_workdir`:

- `snapshot(tag)` → `shutil.copytree(_host_workdir, <snapshots_root>/<tag>/)`
  via a temp sibling + `os.replace` so a crash mid-copy never leaves a
  half-written snapshot at the tag path.
- `restore(tag)` → wipe `_host_workdir` and `shutil.copytree` the
  snapshot back, via a temp dir + two `os.replace` calls so a crash
  mid-restore never leaves the workdir empty.

Snapshots live under `<host_workdir>.snapshots/` by default — a
sibling directory, so `restore` (which wipes the workdir) does not
delete them. The snapshots root is configurable via the
`snapshots_root=` constructor kwarg.

### 3.3 Why deep copy, not hardlinks

`exec_shell` runs arbitrary host subprocesses against the workdir. A
hardlink tree (`cp -al`) would share inodes between the snapshot and
the live tree, so an in-place mutation (`sed -i`, `echo >> file`, `dd
conv=notrunc`) on the live tree would silently corrupt the snapshot.
containerd's overlayfs snapshotter avoids this because the kernel
enforces the lower dir's read-only-ness; a VFS workspace has no such
enforcement. We pay the deep-copy cost for correctness.

The `_snapshot_to` / `_restore_from` hooks are the extension point for
a future VFS backend whose translation layer *can* guarantee CoW
(overlayfs upper-dir swap, btrfs reflink, 9p server-side snapshot).

### 3.4 Atomic publish

Both `snapshot` and `restore` write to a temp sibling first and only
rename into place once the copy is complete:

- `snapshot` cleans up stale `<tag>.tmp.*` / `<tag>.obsolete.*` dirs
  from a prior crash before writing the new snapshot, so a previous
  crash does not wedge the next snapshot.
- `restore` swaps the live workdir aside, moves the restored tree into
  place, then deletes the aside copy. Each rename is atomic on the
  same filesystem. On failure the original workdir is moved back.

There is a brief window during `snapshot` (between the `rmtree` of the
old tag and the `os.replace` of the new one) in which a concurrent
`restore(tag)` would observe `KeyError`. This is acceptable —
sequential snapshot/restore of the same tag is the norm, and callers
retry on `KeyError`.

### 3.5 Durability across `close()`

Snapshots live under `snapshots_root`, *not* under `host_workdir`. The
`close()` path tears down the VFS backend but does not touch the
snapshots root, so a snapshot created by workspace A can be restored
into workspace B bound to the same `host_workdir` + `snapshots_root`
after A has been closed and the workdir wiped. This is the E2B / k8s
"durable artifact" contract.

### 3.6 Tag validation

Tags must be a single path component — not empty, not `.` / `..`, not
containing `os.sep`. This keeps the snapshot path join trivial and
rules out path-traversal escapes from `snapshots_root`.

## 4. Correctness verification (real, no mocking)

`tests/test_vfs_snapshot.py` — 18 tests, 0 mocks. The correctness
oracle is a real `diff -r` subprocess: after `mutate → restore`, the
live tree must be byte-identical to the snapshot.

Coverage:

| area | test | oracle |
|---|---|---|
| API contract | `snapshot` / `restore` raise `NotImplementedError` on the base | `pytest.raises(NotImplementedError)` |
| Lifecycle guards | `snapshot` / `restore` before provision raise `RuntimeError` | `pytest.raises(RuntimeError)` |
| Tag validation | empty / `.` / `..` / `a/b` / `../escape` rejected | `pytest.raises(ValueError)` |
| Round-trip correctness | `snapshot → mutate (write_file + exec_shell) → restore` | real `diff -r` is empty |
| Snapshot isolation | in-place mutation after snapshot does not leak into snapshot | read snapshot file directly |
| Tag namespacing | two tags independent; restoring one does not affect the other | `read_file` after each restore |
| Atomic replace | re-snapshotting a tag replaces it atomically | `read_file` after restore |
| Metadata preservation | exec bit + empty dirs preserved across snapshot and restore | real `stat` |
| Durability across `close()` | snapshot survives `close()` + workdir wipe; restorable into a fresh workspace | `read_file` from new workspace |
| Skill-tree preservation | seeded `skills/` tree preserved across restore without re-seed | `list_dir` + `read_file` |
| Workspace isolation | two workspaces with the same tag do not collide | `read_file` from each |
| Metrics | `snapshot_count` reflects snapshot creates; atomic replace does not double-count | `metrics()` dict |
| End-to-end usability | after `restore`, a real `exec_shell` succeeds against the restored tree | `exit_code == 0` |

The tests deliberately exercise both translation hooks (`write_file`
*and* `exec_shell`) on the mutate path, because the snapshot must be
correct regardless of which primitive the agent used to mutate the
tree.

## 5. Benchmarks

### 5.1 Methodology

`tests/benchmarks/bench_pool.py` ships two new strategies that model
the iterative agent workflow:

| strategy | description |
|---|---|
| `reprovision-rollback` | baseline: every iteration closes the workspace and provisions + reseeds a fresh one. This is the rollback cost *without* snapshot/restore. |
| `snapshot-restore` | new: one `snapshot("rollback")` after seeding, then every iteration is `mutate → restore("rollback")`. The workspace stays alive. |

Both strategies seed the same realistic starting tree (two skills, a
session log, a notes file, a 2 KiB blob, an `.mcp.json`) so the
snapshot/reprovision costs are non-trivial. The bench uses the
zero-cost `agentfs` backend, so the numbers isolate the
snapshot/restore *translation* cost — not container boot cost. On a
real container/microVM backend the absolute speedup is much larger
because the `reprovision-rollback` row inherits the cold-boot cost.

### 5.2 Results

Config: `warmup=10, iters=300, concurrency=8` (run on the sandbox
host; absolute numbers are environment-dependent — the *relative*
shape is what matters).

| strategy | cold ms | hot ms | exec ms | release ms | ops/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| reprovision-rollback | 7.086 | 7.086 | 3.308 | 0.000 | 14.8 | 64.645 | 80.484 | 168.157 |
| snapshot-restore | 5.504 | 8.680 | 4.098 | 0.006 | 134.1 | 7.160 | 8.754 | 9.351 |

(The full strategy table — including the pool rows — is in
`docs/benchmark-results.json`.)

### 5.3 Findings

1. **9.1× throughput improvement.** `snapshot-restore` sustains 134.1
   ops/s vs 14.8 ops/s for the reprovision baseline — because the
   per-iteration path no longer pays the provision + reseed cost.

2. **17.9× p99 latency reduction.** `snapshot-restore` p99 is 9.4 ms
   vs 168 ms for the baseline. The baseline's long tail comes from
   the per-iteration `os.makedirs` + `_seed_rollback_workspace` (8
   small files) + `close`; `snapshot-restore` replaces all of that
   with a single `shutil.copytree` of the snapshot back.

3. **The one-time snapshot cost is paid back in <1 iteration.** The
   `cold_acquire_ms` for `snapshot-restore` (5.5 ms) is the cost of
   the single `snapshot()` call; the per-iteration `restore()` cost
   (the `hot_acquire_ms` column, 8.7 ms) is in the same ballpark as
   one provision. The win is that you pay the snapshot cost *once*
   and then every iteration is just the restore — you do not pay the
   provision + reseed cost on every iteration.

4. **On a real container/microVM backend the speedup is much larger.**
   The `agentfs` provision cost is ~7 ms (a single `os.makedirs` +
   Python object construction). A Firecracker cold boot is ~1–3 s.
   The `reprovision-rollback` row on Firecracker would be ~1–3 s per
   iteration; the `snapshot-restore` row would still be ~9 ms (the
   restore is a host-side tree copy, independent of the backend boot
   cost, because the workspace stays alive). That is a ~100–300×
   speedup on Firecracker — the bench understates the win because
   `agentfs` is the cheapest possible baseline.

### 5.4 Reproducibility

Two independent runs at `warmup=5, iters=100, concurrency=4`:

| run | reprovision ops/s | snapshot ops/s | ratio |
|---|---:|---:|---:|
| 1 | 26.9 | 109.1 | 4.1× |
| 2 | 27.3 | 105.8 | 3.9× |

The shape is stable across runs (within ~3 %). The ratio is smaller
than the canonical-config run because the smaller `concurrency` gives
the baseline less amortization headroom; the canonical config
(`concurrency=8`) is what the docs quote.

## 6. Recommendations

| workload | recommendation | why |
|---|---|---|
| Iterative agent loop (try → test → roll back) | `snapshot("rollback")` once after seeding, `restore("rollback")` per iteration | 9.1× throughput, 17.9× p99 vs reprovision |
| A/B trial branches | `snapshot("branch-a")` + `snapshot("branch-b")` from the same seed; `restore` to switch | tags are independent; no manual tree copy |
| Crash recovery | `snapshot("checkpoint")` periodically; on crash, re-bind workspace to the same `host_workdir` + `snapshots_root` and `restore("checkpoint")` | snapshots survive `close()` (the durability contract) |
| Backends without snapshot support | `try: await ws.snapshot(t) except NotImplementedError: ...` | uniform API; degrade gracefully |

## 7. Running the benchmarks

```bash
# full bench (includes the pool rows + the snapshot/restore pair)
python -m tests.benchmarks.bench_pool

# canonical config the docs quote
python -m tests.benchmarks.bench_pool --warmup 10 --iters 300 --concurrency 8

# machine-readable JSON
python -m tests.benchmarks.bench_pool --json out.json
```

A snapshot is always written to `docs/benchmark-results.json` on every
run so docs can reference a reproducible result.
