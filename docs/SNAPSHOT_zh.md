# VFS Snapshot / Restore —— 开源调研与设计闭环

[English](SNAPSHOT.md) | 简体中文

本文记录了催生
[`VFSWorkspaceBase`](../src/agentscope_sandbox_ext/_vfs/_base.py) 上
`snapshot()` / `restore()` API 的开源调研、为何"深拷贝转译"是正确首发
实现的逻辑闭环，以及证明其值得发布的真实（无 mock）基准数据。

## 1. 动机

agent 的主导工作流是**迭代式**：试一个改动 → 跑测试 → 回滚到已知良好
状态 → 试下一个改动。今天本包能提供的回滚原语只有"关闭工作区并
provision 一个新的"——在真实后端上意味着每次迭代都要再付冷启动 + 布局
+ 技能种子的成本。在 `agentfs` 上约 7 ms；Firecracker 约 1–3 s；Kata
约 500 ms–2 s。

snapshot/restore 是每一个可比系统（E2B、gVisor、Firecracker、containerd、
k8s）给出的标准答案。本包应暴露同样的原语，以便：

1. 迭代式 agent 循环获得一条廉价的回滚路径，不再每次迭代都付 provision
   成本。
2. A/B 试验分支能从快照分叉，而无需手动拷贝整个工作区。
3. API 跨后端统一——调用方可以
   `try: await ws.snapshot(t) except NotImplementedError: ...` 在尚未
   支持快照的后端上优雅降级。

## 2. 开源调研

共调研 16 个系统；下表汇总了 5 个有可直接借鉴模式的系统。完整表格见
`SESSION_LOG.md`。

| 系统 | 快照原语 | 我们借鉴了什么 |
|------|----------|----------------|
| **E2B**（`createSnapshot` / `connect(snapshotId)`）| 云端每沙箱 FS 快照；沙箱 ID *就是* 快照句柄 | "快照比创建它的工作区活得更久"的持久化契约；"快照 ID 即 restore 句柄"的 API 形态 |
| **Firecracker**（`PUT /snapshot/create` / `PUT /snapshot/load`）| microVM 暂停 → 差分 dump 内存 + 磁盘 → 恢复；restore = 从 memfile + diff-disk 创建 VM | "先写临时路径，再 rename 就位"的原子发布模式；"快照独立于运行中的 VM"的框架 |
| **gVisor**（`runsc checkpoint` / `runsc restore`）| CRIU 风格的进程检查点；restore 重新水合 Sentry + 应用 | "restore 后工作区端到端可用"的预期（不 re-provision、不 re-seed）|
| **containerd** snapshotter 状态机（`Prepare` / `Commit` / `Active` / `Remove`）| overlayfs 风格：`Prepare` 创建 active 目录，`Commit` 提升为只读快照，后续 `Prepare` 把它作为 lower dir | "快照按工作区命名空间隔离"规则；"带 tag 的快照相互独立"规则 |
| **k8s agent-sandbox**（snapshot/restore 控制器模式）| PVC 快照 + 从快照建新 pod | "快照必须熬过 `close()`，并能 restore 进新工作区"的持久化契约（即 E2B 契约在 k8s 上的重述）|

### 2.1 所有被调研系统的共识

从每个系统都浮现出三条不变量：

1. **快照比创建它的工作区活得更久。** 关闭工作区绝不能删除快照；否则
   "snapshot、干活、崩溃、restore" 链路不可能成立。
2. **restore 不 re-provision。** 工作区在 restore 期间保持存活——不
   re-bootstrap、不 re-seed。restore 是*廉价*的回滚路径；若它与一次新
   provision 一样贵，它就没有存在理由。
3. **快照按工作区命名空间隔离。** 两个工作区用同一个 tag 不能冲突。
   containerd 用 per-image snapshotter 句柄强制；E2B 用 per-sandbox 快照
   ID 强制；我们用 per-workspace 的 `snapshots_root` 强制。

### 2.2 我们*没有*借鉴什么

- **CRIU / 内存检查点。** gVisor 和 Firecracker 都快照*进程状态*
  （内存 + 寄存器 + 打开的 fd）。VFS 工作区没有长寿命进程可检查点——
  后端是无状态转译器。借鉴 CRIU 会为零收益增加硬运行时依赖。
- **overlayfs / btrfs reflink / ZFS 快照。** containerd 的 snapshotter
  能用硬链接逃过这一劫，是因为内核强制 lower dir 只读。VFS 工作区没有
  这种强制：`exec_shell` 跑的是任意子进程，可能原地改文件
  （`sed -i`、`echo >> file`、`dd conv=notrunc`），硬链接树会让这些改动
  悄悄漏回快照。我们为正确性付深拷贝成本；`_snapshot_to` /
  `_restore_from` 钩子给未来*能*保证 CoW 的 overlayfs VFS 后端留了门。
- **分布式快照协调（Chandy-Lamport 等）。** VFS 工作区是单写者；无需
  协调。

## 3. 设计

### 3.1 API 表面

`SandboxedWorkspaceExtBase`（每个后端的共同基类）新增两个协程：

```python
async def snapshot(self, tag: str) -> str: ...
async def restore(self, tag: str) -> None: ...
```

默认实现抛 `NotImplementedError`，所以 API 跨每个后端统一——尚无快照
原语的后端（未来的 `memoryfs`、现有 Firecracker/gVisor/Kata/Sysbox 后端直到它们
长出原生快照支持）保持原样工作。`VFSWorkspaceBase` 用真实实现覆盖二者；
`AgentFSWorkspace` 继承之。

### 3.2 VFS 转译

`VFSWorkspaceBase` 把 snapshot/restore 转译为对 `_host_workdir` 的宿主侧
树拷贝：

- `snapshot(tag)` → `shutil.copytree(_host_workdir, <snapshots_root>/<tag>/)`，
  经临时兄弟目录 + `os.replace`，使崩溃中途不会在 tag 路径留下半写快照。
- `restore(tag)` → 清空 `_host_workdir` 并 `shutil.copytree` 快照回来，
  经临时目录 + 两次 `os.replace`，使崩溃中途不会让 workdir 变空。

快照默认放在 `<host_workdir>.snapshots/`——一个兄弟目录，因此 `restore`
（清空 workdir）不会删掉它们。快照根可通过 `snapshots_root=` 构造参数
配置。

### 3.3 为什么是深拷贝而非硬链接

`exec_shell` 跑的是针对 workdir 的任意宿主子进程。硬链接树（`cp -al`）
会在快照与活树之间共享 inode，因此活树上原地改动（`sed -i`、
`echo >> file`、`dd conv=notrunc`）会悄悄腐蚀快照。containerd 的 overlayfs
snapshotter 逃过此劫是因为内核强制 lower dir 只读；VFS 工作区没有这种
强制。我们为正确性付深拷贝成本。

`_snapshot_to` / `_restore_from` 钩子是未来*能*保证 CoW 的 VFS 后端
（overlayfs upper-dir 切换、btrfs reflink、9p 服务端快照）的扩展点。

### 3.4 原子发布

`snapshot` 与 `restore` 都先写临时兄弟目录，拷贝完成后再 rename 就位：

- `snapshot` 在写新快照前清理上次崩溃留下的 `<tag>.tmp.*` /
  `<tag>.obsolete.*` 目录，使上次崩溃不会卡住下次快照。
- `restore` 把活 workdir 挪到一边，把恢复的树移到位，再删除挪开的那份。
  每次 rename 在同一文件系统上都是原子的。失败时原 workdir 会被挪回。

`snapshot` 期间（在删旧 tag 与 `os.replace` 新 tag 之间）有一个短暂窗口，
并发的 `restore(tag)` 会观察到 `KeyError`。这是可接受的——同一 tag 的
顺序 snapshot/restore 是常态，调用方在 `KeyError` 上重试。

### 3.5 跨 `close()` 的持久性

快照放在 `snapshots_root`，*而非* `host_workdir` 下。`close()` 路径会
拆解 VFS 后端但不碰快照根，因此工作区 A 创建的快照在 A 被关闭、workdir
被清空后，可被绑定到同一 `host_workdir` + `snapshots_root` 的工作区 B
restore。这就是 E2B / k8s 的"持久化产物"契约。

### 3.6 tag 校验

tag 必须是单一路径分量——非空、非 `.` / `..`、不含 `os.sep`。这让快照
路径拼接变得平凡，也排除了从 `snapshots_root` 逃逸的路径遍历。

## 4. 正确性验证（真实，无 mock）

`tests/test_vfs_snapshot.py`——18 个测试，0 个 mock。正确性预言机是真实
的 `diff -r` 子进程：`mutate → restore` 后，活树必须与快照字节一致。

覆盖：

| 领域 | 测试 | 预言机 |
|------|------|--------|
| API 契约 | 基类上 `snapshot` / `restore` 抛 `NotImplementedError` | `pytest.raises(NotImplementedError)` |
| 生命周期守卫 | provision 之前 `snapshot` / `restore` 抛 `RuntimeError` | `pytest.raises(RuntimeError)` |
| tag 校验 | 空 / `.` / `..` / `a/b` / `../escape` 被拒 | `pytest.raises(ValueError)` |
| 往返正确性 | `snapshot → mutate（write_file + exec_shell）→ restore` | 真实 `diff -r` 为空 |
| 快照隔离 | snapshot 之后原地改动不漏入快照 | 直接读快照文件 |
| tag 命名空间 | 两个 tag 独立；restore 一个不影响另一个 | 每次 restore 后 `read_file` |
| 原子替换 | 重新 snapshot 同一 tag 原子替换 | restore 后 `read_file` |
| 元数据保留 | exec 位 + 空目录在 snapshot 与 restore 中保留 | 真实 `stat` |
| 跨 `close()` 持久性 | 快照熬过 `close()` + workdir 清空；可 restore 进新工作区 | 从新工作区 `read_file` |
| 技能树保留 | 种子的 `skills/` 树在 restore 后保留，无需 re-seed | `list_dir` + `read_file` |
| 工作区隔离 | 两个工作区用同一 tag 不冲突 | 各自 `read_file` |
| 指标 | `snapshot_count` 反映快照创建；原子替换不重复计数 | `metrics()` 字典 |
| 端到端可用性 | `restore` 之后真实 `exec_shell` 在恢复的树上成功 | `exit_code == 0` |

测试刻意在 mutate 路径上同时跑两个转译钩子（`write_file` *和*
`exec_shell`），因为无论 agent 用哪个原语改树，快照都必须正确。

## 5. 基准测试

### 5.1 方法论

`tests/benchmarks/bench_pool.py` 提供两个新策略，建模迭代式 agent 工作流：

| 策略 | 描述 |
|------|------|
| `reprovision-rollback` | 基线：每次迭代关闭工作区并 provision + 重种子一个新的。这是*没有* snapshot/restore 时的回滚成本。|
| `snapshot-restore` | 新增：种子后一次 `snapshot("rollback")`，之后每次迭代 `mutate → restore("rollback")`。工作区保持存活。|

两个策略种子同一棵真实起点树（两个技能、一个 session 日志、一个 notes
文件、一个 2 KiB blob、一个 `.mcp.json`），使 snapshot/reprovision 成本
非平凡。基准用零成本 `agentfs` 后端，所以数据隔离出的是 snapshot/restore
的*转译*成本——而非容器启动成本。在真实容器/microVM 后端上绝对加速比
大得多，因为 `reprovision-rollback` 行会继承冷启动成本。

### 5.2 结果

配置：`warmup=10, iters=300, concurrency=8`（在沙箱宿主上运行；
绝对值依赖环境——*相对*形态才是重点）。

| 策略 | cold ms | hot ms | exec ms | release ms | ops/s | p50 ms | p95 ms | p99 ms |
|------|--------:|-------:|--------:|-----------:|------:|-------:|-------:|-------:|
| reprovision-rollback | 7.086 | 7.086 | 3.308 | 0.000 | 14.8 | 64.645 | 80.484 | 168.157 |
| snapshot-restore | 5.504 | 8.680 | 4.098 | 0.006 | 134.1 | 7.160 | 8.754 | 9.351 |

（含池化行的完整策略表见 `docs/benchmark-results.json`。）

### 5.3 发现

1. **9.1 倍吞吐提升。** `snapshot-restore` 持续 134.1 ops/s，对比基线
   14.8 ops/s——因为每迭代路径不再付 provision + 重种子成本。

2. **17.9 倍 p99 延迟降低。** `snapshot-restore` 的 p99 是 9.4 ms，对比
   基线 168 ms。基线长尾来自每迭代的 `os.makedirs` +
   `_seed_rollback_workspace`（8 个小文件）+ `close`；`snapshot-restore`
   把这些全部换成单次 `shutil.copytree` 把快照拷回。

3. **一次性快照成本 < 1 次迭代即回本。** `snapshot-restore` 的
   `cold_acquire_ms`（5.5 ms）是单次 `snapshot()` 调用成本；每迭代
   `restore()` 成本（`hot_acquire_ms` 列，8.7 ms）与一次 provision 同
   量级。胜在于快照成本*只付一次*，之后每次迭代只是 restore——不再每
   次迭代付 provision + 重种子成本。

4. **在真实容器/microVM 后端上加速比大得多。** `agentfs` 的 provision
   成本约 7 ms（一次 `os.makedirs` + Python 对象构造）。Firecracker 冷
   启动 1–3 s。Firecracker 上 `reprovision-rollback` 行会是每迭代 1–3 s；
   `snapshot-restore` 行仍是约 9 ms（restore 是宿主侧树拷贝，独立于后端
   启动成本，因为工作区保持存活）。这在 Firecracker 上是约 100–300 倍
   加速——基准低估了收益，因为 `agentfs` 是最廉价的基线。

### 5.4 可复现性

`warmup=5, iters=100, concurrency=4` 下两次独立运行：

| 运行 | reprovision ops/s | snapshot ops/s | 比值 |
|------|-------------------:|---------------:|-----:|
| 1 | 26.9 | 109.1 | 4.1× |
| 2 | 27.3 | 105.8 | 3.9× |

形态跨运行稳定（在 ~3% 内）。比值小于 canonical 配置运行，因为更小的
`concurrency` 给基线更少的摊销空间；文档引用的是 canonical 配置
（`concurrency=8`）。

## 6. 推荐

| 工作负载 | 推荐 | 原因 |
|----------|------|------|
| 迭代式 agent 循环（试 → 测 → 回滚）| 种子后一次 `snapshot("rollback")`，每迭代 `restore("rollback")` | 对比 reprovision 9.1 倍吞吐、17.9 倍 p99 |
| A/B 试验分支 | 从同一种子 `snapshot("branch-a")` + `snapshot("branch-b")`；用 `restore` 切换 | tag 独立；无需手动树拷贝 |
| 崩溃恢复 | 周期性 `snapshot("checkpoint")`；崩溃后把工作区重绑到同一 `host_workdir` + `snapshots_root` 并 `restore("checkpoint")` | 快照熬过 `close()`（持久化契约）|
| 无快照支持的后端 | `try: await ws.snapshot(t) except NotImplementedError: ...` | API 统一；优雅降级 |

## 7. 运行基准测试

```bash
# 完整基准（含池化行 + snapshot/restore 对）
python -m tests.benchmarks.bench_pool

# 文档引用的 canonical 配置
python -m tests.benchmarks.bench_pool --warmup 10 --iters 300 --concurrency 8

# 机器可读 JSON
python -m tests.benchmarks.bench_pool --json out.json
```

每次运行都会把快照写入 `docs/benchmark-results.json`，文档可引用可复现
结果。
