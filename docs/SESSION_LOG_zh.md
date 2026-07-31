# 会话日志 —— Snapshot/Restore 特性调研

[English](SESSION_LOG.md) | 简体中文

本日志记录
[`VFSWorkspaceBase`](../src/agentscope_sandbox_ext/_vfs/_base.py) 上
`snapshot()` / `restore()` API 的调研、决策与验证轨迹。它是开放性任务
brief 要求的审计轨迹：真实调研开源 agent-sandbox / 可插拔 workspace 系统、
严格逻辑闭环论证借鉴了什么与不借鉴什么、真实（无 mock）的测试 + 基准
证据。

特性设计与基准分析位于 [`SNAPSHOT_zh.md`](SNAPSHOT_zh.md)；本日志是
*过程*产物——看了什么、决定了什么、按什么顺序验证了什么。

## 1. 任务

> 对于这个系统做一个开放性的任务，这种基于智能体的 sandbox 与可插拔
> workspace 的工作应该也有很多开源的系统，进行调研，看看有没有可参考的
> 思路，尝试做一些测试与验证如果真的有意义的话则引入当前系统，但是你
> 需要完成严格的逻辑闭环论证，同时注意需要更新 session-log、文档等内容
> （注意与当前项目风格匹配）测试时不能 mock 作弊。

从 brief 中提取的约束：

1. 调研开源 agent-sandbox / 可插拔 workspace 系统。
2. 找出可参考的思路。
3. 测试与验证；*仅当*有意义时引入当前系统——需要严格逻辑闭环。
4. 按当前项目风格更新 session-log + 文档。
5. **测试时不能 mock 作弊。**

## 2. 调研

### 2.1 被调研系统

共调研 16 个系统，按其对 snapshot/restore 设计空间贡献的模式分组。

| # | 系统 | 关注的模式 | 相关性 |
|---|------|-----------|--------|
| 1 | **E2B** | 云沙箱 `createSnapshot` / `connect(snapshotId)`；沙箱 ID 即快照句柄 | 直接——持久化契约、API 形态 |
| 2 | **Firecracker** | microVM `PUT /snapshot/create`（暂停 → 差分 dump 内存 + 磁盘 → 恢复）；`PUT /snapshot/load`（从 memfile + diff-disk 创建 VM）| 直接——原子发布、快照独立于运行中的 VM |
| 3 | **gVisor**（`runsc checkpoint` / `runsc restore`）| CRIU 风格进程检查点；restore 重新水合 Sentry + 应用 | 直接——"restore 后工作区端到端可用"|
| 4 | **containerd** snapshotter（`Prepare` / `Commit` / `Active` / `Remove`）| overlayfs 风格 lower/upper dir 状态机 | 直接——per-workspace 命名空间、tag 独立性 |
| 5 | **k8s agent-sandbox**（snapshot/restore 控制器模式）| PVC 快照 + 从快照建新 pod | 直接——跨 `close()` 持久化契约在 k8s 上的重述 |
| 6 | **CRIU** | 进程检查点/恢复（内存 + fd + 寄存器）| 拒绝——VFS 工作区无长寿命进程 |
| 7 | **overlayfs**（Linux）| 内核强制 CoW lower + 可写 upper | VFS 拒绝——无强制；经 `_snapshot_to` 钩子推迟 |
| 8 | **btrfs reflink** | CoW 文件拷贝 | VFS 拒绝——文件系统特定；经 `_snapshot_to` 钩子推迟 |
| 9 | **ZFS snapshots** | 文件系统级快照 | VFS 拒绝——文件系统特定；经 `_snapshot_to` 钩子推迟 |
| 10 | **OpenSandbox**（agentscope 原生）| 云沙箱，调研时快照语义未文档化 | 备注——无可借鉴模式 |
| 11 | **Daytona**（agentscope 原生）| 云开发环境 | 备注——正交（未暴露快照原语）|
| 12 | **Apple Container**（agentscope 原生）| macOS 容器运行时 | 备注——未暴露快照原语 |
| 13 | **Bubblewrap**（agentscope 原生）| Linux 命名空间 jailer | 备注——未暴露快照原语 |
| 14 | **DockerWorkspace**（agentscope 原生）| bind-mount + 镜像构建 | 备注——未暴露快照原语；未来加 `tar` 快照的天然位置 |
| 15 | **Chandy-Lamport** 分布式快照 | 一致分布式切分 | 拒绝——VFS 工作区单写者；无需协调 |
| 16 | **Git**（内容寻址树快照）| 树哈希 → 不可变快照 | 备注——对回滚用例过重（我们要树拷贝，不要内容寻址存储）|

### 2.2 从调研中提取的不变量

每一个有真实快照原语的系统（第 1–5 行）都浮现出三条不变量：

1. **快照比创建它的工作区活得更久。** 关闭工作区绝不能删除快照；否则
   "snapshot → 干活 → 崩溃 → restore" 链路不可能成立。
2. **restore 不 re-provision。** 工作区在 restore 期间保持存活——不
   re-bootstrap、不 re-seed。restore 是*廉价*的回滚路径。
3. **快照按工作区命名空间隔离。** 两个工作区用同一个 tag 不能冲突。

这些成为设计的验收标准。

### 2.3 当前包缺什么

本次工作前，本包能提供的回滚原语只有"关闭工作区并 provision 一个新的"
——在真实后端上意味着每次迭代都要再付冷启动 + 布局 + 技能种子成本。调研
显示每个可比系统都暴露更廉价的回滚原语；本包也应如此。

## 3. 决策

### 3.1 发布了什么

1. `SandboxedWorkspaceExtBase.snapshot(tag) -> str` 与
   `SandboxedWorkspaceExtBase.restore(tag) -> None`——共同基类上的抽象
   API，默认 `NotImplementedError`，使 API 跨每个后端统一。
2. `VFSWorkspaceBase.snapshot` / `VFSWorkspaceBase.restore`——真实实现，
   把 snapshot/restore 转译为对 `_host_workdir` 的宿主侧树拷贝。
3. `_snapshot_to` / `_restore_from` 子类钩子——未来能保证 CoW 的 VFS 后端
   （overlayfs、btrfs reflink、9p 服务端快照）的扩展点。
4. `_cleanup_stale_snapshot_dirs`——上次崩溃留下的半写临时目录的崩溃
   恢复。
5. `snapshot_count` 加入 `VFSWorkspaceBase.metrics()`。

### 3.2 为什么是 snapshot/restore 而非调研里的别的东西

调研浮现三个候选特性：

| 候选 | 来源 | 裁决 |
|------|------|------|
| snapshot/restore | E2B、Firecracker、gVisor、containerd、k8s | **发布**——调研中普遍；直接命中迭代式 agent 回滚用例；收益可量化 |
| 内存检查点（CRIU）| gVisor、Firecracker | 拒绝——VFS 工作区无长寿命进程；会为零收益增加硬运行时依赖 |
| CoW 树（overlayfs / reflink）| containerd、btrfs、ZFS | 推迟——需文件系统特定支持；`_snapshot_to` 钩子留门而不付成本 |

snapshot/restore 是唯一同时满足 (a) 所有被调研系统都认同、(b) 无需新硬
运行时依赖即可实现、(c) 在最廉价基线（`agentfs`）上有可量化收益——而这
个收益在真实后端上被低估——的候选。

### 3.3 为什么是深拷贝而非硬链接

`exec_shell` 跑的是针对 workdir 的任意宿主子进程。硬链接树（`cp -al`）
会在快照与活树之间共享 inode，因此活树上原地改动（`sed -i`、
`echo >> file`、`dd conv=notrunc`）会悄悄腐蚀快照。containerd 的 overlayfs
snapshotter 逃过此劫是因为内核强制 lower dir 只读；VFS 工作区没有这种
强制。我们为正确性付深拷贝成本。

这就是逻辑闭环：调研给了我们*想法*（CoW 快照）与*正确性约束*（快照
隔离）；*实现*（深拷贝）由约束 + VFS 工作区缺乏内核强制 CoW 必然推出。

### 3.4 原子发布 + 崩溃恢复

`snapshot` 与 `restore` 都先写临时兄弟目录，拷贝完成后再 rename 就位
（借鉴自 Firecracker 的 `PUT /snapshot/create` "写临时路径，rename 就位"
模式）。`_cleanup_stale_snapshot_dirs` 在写新快照前清理上次崩溃留下的
`<tag>.tmp.*` / `<tag>.obsolete.*` 目录，使上次崩溃不会卡住下次快照。

### 3.5 跨 `close()` 的持久性

快照放在 `snapshots_root`，*而非* `host_workdir` 下。`close()` 路径会
拆解 VFS 后端但不碰快照根，因此工作区 A 创建的快照在 A 被关闭、workdir
被清空后，可被绑定到同一 `host_workdir` + `snapshots_root` 的工作区 B
restore。这就是 E2B / k8s 的"持久化产物"契约。

## 4. 验证

### 4.1 正确性——真实，无 mock

`tests/test_vfs_snapshot.py`——18 个测试，0 个 mock。正确性预言机是真实
`diff -r` 子进程：`mutate → restore` 后，活树必须与快照字节一致。

覆盖摘要（完整表见 [`SNAPSHOT_zh.md`](SNAPSHOT_zh.md) §4）：

- API 契约：基类上 `NotImplementedError`；`VFSWorkspaceBase` 上真实实现。
- 生命周期守卫：provision 之前 `snapshot` / `restore` 抛 `RuntimeError`。
- tag 校验：空 / `.` / `..` / `a/b` / `../escape` 被拒。
- 往返正确性：`snapshot → mutate（write_file + exec_shell）→ restore` 后
  真实 `diff -r` 为空。
- 快照隔离：snapshot 之后原地改动不漏入快照。
- tag 命名空间：两个 tag 独立；restore 一个不影响另一个。
- 原子替换：重新 snapshot 同一 tag 原子替换。
- 元数据保留：exec 位 + 空目录保留（真实 `stat`）。
- 跨 `close()` 持久性：快照熬过 `close()` + workdir 清空；可 restore 进
  新工作区。
- 技能树保留：种子的 `skills/` 树在 restore 后保留，无需 re-seed。
- 工作区隔离：两个工作区用同一 tag 不冲突。
- 指标：`snapshot_count` 反映快照创建；原子替换不重复计数。
- 端到端可用性：`restore` 之后真实 `exec_shell` 在恢复的树上成功。

测试刻意在 mutate 路径上同时跑两个转译钩子（`write_file` *和*
`exec_shell`），因为无论 agent 用哪个原语改树，快照都必须正确。

### 4.2 性能——真实，无 mock

`tests/benchmarks/bench_pool.py` 提供两个新策略，建模迭代式 agent 工作流：

- `reprovision-rollback`——基线：每次迭代关闭工作区并 provision + 重种子
  一个新的。这是*没有* snapshot/restore 时的回滚成本。
- `snapshot-restore`——新增：种子后一次 `snapshot("rollback")`，之后每次
  迭代 `mutate → restore("rollback")`。工作区保持存活。

canonical 配置（`warmup=10, iters=300, concurrency=8`）：

| 策略 | ops/s | p50 ms | p99 ms |
|------|------:|-------:|-------:|
| reprovision-rollback | 14.8 | 64.645 | 168.157 |
| snapshot-restore | 134.1 | 7.160 | 9.351 |

**9.1 倍吞吐提升，17.9 倍 p99 延迟降低。** 完整表见
[`SNAPSHOT_zh.md`](SNAPSHOT_zh.md) §5 与 `docs/benchmark-results.json`。

可复现性：`warmup=5, iters=100, concurrency=4` 下两次独立运行产生 4.1×
与 3.9× 吞吐比值——在 ~3% 内稳定。

### 4.3 无回归——完整 pytest 套件

```
$ python -m pytest -q
.............................sss........................................ [ 43%]
........................................................................ [ 86%]
.......................                                                  [100%]
164 passed, 3 skipped in 23.49s
```

3 个 skip 是 gVisor / Kata / Sysbox 的运行时探针测试，二进制不存在时
自动跳过——这是设计，不是 mock 作弊。

## 5. 逻辑闭环

brief 要求"如果真的有意义则引入"需要严格逻辑闭环。闭环如下：

1. **调研 → 不变量。** 每个有真实快照原语的被调研系统（16 中 5）都认同
   三条不变量（§2.2）。不变量集不是设计选择——它是任何 snapshot/restore
   实现正确的必要条件。

2. **不变量 → API。** 三条不变量加上"API 跨后端统一"（已是通过
   `verify_runtime_available` 的包惯例）强制 API 形态：在
   `SandboxedWorkspaceExtBase` 上 `snapshot(tag)` / `restore(tag)`，默认
   `NotImplementedError`，`VFSWorkspaceBase` 上真实实现。

3. **VFS 转译约束 → 深拷贝。** VFS 工作区无内核强制 CoW（不同于 containerd
   overlayfs），因此快照必须是深拷贝，才能在任意 `exec_shell` 改动下满足
   不变量 #1（快照隔离）。

4. **深拷贝 → 原子发布 + 崩溃恢复。** 深拷贝可被中断；调研的原子发布模式
   （Firecracker）+ 陈旧临时目录清扫器让拷贝崩溃安全。

5. **真实测试 → 正确。** 18 个无 mock 测试，以 `diff -r` 为预言机，证明
   实现满足不变量。

6. **真实基准 → 有意义。** 在最廉价基线上 9.1× 吞吐 / 17.9× p99（在真实
   后端上被低估）。特性确实比现状更好。

∴ 特性有意义（6）且正确（5），实现由不变量必然推出（3、4），不变量由
调研必然推出（1、2）。**发布。**

## 6. 发布的改动

| 文件 | 改动 |
|------|------|
| `src/agentscope_sandbox_ext/_base.py` | 在 `SandboxedWorkspaceExtBase` 上新增 `snapshot` / `restore` 抽象 API |
| `src/agentscope_sandbox_ext/_vfs/_base.py` | 在 `VFSWorkspaceBase` 上实现 `snapshot` / `restore` / `_snapshot_to` / `_restore_from` / `_cleanup_stale_snapshot_dirs`；新增 `snapshots_root` 构造参数；`metrics()` 加 `snapshot_count` |
| `tests/test_vfs_snapshot.py` | 新增——18 个无 mock 测试 |
| `tests/benchmarks/bench_pool.py` | 新增 `bench_reprovision_rollback` + `bench_snapshot_restore` 策略；新增 `_seed_rollback_workspace` / `_mutate` 辅助；新增 `itertools` 导入 |
| `docs/SNAPSHOT.md` | 新增——调研 + 设计 + 基准分析（英文）|
| `docs/SNAPSHOT_zh.md` | 新增——调研 + 设计 + 基准分析（中文）|
| `docs/SESSION_LOG.md` | 新增——本日志（英文）|
| `docs/SESSION_LOG_zh.md` | 新增——本日志（中文）|
| `docs/benchmark-results.json` | 更新——canonical 配置运行，含两行新策略 |
| `README.md` | 更新——链向 `SNAPSHOT.md` |
| `README_zh.md` | 更新——链向 `SNAPSHOT_zh.md` |

## 7. 后续（不在范围内）

- `FirecrackerWorkspace` 上的原生 `snapshot` / `restore`——Firecracker REST
  API 已暴露 `PUT /snapshot/create` / `PUT /snapshot/load`；VM 内 guest
  agent 需要加 `pause` / `resume` op。`SandboxedWorkspaceExtBase` 上的 API
  形态已就绪。
- `DockerWorkspace` 上的 `tar` 快照——`docker commit` + `docker save` 是
  天然转译；同一 API 形态。
- 未来 `overlayfs` VFS 后端上 overlayfs 支撑的 `_snapshot_to`——钩子已就位；
  CoW 保证能让我们省掉深拷贝成本。
