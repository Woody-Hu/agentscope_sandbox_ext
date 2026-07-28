# 沙箱覆盖与性能优化机会

[English](PERFORMANCE.md) | 简体中文

本文档分析：(1) 本包当前支持哪些沙箱后端；(2) 还有哪些主流候选值得加入；(3) 池化 / 缓存 / 调度层可以在哪里做性能收紧。

## 1. 当前已支持的沙箱

### 1.1 本包（`agentscope-sandbox-ext`）

| 后端 | `sandbox_kind` | 类 | 隔离级别 | 冷启动 | 状态 |
|---|---|---|---|---|---|
| Firecracker microVM | `firecracker` | `FirecrackerWorkspace` | KVM 级 VM，独立内核 | 约 125 ms | 已实现 |
| gVisor (runsc) | `gvisor` | `GVisorWorkspace` | 应用级内核（Sentry） | 容器级速度 | 已实现 |
| Kata Containers | `kata` | `KataWorkspace` | VM 支撑的容器 | 约 1 秒 | 已实现 |
| Sysbox | `sysbox` | `SysboxWorkspace` | Docker 运行时（比 runc 隔离更强） | 容器级速度 | 已实现（本分支） |

四者都继承自统一的 `SandboxedWorkspaceExtBase`，管理面可按 `isinstance` 路由，并在 provision 之前调用 `verify_runtime_available()`。

### 1.2 agentScope 原生后端（参考）

agentScope 已内置以下后端，本包*不*重复实现：

| 后端 | 不重复实现的理由 |
|---|---|
| Docker（`DockerWorkspace`） | gVisor / Kata / Sysbox 后端**继承**此类，仅覆盖 `HostConfig.Runtime`。 |
| E2B | 云沙箱，无需主机运行时。 |
| Kubernetes（`K8sWorkspace`） | 集群级，与主机运行时后端正交。 |
| Apple Container | 仅 macOS。 |
| Bubblewrap | Linux namespace jailer —— 与 nsjail 同一细分领域。 |
| Daytona / OpenSandbox | 云沙箱。 |

## 2. 未来后端候选

每个候选都按隔离强度、冷启动开销、与 `SandboxedWorkspaceBase` 契约（POSIX shell + Python venv + MCP 网关）的契合度、实现成本打分。

### 2.1 Cloud Hypervisor（microVM，推荐下一步）

- **隔离：** KVM 级 VM（与 Firecracker 同级）。
- **冷启动：** 约 150 ms —— 与 Firecracker 相当。
- **契合度：** 优秀 —— 与 Firecracker 相同的 vsock / guest agent 传输，完整 POSIX guest。
- **相对 Firecracker 的差异：** 设备热插、热迁移、virtiofs 支持、更灵活的块设备布局。Firecracker 后端约 80 % 代码可复用。
- **成本：** 中 —— 用 `cloud-hypervisor` 替代 `firecracker`，驱动其 `--api-socket` REST API（形状相似），guest agent 原样复用。

### 2.2 WASM/WASI（Wasmtime）—— *不推荐用于 workspace 契约*

- **隔离：** 强（基于能力，默认无系统调用）。
- **冷启动：** <10 ms。
- **契合度：** **差。** `SandboxedWorkspaceBase` 假设有 POSIX shell、Python venv 和 MCP 网关进程。WASI 既无 shell 也无 Python 运行时。要契合 workspace 契约，需要自定义 WASI host 来模拟 `/bin/sh`、文件 I/O 和 Python 解释器 —— 远超“加一个后端”的范畴。
- **结论：** 作为未来*侧信道*执行器（用于短时工具调用）有价值，但不适合作为 `SandboxedWorkspaceBase` 子类。文档化并跳过。

### 2.3 nsjail（Linux jailer）

- **隔离：** Linux namespace + seccomp —— 与 Bubblewrap 同级。
- **契合度：** 好（POSIX guest）。
- **结论：** agentScope 已内置 Bubblewrap，覆盖同一细分领域。**跳过**，除非需要 nsjail 的 per-call 资源限制。

### 2.4 Podman（无 root 容器引擎）

- **隔离：** 容器（默认 runc，可配 runsc/kata）。
- **契合度：** 好，但 Podman 默认不暴露 Docker 兼容 API（`podman.sock` 差异大到 `aiodocker` 无法直接驱动，需要兼容垫片）。
- **成本：** 高 —— 需要新写 `PodmanWorkspace`，绕过 `DockerWorkspace`。
- **结论：** 文档化为未来工作；暂不实现。

### 2.5 QEMU 直接（全系统模拟）

- **隔离：** VM。
- **契合度：** 好但偏重。
- **结论：** Kata 已通过 `kata-qemu` 覆盖了 QEMU 支撑场景。**跳过。**

### 2.6 gVisor / Kata 覆盖已完整

gVisor（`runsc`）和 Kata（`kata-fc`、`kata-qemu`、`kata-clh`）是两种主流 VM/SCS Docker 运行时。无需更多 Docker 运行时后端。

## 3. 性能优化机会

本节列出池化 / 缓存 / 调度层中具体、非破坏性的优化点。标记 **（本分支已实现）** 的已完成；其余作为路线图文档化。

### 3.1 池获取策略 —— **（已实现）**

原池从 `deque` 左端弹出沙箱（FIFO）。FIFO 让每个池内沙箱均匀老化，低流量下所有沙箱会在相近时间同时命中空闲 TTL，预热器不得不一次性替换全部。

**已实现：** 新增 `acquire_strategy` 参数，接受 `"fifo"`（默认，均匀老化）或 `"lifo"`（最新优先，保持最热沙箱热）。LIFO 牺牲均匀老化换缓存局部性 —— 最近返回的沙箱最可能仍有热页缓存和存活的网关连接。

### 3.2 获取时存活探针 —— **（已实现）**

此前 `acquire` 会返回任何在返回时 `is_alive=True` 的沙箱。一个*在池中*崩溃的沙箱（网关 OOM、microVM panic）会被交给下一个调用方，调用方必须自己检测失败并 `release(broken=True)`。

**已实现：** 池上新增可选 `health_check` 回调。设置后，`acquire` 在返回每个候选前调用它；失败的候选被拆除，池尝试下一个。这让“池内沙箱损坏”的情况对调用方不可见。

### 3.3 并发 provision 限流 —— **（已实现）**

当 `N` 个协程同时未命中池（例如启动时一波 session 创建）时，原代码会并行启动 `N` 个 provision。对 Firecracker 来说就是同时启动 `N` 个 microVM —— KVM 调度抖动 + rootfs I/O 争用。

**已实现：** `max_concurrent_provisions` 参数，由 `asyncio.Semaphore` 支撑。默认 `0`（不限流，保留旧行为）；设为 `1`–`N` 串行/限制并发启动。

### 3.4 获取时陈旧驱逐（路线图）

池内空闲 `idle_ttl / 2` 的沙箱仍足够“新鲜”可服务，但空闲几乎到 `idle_ttl` 的沙箱有风险（长时间空闲的 microVM 偶有时钟漂移 / 网关连接掉线）。`max_age` 上限在*返回前*驱逐可让池只服务新鲜沙箱。尚未实现 —— 留作路线图，因为存活探针（3.2）已覆盖安全侧，sweeper 已限制年龄。

### 3.5 按用户/按 agent 池分区（路线图）

当前池是全局的 —— 单一 free list 跨所有租户共享。多租户主机上，吵闹的租户可能抽干池并饿死其他租户。按 `user_id`（或分片键）对 free list 分区，并设每分片 `max_size`，可给每个租户保底的热沙箱数。本分支未实现 —— 需要改动 manager API（`get_workspace` 需要把分区键传给 `acquire`）。

### 3.6 镜像构建缓存共享（已由 agentscope 处理）

`DockerWorkspace` 已对 Dockerfile + COPY 文件做内容哈希，命中即跳过重建，所以 gVisor / Kata / Sysbox 天然继承镜像构建缓存。无需额外动作。

### 3.7 Manager 级缓存驱逐策略（路线图）

manager 的 `_cache` 是普通 `dict` 加 TTL 驱逐。内存压力下，严格 LRU（超硬上限时驱逐最久未用）比当前“仅在过期时驱逐”更好。优先级低 —— TTL sweeper 实际已限制缓存规模。

### 3.8 预热突发预测（路线图）

预热器是反应式的（仅在 tick 时补到 `min_warm`）。预测式变体可观察 acquire 速率，在预测突发前预热，平滑突发工作负载的启动。调参难，留作路线图。

### 3.9 池指标已就位

`SandboxPool.metrics()` 已上报 `warm`、`in_use`、`total`、`max_size`、`min_warm`、`closed`。`manager_metrics()` 在此之上扩展 `backend_kind` 与 `cache_size`。基础可观测性无需更多；更丰富的指标（provision 延迟直方图、acquire 等待直方图）需要 stats 库，超出范围。

## 4. 总结

| 领域 | 状态 |
|---|---|
| 后端 | Firecracker、gVisor、Kata（已有）+ Sysbox（本分支）。Cloud Hypervisor 是推荐的下一后端；WASM 与 nsjail 不契合；Podman 是未来项目。 |
| 池化 | LIFO/FIFO 策略、存活探针、并发 provision 限流（本分支）。分区与预测式预热是路线图。 |
| 缓存 | TTL sweeper + 内容哈希镜像构建（已就位）。严格 LRU 是路线图。 |
| 调度 | 基于条件的 acquire 背压 + 超时（已就位）。按租户分区是路线图。 |
