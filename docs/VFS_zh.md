# VFS 工作区后端 —— 设计与基准测试

## 1. 动机

所有现有沙箱后端（Firecracker、gVisor、Kata、Sysbox）provision 的都是
*真实*的隔离边界 —— microVM 或容器 —— 这对不可信代码是正确选择，但也是
主导的延迟成本：

| 后端 | 冷启动 | 隔离边界 |
|------|--------|----------|
| Firecracker | ~1–3 s | microVM (KVM) |
| gVisor | ~300–800 ms | 系统调用过滤（宿主内核）|
| Kata | ~500 ms–2 s | Docker 运行时下的 microVM |
| Sysbox | ~300–600 ms | 命名空间虚拟化容器 |

但对三类工作负载来说，每次 provision 都付隔离税是浪费的：

1. **CI / 开发循环** —— agent 自己的测试套件、lint、类型检查、文档构建。
   代码可信，只需要一个干净的工作目录。
2. **基准测试基线** —— 要度量池化 / 调度开销，需要一个 provision 成本
   趋近于零的后端，这样数据才能隔离出*池*而非*后端*。
3. **可插拔 VFS 转译** —— 有些部署想把 `exec_shell` / `read_file` /
   `write_file` 转译成非容器的东西（9p 服务器、FUSE 挂载、进程内解释器、
   远端网关）。今天每个新转译都是一个从零开始的后端。

VFS 后端家族同时解决这三个问题。

## 2. 设计

### 2.1 VFS 插入点

`agentscope.tool._builtin._backend.BackendBase` 定义了每个后端必须实现的
三个抽象原语：

```python
async def exec_shell(command, *, cwd=None, timeout=None) -> ExecResult
async def read_file(path) -> bytes
async def write_file(path, data: bytes) -> None
```

每个上层 agent 工具（Bash、Read、Write、Edit、Grep、Glob）都从这三个原语
派生。所以一个 **VFS 后端** 就是一个 `BackendBase` 子类，把三个原语
*转译*为对虚拟工作区的操作，而不是容器。

继承链与 Firecracker 后端对称：

```
BackendBase                                  ← agentscope
└── VFSBackendBase                           ← 本包，抽象
    └── AgentFSBackend                       ← 参考实现

SandboxedWorkspaceBase                       ← agentscope
└── SandboxedWorkspaceExtBase                ← 本包
    └── VFSWorkspaceBase                    ← 本包，抽象
        └── AgentFSWorkspace                 ← 参考实现
```

### 2.2 `VFSBackendBase` —— 转译契约

`VFSBackendBase` 把三个原语留作抽象（`_exec_translated` /
`_read_translated` / `_write_translated`），并提供共享管道：缓存的 `workdir`
（让 `getcwd()` 不付转译往返）、`BackendBase` 原语接线、`close()` 生命周期钩子。

一个新的 VFS 后端是 ~50 行子类：实现三个转译钩子即可。派生的文件系统辅助方法
（`file_exists`、`is_dir`、`list_dir`、`stat_mtime`、`delete_path`）从
`BackendBase` 继承，开箱即用。

### 2.3 `VFSWorkspaceBase` —— 生命周期契约

`VFSWorkspaceBase` 为零运行时情形特化 `SandboxedWorkspaceExtBase`：

* `verify_runtime_available()` —— 总是成功（无需探测 Docker / KVM / 运行时）。
* `_provision_backend()` —— 实例化 VFS 后端（纯 Python 对象）；微秒级成本。
* `_bootstrap_commands()` —— 返回 `[]`（无 guest agent / 镜像引导）。
* `initialize()` —— 重写以跳过 MCP 网关设置。VFS 工作区没有沙箱内网关进程，
  所以网关引导 / 健康轮询路径不适用；其它一切（provision 后端、确保工作区
  布局、种子技能）照常运行。
* `get_backend()` —— 返回绑定的 VFS 后端。

### 2.4 `AgentFSBackend` —— 参考实现

`agentfs` 把三个原语转译为限定在每工作区宿主目录内的宿主 I/O + 子进程：

* `read_file` / `write_file` —— 直接 `open()` 解析到工作区根下的宿主路径。
  会逃逸工作目录的路径遍历抛 `PermissionError`。
* `exec_shell` —— `asyncio.create_subprocess_exec`，`cwd` 解析到工作目录下。
  双保险：逃逸工作目录的 `cwd` 以退出码 126 拒绝。

因为没有镜像构建、没有容器启动、没有 guest agent，`agentfs` 的 provision
就是一次 `os.makedirs` 加一个 Python 对象 —— 本包最廉价的后端，也是基准测试
天然的"理论上限"基线。

## 3. 其它 VFS 实现（草图）

转译层刻意做窄；其它 VFS 后端各 ~50 行：

| 后端 | `exec_shell` | `read_file` / `write_file` | 用例 |
|------|--------------|----------------------------|------|
| `agentfs`（已发布）| 宿主子进程 | 宿主 `open()` | CI / 开发 / 基准基线 |
| `memoryfs`（未来）| 进程内解释器 | 内存字典 | 纯单元测试，无宿主 I/O |
| `ninep`（未来）| 9p 协议到远端 | 9p 协议 | 远端工作区服务器 |
| `fuse`（未来）| 宿主子进程挂载 FUSE | FUSE 读写 | 自定义 FS 布局 |

## 4. 基准测试

### 4.1 方法论

基准测试工具（`tests/benchmarks/bench_pool.py`）度量**真实**的
acquire / exec / release 延迟与稳态吞吐量，覆盖本包支持的优化策略，
使用零成本 `agentfs` 后端，所以数据反映的是*池化*与*调度*开销 —— 而非
容器启动成本。

对比策略：

| 策略 | 描述 |
|------|------|
| `direct` | 无池；每个请求 provision + 销毁 |
| `pool-fifo` | `SandboxPool`，`acquire_strategy="fifo"` |
| `pool-lifo` | `SandboxPool`，`acquire_strategy="lifo"` |
| `prewarm` | 池 + `min_warm=N`（空闲负载下的温池）|
| `prewarm+ratelimit` | 池 + `min_warm=N` 且 `max_concurrent_provisions=1` |

指标：冷/热 acquire 延迟、exec 延迟、release 延迟、稳态吞吐量（ops/s）、
p50/p95/p99 acquire 尾延迟。

### 4.2 结果

配置：`warmup=10, iters=300, concurrency=8`（在沙箱宿主上运行；
绝对值依赖环境 —— *相对*形态才是重点）。

| 策略 | cold ms | hot ms | exec ms | release ms | ops/s | p50 ms | p95 ms | p99 ms |
|------|--------:|-------:|--------:|-----------:|------:|-------:|-------:|-------:|
| direct | 6.387 | 6.387 | 3.084 | 0.000 | 21.9 | 46.292 | 51.376 | 58.447 |
| pool-fifo | 6.506 | 0.001 | 3.604 | 0.002 | 62.4 | 16.726 | 21.265 | 39.537 |
| pool-lifo | 6.347 | 0.001 | 3.389 | 0.002 | 57.5 | 18.391 | 21.373 | 40.987 |
| prewarm | 6.392 | 0.001 | 3.371 | 0.002 | 59.6 | 17.480 | 22.197 | 41.956 |
| prewarm+ratelimit | 7.512 | 0.001 | 3.511 | 0.002 | 44.9 | 21.113 | 29.271 | 74.297 |

### 4.3 发现

1. **池化是最大的单项收益。** `direct` 在并发 8 下仅 21.9 ops/s；
   `pool-fifo` 达到 62.4 ops/s —— **2.8 倍吞吐提升** —— 因为稳态路径
   不再为每个请求付 provision + close 成本。

2. **热 acquire 比冷快 ~6000 倍。** 冷 acquire 6.4 ms（provision 一个
   新 `agentfs` 工作区）；热 acquire 0.001 ms（从空闲列表弹出）。这量化了
   池化的全部前提。

3. **FIFO 稳态吞吐略胜 LIFO**（62.4 vs 57.5 ops/s）。`agentfs` 后端每
   工作区无状态，LIFO 的 guest 缓存局部性优势不适用；小差距在运行间噪声
   范围内。在真实容器后端上（guest 页缓存重要），LIFO 预期会胜出。

4. **预热在持续负载下无益。** `prewarm`（59.6 ops/s）与 `pool-lifo`
   （57.5 ops/s）持平。预热只在空闲后的*首批*请求有帮助；持续负载下池已
   被 release 填满。

5. **`max_concurrent_provisions=1` 在负载下损害吞吐。** 限流变体降到
   44.9 ops/s（对比 `pool-fifo` 62.4），p99 膨胀到 74 ms。这个旋钮是
   **惊群式冷启动的安全阀**，针对并行 provision 争用的后端（Firecracker
   KVM 调度抖动、rootfs I/O 争用）；它*不是*吞吐优化，对 VFS 类后端应
   默认 `0`（无限）。

### 4.4 推荐

| 工作负载 | 推荐策略 | 原因 |
|----------|----------|------|
| CI / 开发（可信代码）| `agentfs` + `pool-fifo` | 零运行时，2.8 倍吞吐 |
| Firecracker 突发负载 | `pool-lifo` + `prewarm(min_warm=2)` | LIFO 保持最热的 VM；预热吸收首批突发 |
| Firecracker 惊群式冷启动 | 加 `max_concurrent_provisions=2` | 防止大规模冷启动的 KVM 抖动，接受吞吐代价 |
| 持续高吞吐 | `pool-fifo`，`min_warm=0` | 预热在持续负载下无用；FIFO 略快 |

## 5. 运行基准测试

```bash
# 默认配置（warmup=5, iters=200, concurrency=8）
python -m tests.benchmarks.bench_pool

# 更大样本以稳定尾百分位
python -m tests.benchmarks.bench_pool --warmup 10 --iters 300 --concurrency 8

# 机器可读 JSON
python -m tests.benchmarks.bench_pool --json out.json

# PNG 柱状图（需要 matplotlib）
python -m tests.benchmarks.bench_pool --plot out.png
```

每次运行都会把快照写入 `docs/benchmark-results.json`，文档可引用可复现结果。
