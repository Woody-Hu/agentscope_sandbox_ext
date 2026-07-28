# 架构设计

[English](ARCHITECTURE.md) | 简体中文

本文档介绍 `agentscope-sandbox-ext` 的设计 —— 三种沙箱后端（Firecracker、gVisor、Kata Containers）如何在不修改任何原生代码的前提下接入 agentScope，以及池化层如何在它们之前组合使用。

## 设计约束

本包的设计基于任务要求的四条硬性约束：

1. **不修改原生代码。** 仅从 agentscope `import`。
2. **统一继承接口**，使后续管理面可以按 `isinstance` 路由，无需按后端分支。
3. **池化** 作为可选优化层，可组合在任何 manager 之前。
4. **真实测试，无 mock。** 线协议使用真实协议对端做端到端验证。

## 分层组合

```
            ┌─────────────────────────────────────────────────────────────┐
            │           agentscope.app  (host)                            │
            │                                                             │
            │   WorkspaceManagerBase          SandboxedWorkspaceBase       │
            │   (抽象管理器)                  (模板方法生命周期)            │
            │          ▲                              ▲                    │
            │          │ 子类继承                     │ 子类继承            │
            └──────────┼──────────────────────────────┼────────────────────┘
                       │                              │
            ┌──────────┴──────────────┐    ┌──────────┴────────────────┐
            │  SandboxExtManagerBase   │    │  SandboxedWorkspaceExtBase│
            │   + backend_kind        │    │   + sandbox_kind           │
            │   + manager_metrics()   │    │   + metrics()              │
            │   (本包)                 │    │   + verify_runtime_available()│
            └─────┬──────┬──────┬──────┘    └─────┬──────┬──────┬────────┘
                  │      │      │                  │      │      │
            ┌─────┴─┐ ┌──┴──┐ ┌─┴────┐      ┌─────┴─┐ ┌──┴──┐ ┌─┴────┐
            │ FC Mgr│ │GVis │ │Kata  │      │ FC WS │ │GVis │ │Kata  │
            │       │ │ Mgr │ │ Mgr  │      │       │ │ WS  │ │ WS   │
            └───┬───┘ └──┬──┘ └──┬───┘      └───┬───┘ └──┬──┘ └──┬───┘
                │        │       │              │        │       │
                └────────┴───────┴──────────────┴────────┴───────┘
                                  │
                          (可选) SandboxPool
                                  │
                          ┌───────┴────────┐
                          │  预热循环        │
                          │  空闲驱逐        │
                          │  容量上限        │
                          └────────────────┘
```

### 为什么是两个基类而不是一个？

agentScope 把 workspace 的*生命周期*（`SandboxedWorkspaceBase` —— 网关引导、MCP、技能）与 workspace 的*所有权*（`WorkspaceManagerBase` —— 缓存、TTL、隔离）拆分。扩展包镜像这一拆分，使继承图保持浅显，避免出现混合关注点的“上帝类”。

### `verify_runtime_available` 探针

每个后端都实现 `classmethod async verify_runtime_available()`，当主机无法运行该后端时抛出带可读信息的 `RuntimeError`。Manager 在尝试 provision 之前调用它，使错误同步暴露，而不是在 VM 启动一半时才失败。

| 后端 | 探针 |
|---|---|
| Firecracker | `$PATH` 上的 `firecracker --version`、`/dev/kvm` 可写、内核 + rootfs 存在 |
| gVisor | `$PATH` 上的 `runsc --version`、Docker daemon 对 `/v1.41/info` 响应 |
| Kata | `$PATH` 上的 `kata-runtime --version`、Docker daemon 响应 |

## Firecracker 后端

```
┌──────────── 主机 ─────────────────────────────┐
│  FirecrackerWorkspace                        │
│    │                                         │
│    ├── 启动 `firecracker --api-socket …`     │
│    ├── FirecrackerApi (Unix-socket HTTP)     │
│    │     PUT /boot-source                    │
│    │     PUT /machine-config                  │
│    │     PUT /drives/rootfs                   │
│    │     PUT /vsock  (CID = 主机分配)         │
│    │     POST /actions instance.start          │
│    │                                         │
│    └── GuestAgentClient (vsock → Unix socket)│
│          exec_shell / read_file / write_file  │
└──────────────────┬─────────────────────────────┘
                   │ virtio-vsock
┌──────────────────┴──────────── guest ──────────┐
│  init → python3 /root/.agentscope/_guest_agent.py
│     AF_VSOCK 监听 socket                      │
│     长度前缀 JSON 协议                         │
│     exec_shell / read_file / write_file / ping │
└──────────────────────────────────────────────────┘
```

guest agent 是一个纯标准库 Python 脚本，内置在 rootfs 中。它监听 `AF_VSOCK` 并使用长度前缀 JSON 协议。同一份源码字符串也被测试套件以 Unix-socket 传输方式进行验证，因此线协议可以端到端验证，CI 中无需运行真实 microVM。

## gVisor / Kata 后端

这两个后端完整复用 `agentscope.workspace.DockerWorkspace` 的流程（镜像构建、bind-mount、网关引导），仅覆盖 `HostConfig.Runtime`：

```python
config = {
    "Image": self._image_tag,
    "HostConfig": {
        "Runtime": self._runtime,   # "runsc" 或 "kata-fc"
        "Binds": [...],
    },
    ...
}
```

这是最小的 delta —— 其他所有关注点（网关端口、MCP 持久化、技能注入）均原样继承。

## 池化层

`SandboxPool` 是一个独立的构建块 —— 它本身不实现 `WorkspaceManagerBase`。Manager 在自身缓存之前串联它：

```python
class FirecrackerWorkspaceManager(SandboxExtManagerBase):
    def __init__(self, ..., pool: SandboxPool | None = None):
        ...
        self._pool = pool or SandboxPool(factory=self._make, max_size=8, min_warm=2)
```

### 驱逐路径

1. **空闲驱逐** —— 后台 sweeper 关闭 `last_returned` 时间戳早于 `idle_ttl` 的池内沙箱。
2. **容量上限** —— `max_size` 限制热池；超额返回的沙箱立即被拆除，而不是排队。
3. **等待路径** —— 当无热沙箱可用且池已达到 `max_size` 时，`acquire` 阻塞（带超时）。这是 manager 负载下的预期背压路径。

### 并发

- 每个公开方法都是协程，且并发安全；`asyncio.Lock` 保护 free list 的修改。
- 预热任务是 best-effort：它在后台尝试维持 `min_warm` 个就绪沙箱，失败时按指数退避重试。失败被记录并吞掉，瞬时的 provision 错误不会击垮池。
- `acquire` 使用 `asyncio.Condition`，使阻塞调用方在沙箱返回时被及时唤醒。

## 测试策略

测试从不使用 mock。它们启动真实的协议对端：

- guest-agent 线协议使用打包进 microVM 的同一份 handler 源码做端到端验证。CI 中由于 `AF_VSOCK` 在 VM 外不可用，改用 Unix socket。
- Firecracker REST API 客户端驱动的是用 `asyncio.start_server` 搭建的真实 Unix-socket HTTP 服务端 —— 与 Firecracker 自身使用的 socket 类型相同。
- 池化测试使用一个真实（且廉价）的沙箱子类，它会真正翻转 `is_alive` 标志并记录每次 provision / close，因此预热、空闲驱逐、容量上限都能在无 mock 下被观察到。
- 需要 Docker / Firecracker 的运行时探针测试，在主机上没有相应二进制时自动跳过（用 `@pytest.mark.integration` 标记）。

## 为什么不只做一个后端？

三种后端覆盖了隔离 / 开销权衡曲线上三个不同的点：

- **Firecracker** —— 最强隔离（独立内核），亚秒级冷启动，但需要 KVM 和 rootfs。
- **gVisor** —— 容器级速度，应用级内核，无 VM 开销，但 Sentry 不是完整内核。
- **Kata** —— VM 级隔离 + 容器易用性，但比 gVisor 更重。

管理面可以同时提供三者，并按 per-agent 隔离策略选择 —— 统一的 `SandboxedWorkspaceExtBase` 判别符让路由变得平凡。
