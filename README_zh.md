# agentscope-sandbox-ext

[English](README.md) | 简体中文

为 [agentScope](https://github.com/agentscope-ai/agentscope) 框架提供的扩展沙箱后端 —— **Firecracker microVM**、**gVisor (runsc)** 与 **Kata Containers**。

本包在不修改任何 agentscope 原生代码的前提下，新增三种 sandboxed-workspace 后端。所有后端均通过继承 agentscope 自身（且文档明确支持子类化）的抽象基类组合而成：

- [`agentscope.workspace._sandboxed_base.SandboxedWorkspaceBase`](https://github.com/agentscope-ai/agentscope/blob/main/src/agentscope/workspace/_sandboxed_base.py) —— 沙箱内 MCP 网关的模板方法生命周期。
- [`agentscope.app.workspace_manager._base.WorkspaceManagerBase`](https://github.com/agentscope-ai/agentscope/blob/main/src/agentscope/app/workspace_manager/_base.py) —— `agentscope.app` 调用的管理器接口。

每个 workspace 都继承自统一的 `SandboxedWorkspaceExtBase`，因此管理面（management plane）可以使用统一的 `isinstance` 判别符和 `metrics()` 钩子，同时完整保留 `SandboxedWorkspaceBase` 的沙箱生命周期（网关引导、MCP 持久化、技能注入、卸载等）。

## 后端一览

| 后端 | `sandbox_kind` | 隔离级别 | 冷启动 | 适用场景 |
|---|---|---|---|---|
| **Firecracker** | `firecracker` | KVM 级 microVM，独立内核 | 约 125 ms 到 guest init | 最强隔离，多租户，不可信代码 |
| **gVisor (runsc)** | `gvisor` | 应用级内核（Sentry 拦截系统调用） | 容器级速度 | 无 VM 开销下的强隔离 |
| **Kata Containers** | `kata` | 硬件 VM 支撑的容器（Firecracker/QEMU hypervisor） | 约 1 秒 | 兼顾 VM 级隔离与容器易用性 |

agentScope 已内置 Docker、E2B、K8s、Apple Container、Bubblewrap、Daytona、OpenSandbox 等后端 —— 本包填补了框架尚未覆盖的三种主流 VM/沙箱运行时，从最轻量、最主流的 Firecracker 开始实现。

## 主要特性

- **不修改原生代码。** 仅通过 import 使用 agentscope，可直接接入任何 `>=2.0.5` 的 agentscope 环境。
- **统一的扩展接口。** `SandboxedWorkspaceExtBase` + `SandboxExtManagerBase` 提供统一的 `isinstance` 判别符、`metrics()` 钩子，以及每个后端都实现的 `verify_runtime_available()` 探针。
- **池化优化。** `SandboxPool` 维持 `min_warm` 个热沙箱，超过 `idle_ttl` 的空闲实例会被驱逐，热池上限为 `max_size`。每个 manager 可在自身缓存前串联一个 pool。
- **真实测试，无 mock。** guest-agent 线协议使用打包进 microVM 的同一份 handler 源码做端到端验证（CI 中由于 AF_VSOCK 不可用，改用 Unix socket）；Firecracker REST API 客户端驱动的是真实的 Unix-socket HTTP 服务端。共 90 个测试，0 处 mock。

## 快速开始

```bash
pip install agentscope-sandbox-ext[docker-runtime]
```

```python
import asyncio
from agentscope_sandbox_ext import (
    GVisorWorkspaceManager,
    IsolationPolicy,
)

async def main():
    async with GVisorWorkspaceManager(
        basedir="/tmp/as-workspaces",
        isolation=IsolationPolicy.PER_AGENT,
    ) as mgr:
        ws = await mgr.get_workspace(
            user_id="alice",
            agent_id="bash",
            session_id="s1",
        )
        result = await ws.get_backend().exec_shell(
            ["sh", "-c", "echo hello from gVisor"],
        )
        print(result.stdout.decode())

asyncio.run(main())
```

## 架构

```
                 ┌─────────────────────────────────────────────┐
                 │           agentscope.app (host)              │
                 │  WorkspaceManagerBase  ← SandboxExtManagerBase
                 └───────────────┬─────────────────────────────┘
                                 │  get_workspace / close / close_all
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│ FirecrackerMgr   │   │  GVisorMgr       │   │   KataMgr        │
│  + SandboxPool    │   │  + SandboxPool   │   │   + SandboxPool   │
└────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
         │                      │                      │
         ▼                      ▼                      ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│FirecrackerWorkspace│  │ GVisorWorkspace  │   │  KataWorkspace    │
│  (microVM + vsock) │   │  (Docker + runsc)│   │  (Docker + kata)  │
└────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
         │                      │                      │
         ▼                      ▼                      ▼
   SandboxedWorkspaceExtBase  (sandbox_kind + metrics + verify_runtime_available)
         │
         ▼
   SandboxedWorkspaceBase  ← agentscope 原生（网关引导、MCP、技能）
```

完整设计见 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)。

## Firecracker 前置条件

Firecracker 需要一台带 KVM 的 Linux 主机、一份内核镜像和一份 ext4 rootfs：

```bash
# /dev/kvm 必须可访问
test -w /dev/kvm && echo OK

# firecracker 二进制位于 $PATH
firecracker --version

# 内核 + rootfs（用 tools/build-rootfs.sh 构建，详见 docs/firecracker.md）
ls /var/lib/firecracker/vmlinux /var/lib/firecracker/rootfs.ext4
```

VM 内的 guest agent（`src/agentscope_sandbox_ext/_firecracker/_guest_agent.py`）必须烘焙进 rootfs，放在 `/root/.agentscope/_guest_agent.py` 并由 init 启动。`tools/build-rootfs.sh` 辅助脚本可完成此操作。

## gVisor / Kata 前置条件

gVisor 与 Kata 是 Docker 的*运行时*（runtime）—— 在 `/etc/docker/daemon.json` 中注册：

```json
{
  "runtimes": {
    "runsc": { "path": "/usr/bin/runsc" },
    "kata-fc": { "path": "/usr/bin/kata-fc" }
  }
}
```

重启 daemon 后验证：

```bash
docker info --format '{{json .Runtimes}}'
```

## 测试

```bash
pip install -e ".[test]"
pytest -q
```

测试从不使用 mock —— 它们启动真实的协议对端（使用同一份 handler 源码串接的 Unix-socket guest-agent 服务端，针对 Firecracker API 的 Unix-socket HTTP 服务端，以及真实的 vsock-bridge 握手服务端）。需要 Docker / Firecracker 的运行时探针测试，在主机上没有相应二进制时会自动跳过。

## 许可证

MIT
