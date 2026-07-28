# Firecracker 后端

[English](firecracker.md) | 简体中文

本文档介绍如何准备运行 Firecracker 后端的主机、VM 内 guest agent 的接入方式，以及测试套件如何验证线协议。

## 主机前置条件

```bash
# 1. KVM 设备节点必须存在且对 firecracker 用户可写。
test -c /dev/kvm && test -w /dev/kvm && echo OK

# 2. firecracker 二进制位于 $PATH（或设置 DEFAULT_FIRECRACKER_BIN）。
firecracker --version

# 3. 内核镜像 —— 未压缩 ELF Linux 内核，使用 Firecracker 推荐的
#    guest 配置构建（virtio_blk、virtio_net、virtio_vsocks、
#    8250 serial、ext4）。
ls -l /var/lib/firecracker/vmlinux

# 4. 根文件系统 —— ext4 镜像，含 /bin/sh、python3 和 socat/nc。
ls -l /var/lib/firecracker/rootfs.ext4
```

任何一项缺失时，`FirecrackerWorkspace.verify_runtime_available()` 会抛出带描述的 `RuntimeError`，manager 拒绝 provision。

## 构建 rootfs

rootfs 必须包含：

- 可用的 `/bin/sh` 与 `/usr/bin/python3`。
- 用于 vsock guest agent 引导的 `socat` 或 `nc`（若 agent 在构建时烘焙进去则可选）。
- agentscope guest agent 位于 `/root/.agentscope/_guest_agent.py`，由 init 启动。

使用辅助脚本：

```bash
sudo tools/build-rootfs.sh /var/lib/firecracker/rootfs.ext4
```

脚本会：

1. 创建 512 MiB ext4 镜像。
2. 通过 `debootstrap` 引导一个最小化 Debian。
3. 安装 `python3` 和 `socat`。
4. 从本包复制 `_guest_agent.py` 到 `/root/.agentscope/`。
5. 添加 init 入口（`/etc/rc.local`）以在启动时启动 guest agent。

## 线协议

Host → guest 请求在 virtio-vsock 设备上以 `[4 字节大端长度][json]` 帧格式发送。guest agent 在 `AF_VSOCK` 的 `DEFAULT_GUEST_AGENT_PORT`（默认 1024）端口监听。Firecracker 在主机上将 vsock 设备暴露为 Unix-domain socket；host 拨号后发送 `CONNECT <port>\n` 桥接到 guest 端口。

### 操作

| `op` | 请求字段 | 响应字段 |
|---|---|---|
| `ping` | — | `ok`、`pong` |
| `exec` | `argv: list[str]`、`timeout: float` | `ok`、`exit_code`、`stdout`（base64）、`stderr`（base64） |
| `read_file` | `path: str` | `ok`、`data`（base64） —— 或 `ok=false, error` |
| `write_file` | `path: str`、`data`（base64） | `ok` —— 或 `ok=false, error` |

agent 故意保持精简：仅使用标准库，无第三方依赖，因此可在任何提供 `python3` 的镜像上运行。

## 生命周期

```
FirecrackerWorkspace.initialize()
    │
    ├── verify_runtime_available()       # 探针主机
    ├── spawn firecracker --api-socket …  # 异步子进程
    ├── 等待 API socket 出现               # 5s 超时
    │
    ├── PUT /boot-source                 # kernel_image_path + boot_args
    ├── PUT /machine-config              # vcpu_count, mem_size_mib
    ├── PUT /drives/rootfs               # rootfs 路径, read/write
    ├── PUT /vsock                       # guest CID
    ├── POST /actions instance.start     # 启动 VM
    │
    ├── 等待 guest agent                 # ping 循环, 30s 超时
    └── SandboxedWorkspaceBase.bootstrap_gateway()
            │
            └── 通过 vsock exec / 文件操作：
                安装 agentscope 到网关 venv，
                写入网关脚本，
                启动网关，
                轮询网关 /healthz
```

## 池调优

Firecracker microVM 比容器重，因此默认池较保守：

| 旋钮 | 默认 | 效果 |
|---|---|---|
| `max_size` | 4 | 热门 microVM 硬上限 |
| `min_warm` | 1 | 启动时预热 1 个 microVM |
| `idle_ttl` | 1800 s | 30 分钟后回收空闲 microVM |

对于突发流量的多租户主机，可将 `min_warm` 提到 2-3。对于单用户开发机，可设 `min_warm=0` 完全禁用预热。

## CI 测试

CI 无法运行真实 Firecracker microVM（无 `/dev/kvm`），因此测试套件针对一个真实 Unix-socket 服务端验证线协议，该服务端加载的 `GUEST_AGENT_SOURCE` 字符串与 workspace 写入 rootfs 的完全一致：

```python
from tests._helpers.guest_agent_server import run_guest_agent_server

async with run_guest_agent_server() as path:
    client = GuestAgentClient(connect=make_unix_connect(path))
    result = await client.exec_shell(["echo", "hello"])
    assert result.exit_code == 0
    assert result.stdout.strip() == b"hello"
```

这无需 VM 即可端到端验证协议，意味着 host 客户端与 guest handler 之间的任何漂移都能被立即捕获。
