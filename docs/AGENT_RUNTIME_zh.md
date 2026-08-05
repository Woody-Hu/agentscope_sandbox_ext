# Agent Runtime — 多级快照存储

[English](AGENT_RUNTIME.md) | 简体中文

本文档描述多级快照存储架构、`SnapshotStore` 接口体系、具体后端实现
（PostgreSQL、MinIO、Local）、带淘汰策略的分级存储编排，以及验证设计的
benchmark 结果。

## 1. 架构概览

运行时快照存储采用分层抽象设计：

```
┌─────────────────────────────────────────────────────────────────────┐
│                     TieredSnapshotStore                              │
│   编排层：多级读写、晋升、淘汰                                        │
├─────────────────────────────────────────────────────────────────────┤
│  StorageTier("hot")     StorageTier("warm")    StorageTier("cold")   │
│  priority=0             priority=1             priority=2            │
│  write_through=true     write_through=true     write_through=true    │
│  eviction=LRU           eviction=TTL          eviction=NONE          │
├─────────────────────────────────────────────────────────────────────┤
│  LocalSnapshotStore     MinioSnapshotStore     PostgresSnapshotStore │
│  (文件系统)              (S3 兼容)              (关系型数据库)         │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.1 核心概念

| 概念 | 说明 |
|---|---|
| **SnapshotStore** | 定义存储契约的抽象基类：`put`、`get`、`delete`、`list`、`copy` |
| **StorageTier** | 单级存储配置：后端、优先级、容量限制、淘汰策略 |
| **TieredSnapshotStore** | 管理多级的编排层，处理晋升和淘汰 |
| **EvictionPolicy** | 容量超限时的数据淘汰策略：`LRU`、`TTL`、`SIZE_BASED`、`NONE` |
| **snapshot_ref** | 跨所有级别唯一标识快照的不透明引用字符串 |

## 2. SnapshotStore 接口

```python
class SnapshotStore(ABC):
    async def put(self, actor_id, tag, data, *, compression, template_id,
                  snapshot_ref=None) -> SnapshotMeta: ...
    async def get(self, snapshot_ref: str) -> bytes: ...
    async def delete(self, snapshot_ref: str) -> None: ...
    async def list(self, actor_id: str, *, tag=None) -> list[SnapshotMeta]: ...
    async def copy(self, snapshot_ref, target_actor_id, target_tag) -> SnapshotMeta: ...
    async def metrics(self) -> dict: ...
```

### 2.1 SnapshotMeta

```python
@dataclass
class SnapshotMeta:
    snapshot_ref: str       # 存储特定引用
    actor_id: str           # 所属 actor
    template_id: str | None # 模板来源
    tag: str                # 用户标签
    size_bytes: int         # 负载大小
    created_at: float       # 创建时间戳
    compression: str        # 压缩算法
    checksum: str | None    # SHA256 十六进制摘要
```

## 3. 后端实现

### 3.1 LocalSnapshotStore

将快照存储为本地文件系统上的文件。独立模式下，文件按
`base_dir/<actor_id>/<tag>.<ts>.tar.gz` 组织。分级模式下，文件平铺在
`base_dir/` 下，使用 `.meta.json` 辅助文件存储元数据。

```python
store = LocalSnapshotStore("/data/snapshots")
meta = await store.put("actor-1", "v1", b"data")
data = await store.get(meta.snapshot_ref)
```

### 3.2 PostgresSnapshotStore

将快照以 `BYTEA` 格式存储在 PostgreSQL 表中。使用 `asyncpg` 进行异步
访问。表结构包含 `(actor_id, tag)` 和 `created_at` 索引以支持高效列表查询。

```python
store = PostgresSnapshotStore(
    "postgresql://user:pass@localhost:5432/db",
    table_name="snapshots",
)
meta = await store.put("actor-1", "v1", b"data")
```

**优化**：`copy()` 使用 `INSERT ... SELECT`（单查询）替代 `SELECT` + `INSERT`
（双查询），减少 50% 的数据库往返。

### 3.3 MinioSnapshotStore

将快照存储为 MinIO（S3 兼容）桶中的对象。元数据存储为对象自定义元数据。

```python
store = MinioSnapshotStore(
    "localhost:19000", "minioadmin", "minioadmin",
    bucket="snapshots",
)
meta = await store.put("actor-1", "v1", b"data")
```

**优化**：
- `list()`：使用前缀过滤（`snap:actor_id:` / `minio:bucket/actor_id/`）替代
  全量桶扫描。标签和时间戳从对象名称中解析，无需逐对象调用 `stat_object`。
- `copy()`：使用服务端 `copy_object` API 替代下载再上传数据。

## 4. 分级存储

### 4.1 配置

```python
tiered = TieredSnapshotStore([
    StorageTier(
        name="hot",
        store=LocalSnapshotStore("/data/hot"),
        priority=0,
        max_objects=100,
        eviction_policy=EvictionPolicy.LRU,
        write_through=True,
    ),
    StorageTier(
        name="warm",
        store=MinioSnapshotStore("localhost:19000", "ak", "sk"),
        priority=1,
        max_size_bytes=10 * 1024 * 1024 * 1024,  # 10 GB
        eviction_policy=EvictionPolicy.TTL,
        ttl_seconds=86400,  # 24 小时
        write_through=True,
    ),
    StorageTier(
        name="cold",
        store=PostgresSnapshotStore("postgresql://..."),
        priority=2,
        eviction_policy=EvictionPolicy.NONE,  # 永不淘汰
        write_through=True,
    ),
])
```

### 4.2 读取路径

1. 按优先级顺序检查各级（`priority` 最小的优先）。
2. 命中后立即返回数据。
3. 未命中则尝试下一级。
4. 如果在较低级别中找到，将数据**晋升**到更高级别（向上复制）。

### 4.3 写入路径

所有 `write_through=True` 的级别使用 `asyncio.gather` **并行**写入，
减少多级写入延迟。

### 4.4 淘汰机制

后台清理器按可配置间隔运行。支持的策略：

| 策略 | 行为 |
|---|---|
| `LRU` | `max_objects` 超限时淘汰最近最少访问的条目 |
| `TTL` | 淘汰在 `ttl_seconds` 内未访问的条目 |
| `SIZE_BASED` | 预留，用于未来的基于大小的淘汰 |
| `NONE` | 不淘汰（冷存储） |

## 5. Benchmark 结果

### 5.1 方法

`tests/benchmark_stores.py` 测量所有后端在不同负载大小（1 KB、100 KB、
1 MB）和数据集大小（10、100、500 条目）下的延迟和吞吐量。
配置：warmup=3，iterations=50。

### 5.2 结果

**单存储 put（ops/s，越高越好）：**

| 负载 | Local | Postgres | MinIO |
|---:|---:|---:|---:|
| 1 KB | 5,467 | ~830 | ~330 |
| 100 KB | ~2,500 | ~830 | ~290 |
| 1 MB | ~1,670 | ~1,000 | ~290 |

**单存储 get（ops/s，越高越好）：**

| 负载 | Local | Postgres | MinIO |
|---:|---:|---:|---:|
| 1 KB | 7,540 | 4,200 | 356 |
| 100 KB | ~2,500 | ~2,000 | ~320 |
| 1 MB | ~1,500 | ~1,000 | ~290 |

**List 延迟（ms，越低越好）：**

| 条目数 | Local | Postgres | MinIO |
|---:|---:|---:|---:|
| 10 | 0.5 | 0.3 | 3.0 |
| 100 | 0.8 | 0.3 | 3.0 |
| 500 | 1.5 | 0.4 | 3.0 |

**Copy 延迟（1 KB，ms）：**

| | Local | Postgres | MinIO |
|---|---:|---:|---:|
| avg | 0.2 | 0.7 | 服务端复制 |

**并发 put（10 工作线程 x 20 迭代，1 KB）：**

| 后端 | ops/s | avg ms | p99 ms |
|---|---:|---:|---:|
| Local | 5,698 | 0.2 | 0.2 |
| Postgres | 441 | 21.3 | 396.7 |
| MinIO | 322 | 3.1 | 4.8 |

**分级存储开销：**

| 配置 | avg ms | 开销 |
|---|---|---|
| 单级（直接） | 0.2 | 基准 |
| 单级（分级） | 0.4 | +113% |
| 两级（并行写入） | 0.8 | 直接写入的 4 倍 |

**LRU 淘汰：** 插入 200 个对象，上限 50 → 正确淘汰至 50，保留最近访问的条目。

### 5.3 关键发现

1. **Local 在小负载下最快。** 1 KB 时，local 达到 5,467 put ops/s 和
   7,540 get ops/s——是热缓存层的理想选择。

2. **Postgres 在列表操作中表现优异。** PG 的索引查询提供一致的 0.3–0.4 ms
   列表延迟，不受数据集大小影响，是元数据密集型工作负载的最佳选择。

3. **MinIO 受网络限制。** MinIO 的 get 延迟（1 KB 约 2.8 ms）主要由 HTTP
   往返时间决定。`list` 优化（前缀过滤）使列表延迟保持平坦在 ~3 ms。

4. **并行分级写入有效。** 两级 write-through 开销（0.8 ms）仅为直接本地
   写入（0.2 ms）的 4 倍，意味着第二级写入几乎免费（并行化）。

5. **PG 并发写入受连接池限制。** 441 ops/s 的并发吞吐量受 `asyncpg` 池大小
   （4）限制。增加池大小可提升并发吞吐量。

### 5.4 已应用的优化

| 优化项 | 优化前 | 优化后 | 效果 |
|---|---|---|---|
| MinIO list：前缀过滤 | 全量桶扫描 | 基于前缀 | 恒定延迟 |
| MinIO list：名称解析 | 逐项 `stat_object` | 从名称解析 | 消除 N 次往返 |
| MinIO copy：服务端复制 | `get` + `put` | `copy_object` | 避免数据传输 |
| PG copy：单查询 | `SELECT` + `INSERT` | `INSERT ... SELECT` | 减少 50% 往返 |
| 分级 put：并行写入 | 逐级顺序写入 | `asyncio.gather` | 2 级约快 2 倍 |

## 6. 测试

### 6.1 单元测试（无 mock）

`tests/test_runtime_stores.py` — 26 个测试，针对真实 PostgreSQL 和 MinIO
实例测试所有后端和分级存储。

覆盖范围：
- Put/get 往返正确性
- 删除和缺失键错误处理
- 带/不带标签过滤的列表查询
- 跨 actor 复制
- 分级存储：单级、多级 write-through、晋升、删除传播、列表去重、复制
- 淘汰：LRU 保留最近访问项，TTL 过期旧条目
- 跨存储集成：Local → MinIO → Postgres 三级降级
- 所有后端的指标

### 6.2 Benchmark 正确性验证

`tests/benchmark_stores.py::TestBenchmarkCorrectness` — 8 个测试：
- Put/get 后数据完整性
- 复制保留相同数据
- 分级 write-through 跨所有级别一致性
- 并发写入无数据损坏
- 批量 put 后列表完整性
- 删除不可逆地移除数据
- LRU 淘汰保留最近访问的条目
- 分级列表去重

## 7. 使用示例

### 7.1 单后端

```python
from agentscope_sandbox_ext._runtime import LocalSnapshotStore

store = LocalSnapshotStore("/data/snapshots")
meta = await store.put("agent-1", "checkpoint", snapshot_data)
restored = await store.get(meta.snapshot_ref)
```

### 7.2 三级存储带淘汰

```python
from agentscope_sandbox_ext._runtime import (
    TieredSnapshotStore, StorageTier, EvictionPolicy,
    LocalSnapshotStore, MinioSnapshotStore, PostgresSnapshotStore,
)

tiered = TieredSnapshotStore([
    StorageTier("hot", LocalSnapshotStore("/data/hot"),
                priority=0, max_objects=100,
                eviction_policy=EvictionPolicy.LRU),
    StorageTier("warm", MinioSnapshotStore("localhost:19000", "ak", "sk"),
                priority=1, max_size_bytes=10_737_418_240,
                eviction_policy=EvictionPolicy.TTL, ttl_seconds=86400),
    StorageTier("cold", PostgresSnapshotStore("postgresql://..."),
                priority=2, eviction_policy=EvictionPolicy.NONE),
])

await tiered.start()  # 启动后台淘汰清理器

# 写入：并行写入所有三级
meta = await tiered.put("agent-1", "daily", data)

# 读取：检查 hot → warm → cold，未命中时晋升
data = await tiered.get(meta.snapshot_ref)

# 列出所有级别，去重
snapshots = await tiered.list("agent-1", tag="daily")

await tiered.close()
```

## 8. 运行测试和 benchmark

```bash
# 运行所有存储测试
python -m pytest tests/test_runtime_stores.py -v

# 运行所有 benchmark
python -m pytest tests/benchmark_stores.py -v -s

# 仅运行性能 benchmark（跳过正确性验证）
python -m pytest tests/benchmark_stores.py -v -s \
    -k "not TestBenchmarkCorrectness"

# 仅运行正确性验证
python -m pytest tests/benchmark_stores.py::TestBenchmarkCorrectness -v
```
