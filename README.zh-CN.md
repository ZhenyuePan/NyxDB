# NyxDB

NyxDB 是一套可嵌入、可裁剪的强一致键值数据库。它以 etcd/raft 提供线性一致复制，以 Bitcask 风格的追加写存储 + MVCC 提供快照友好与可恢复能力；通过单端口 gRPC 暴露 KV/Admin/Raft 入口，默认无外部强依赖，开箱即可在本地或三节点集群运行。针对不同负载，NyxDB 支持索引可插拔、组提交/BytesPerSync/SyncWrites 等耐久与延迟调参，并可按需接入 PD 控制面实现多 Region 管理与调度。适合嵌入微服务、边缘/私有小集群、平台元数据与任务调度状态等对一致性与可恢复性有要求的场景。

[English](README.md) | 简体中文

```
N   N   y   y   x   x   DDDD    BBBB 
NN  N    y y     x x    D   D   B   B
N N N     y       x     D   D   BBBB 
N  NN     y      x x    D   D   B   B
N   N     y     x   x   DDDD    BBBB 
```

---

## 为什么选择 NyxDB？
- **线性一致写读**：所有写入通过 etcd/raft 多数派持久化后才对外可读，读路径使用 ReadIndex + WaitApplied 保证线性一致。
- **支持快照的存储引擎**：内置 MVCC 引擎能快速获取时间点快照，手动或自动控制日志截断。
- **内建运维保障**：成员管理、健康探针、落盘元数据让集群重启和恢复不依赖外部协同。
- **一体化 CLI**：`nyxdb-cli` 覆盖 KV、管理、快照操作，对非 Leader 节点返回的地址自动重定向（幂等操作）。
- **可选控制面**：在需要时接入 PD 原型，体验 Store/Region 心跳、调度决策和观测能力。

## 与 etcd 的关系
- 相同点：
  - 都基于 etcd/raft 的多数派复制，提供线性一致的读写；
  - 都采用 MVCC 语义支持快照读与时间点可见性；
  - 都支持快照/截断与成员管理。
- 不同点（NyxDB 的侧重）：
  - 存储布局：Bitcask 风格（追加写）+ per‑key 版本链（`CommitTs + Prev*`），I/O 模式更简单、可预测；
  - 写入控制：原生支持组提交（`MaxBatchEntries/MaxBatchBytes/MaxWait`）与 `BytesPerSync/SyncWrites`，便于按延迟/耐久 SLO 调参；
  - 索引可插拔：B‑Tree/SkipList/ART/Sharded‑BTree 可按场景选择；
  - 易嵌入：单端口 gRPC（KV/Admin/RaftTransport）+ 轻依赖，适合内嵌微服务或小型/边缘三节点集群；
  - 控制面可选：PD 为可选组件，不把系统强绑定为“大而全”的控制面。
- 何时优先 NyxDB：
  - 需要可嵌入、易裁剪的强一致 KV 基座；
  - 希望更细粒度掌控写入耐久/延迟（组提交、同步策略、索引选择）；
  - 面向小规模集群/边缘/本地优先的场景。
- 何时优先 etcd：
  - 需要经过长年生产验证的通用控制面 KV、成熟生态与 Watch/租约等完整特性时。

---

## 功能概览
| 能力 | 描述 |
| --- | --- |
| 一致性 | Raft 复制；ReadIndex + WaitApplied 实现线性一致读取 |
| 存储 | 类 Bitcask 追加写，附带 MVCC 元数据和 per-key 版本链 |
| 快照 | CLI 手动触发；按时间与日志阈值自动快照和截断 |
| 成员管理 | Join/Leave/Members，成员信息持久化，重启自动选主 |
| 传输 | 基于 gRPC 的 Raft 流式传输，集成健康检查 |
| 可观测性 | 可选 Prometheus `/metrics`，诊断日志可开关 |
| 工具链 | CLI 一站式操作，示例配置覆盖单机/多节点实验 |

---

## 快速开始

### 环境准备
- Go 1.21 及以上。
- 仅在重新生成 gRPC 代码时需要 `protoc`（参考 `scripts/genproto.sh`）。

### 启动单节点
```bash
go run cmd/nyxdb-server/main.go -config configs/server.single.yaml
```

### 与集群交互
```bash
# 线性一致写/读
go run cmd/nyxdb-cli/main.go kv put --addr 127.0.0.1:10001 --key greeting --value hello
go run cmd/nyxdb-cli/main.go kv get --addr 127.0.0.1:10001 --key greeting

# 可选：触发快照
go run cmd/nyxdb-cli/main.go admin snapshot --addr 127.0.0.1:10001 --force
```

要模拟多副本，可复制配置文件，为每个节点指定独立的数据目录与 `grpc.address`，再分别运行 `go run` 或 `go build` 后的可执行文件。

### 可选组件
- **PD 原型（控制面）**
  ```bash
  go run cmd/nyxdb-pd/main.go -addr 0.0.0.0:18080 -data /tmp/nyxdb-pd
  ```
  在服务器配置中填写 `pd.address`，即可启用 Store/Region 心跳与调度实验。
- **Prometheus 指标**：将 `observability.metricsAddress` 设置为 `127.0.0.1:2112` 等地址，即可在 `/metrics` 暴露指标。

---

## 架构速览
- **集群管理 (`internal/cluster`)**：封装 etcd/raft 节点生命周期（Start/Ready/Apply）、提案流与线性一致读（ReadIndex + WaitApplied），负责成员与 Region 管理、快照编排，是 KV/Admin 的协调入口。
- **Raft 层 (`internal/layers/raft`)**：
  - `node`：对接 `raft.Node`，桥接存储与传输；
  - `storage`：实现 `raft.Storage` 持久化与快照；
  - `transport`：提供 gRPC 与本地 Noop 传输。gRPC 传输基于 `pkg/api.RaftTransport`，由同一 gRPC 端口注册。
- **存储引擎 (`internal/layers/engine`)**：Bitcask 风格（追加写，非 LSM）。利用 `CommitTs + PrevFid/PrevOffset` 串联 per‑key 版本链实现 MVCC；索引支持 B‑Tree/SkipList/ART/Sharded‑BTree（默认）。提供组提交（MaxBatchEntries/MaxBatchBytes/MaxWait）、`BytesPerSync`、`SyncWrites`；启动可用 MMap，写入使用标准文件 IO。
- **事务与复制 (`internal/layers/txn`)**：`replication` 将 Raft 提交安全应用至引擎；`percolator` 为本地 MVCC/事务实验（非分布式层）。
- **控制面 (`internal/layers/pd`)**：内存型 PD 原型，可选使用 BoltDB 持久化 Region 元数据；包含 gRPC 客户端/服务端。
- **Server & CLI (`cmd/nyxdb-server`, `cmd/nyxdb-cli`)**：单个 gRPC 端口承载 KV/Admin 与 RaftTransport，内建健康检查；可选观测位于 `internal/layers/observability/*`。

### ASCII 架构图

```
            +-------------------+                +----------------------+
            |  客户端 / CLI     |  gRPC (KV/Admin)|     PD（可选）      |
            |  (nyxdb-cli)      |<---------------->|  Store/Region 元数据 |
            +---------^---------+                +----------^-----------+
                      |                                   |
                      | gRPC（单端口）                     |
              +-------+--------+                          |
              |   gRPC 服务端  |  注册                     |
              | (health+KV+PD) |<-------------------------+
              +-------+--------+
                      |
                      v
              +-------+--------+     Raft 消息     +--------------------+
              |  集群管理      |<----------------->|  Raft 传输         |
              | (ReadIndex,    |                    |  (gRPC/Noop)      |
              |  快照, 成员)   |                    +--------------------+
              +-------+--------+
                      |
                      v
              +-------+--------+
              |    Raft 节点   |
              +-------+--------+
                      |
                      v
              +-------+--------+        +-----------------+
              |   引擎         |<------>|  内存索引       |
              | (Bitcask+MVCC) |        | (BTree/ART/...) |
              +-------+--------+        +-----------------+
                      |
                      v
              +-----------------+   文件   +-----------------+
              |  数据文件       |<-------->|   Hint / 合并   |
              +-----------------+         +-----------------+
```

更多设计说明见 `docs/architecture.mdx`、`docs/development_status.md`，以及 `docs/engine/` 下的引擎专题文档。

---

## 基准数据
以下为本地环境（Intel i7-14700，Go 1.21，Ubuntu）得到的基准数据，完整结果见 `test_result/`。

| 场景 | 吞吐 / 延迟 |
| --- | --- |
| 引擎 PUT（1 KB value，未 fsync） | ~183K ops/s · 6.8 µs/op [[详情]](test_result/benchmark.txt) |
| 引擎 GET（热点 key） | ~4.1M ops/s · 287 ns/op [[详情]](test_result/benchmark.txt) |
| 集群 PUT（单 Region） | ~330 ops/s · 3.6 ms/op [[详情]](test_result/benchmark_cluster.txt) |
| 集群 GET（预填充） | ~680K ops/s · 1.7 µs/op [[详情]](test_result/benchmark_cluster.txt) |

> 数据主要用于展示基线表现。开启 fsync、跨网络部署、启用 PD 等因素都会改变结果，可使用 `benchmark/` 目录下的 `go test -bench` 脚本复现与扩展测试。

---

## 路线图
- **短期**
  - 强化 Raft 生命周期：分块快照、主从切换、背压控制。
  - 丰富可观测性：CLI 诊断、指标面板、Tracing 接入。
  - 优化写入路径：减少编码分配，引入零拷贝选项。
- **中期**
  - PD 驱动的多 Region 路由。
  - 自动调度：Store/Region 在线均衡、分裂/合并流程。
  - Read Lease、Follower Read 支持低延迟读。
- **长期**
  - 分布式事务（Percolator 风格）、锁冲突处理与恢复工具。
  - 在 KV 之上探索 SQL 或文档型接口。

路线详情与讨论见 `docs/development_status.md`，欢迎贡献想法与代码。

---

## 仓库结构
- `cmd/nyxdb-server`：Server 端主二进制。
- `cmd/nyxdb-cli`：KV / 管理命令行工具。
- `internal/`：存储引擎、Raft 适配、集群管理、PD 原型。
- `benchmark/`：引擎与集群基准测试。
- `configs/`：单机与多节点配置示例。
- `docs/`：架构笔记、项目进展、专题文章。
- `examples/`：MVCC 快照、客户端示例程序。
- `scripts/`：工具脚本（protobuf 生成、bench 辅助等）。

---

## 参与贡献
NyxDB 仍在快速演进阶段，期待反馈 Bug、分享基准结果或讨论设计方案。欢迎通过 Issue、PR 与我们交流，也欢迎共建控制面、存储引擎或事务层的更多特性。
