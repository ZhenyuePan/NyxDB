# NyxDB

NyxDB 是一个强一致性键值数据库，将 Raft 集群、带多版本能力的 Bitcask 引擎以及运维工具整合在一个 Go 项目中。它面向实验与原型场景：几秒内拉起单节点，在本地扩展到多 Region，再迭代事务、调度等高层能力而无需重新搭建底座。

[English](README.md) | 简体中文

---

## 为什么选择 NyxDB？
- **线性一致写读**：所有写入通过 etcd/raft 多数派持久化后才对外可读，读路径使用 ReadIndex + WaitApplied 保证线性一致。
- **支持快照的存储引擎**：内置 MVCC 引擎能快速获取时间点快照，手动或自动控制日志截断。
- **内建运维保障**：成员管理、健康探针、落盘元数据让集群重启和恢复不依赖外部协同。
- **一体化 CLI**：`nyxdb-cli` 覆盖 KV、管理、快照操作，对非 Leader 节点返回的地址自动重定向（幂等操作）。
- **可选控制面**：在需要时接入 PD 原型，体验 Store/Region 心跳、调度决策和观测能力。

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
- **集群管理 (`internal/cluster`)**：封装 Raft，负责提案、成员管理、快照编排。
- **Raft 层 (`internal/layers/raft`)**：etcd/raft 适配层，包含自研存储、传输抽象与 ReadIndex 辅助。
- **存储引擎 (`internal/layers/engine`)**：吸收 Bitcask 思路，追加写 + MVCC 元数据信息，支持 SkipList/B-Tree/ART 索引和可选的组提交。
- **事务实验 (`internal/layers/txn`)**：包括 Percolator 风格的 MVCC 原型与复制应用器。
- **控制面 (`internal/layers/pd`)**：基于 BoltDB 的轻量调度原型。
- **Server & CLI (`cmd/nyxdb-server`, `cmd/nyxdb-cli`)**：提供 gRPC 服务、健康探针和日常运维命令。

更多设计说明见 `docs/architecture.mdx`、`docs/development_status.md`。

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

