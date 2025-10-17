# Phase 3 架构设计与基线拆分

本文件用于梳理 NyxDB 向 Phase 3（Multi-Raft、Region/PD、分布式事务）演进的整体架构与里程碑，为后续实现提供统一参照。

## 目标概览

1. **多 Region / Multi-Raft**：将全局 Key 空间划分为多个 Region（Key Range），每个 Region 维护独立的 Raft Group，实现横向扩展与容灾。
2. **PD（Placement Driver）角色**：集中管理集群拓扑、Region 元数据、调度策略，并负责 Region 调度、Split/Merge、成员变更协调。
3. **分布式事务**：在现有 MVCC 基础上引入跨 Region 的两阶段提交（2PC），提供 ACID 事务语义。
4. **运维与观测增强**：保障多 Region 下的监控、告警、故障恢复；提供自动化脚本、诊断工具和指标。

## 现状依赖

- **现有组件**：单 Raft Group、Snapshot/Compaction、KV/Admin gRPC 服务、Prometheus 指标、健康检查。
- **缺失能力**：Region 元数据、PD 原子性操作、Raft 多实例调度、事务协调器、Lease/Lock 语义。

## 架构演进总览

```
+-------------------+       +-------------------+      +-------------------+
|   SQL / API 层     | ----> |  事务协调器 (2PC)  | ---> |   Region / Raft   |
+-------------------+       +-------------------+      +-------------------+
                                                ^
                                                |
                                       +-------------------+
                                       |     PD Service    |
                                       +-------------------+
```

- SQL / KV API 将调用事务协调器；协调器根据 key 映射至 Region，驱动两阶段提交。
- 每个 Region 对应一组 Raft 节点（Store 上多实例）；PD 维护 Region 的分布、Leader 信息等。
- 心跳/调度：Store 周期性向 PD 汇报 region state，PD 下发调度指令。

## 里程碑拆分

### 1. Region 元数据与 Store 托管
- 定义 Region 结构（ID、KeyRange、Peers、Epoch、状态）。
- Store（原 cluster.Node 拓展）支持同时托管多个 Region：
  - 每个 Region 拥有独立的 Raft `Node`、Log、Snapshot 目录。
  - 引擎层增加 Region → ColumnFamily/Prefix 映射或独立目录。
- 静态配置 + 手动分片：初期可在配置中预先划分几个 Region，验证多 Raft 同时运行。

### 2. PD 原型
- PD 节点维护集群元数据：Store 列表、Region 分布、Lease 信息。
- 定义 PD API（gRPC 或内存接口）：
  - `StoreHeartbeat(req StoreHeartbeatRequest)`
  - `RegionHeartbeat(req RegionHeartbeatRequest)`
  - `GetRegionByKey(key)` 等。
- Store 上报 Region 状态，PD 决定/返回调度指令（增加/移除副本、转移 leader、触发 split/merge）。
- 元数据持久化：阶段一可先用 BoltDB/自研 JSON snapshot，实现简单快照 + 重启恢复。
- 当前进展：`internal/pd` 提供内存/持久化 Service 与 gRPC MVP（`cmd/nyxdb-pd`），Cluster 可通过 `AttachPDClient` 周期上报 Region 列表和状态，为后续调度指令打基础。

### 3. Region 管理生命周期
- **Split**：当 Region Size/Key 数达阈值，Store 发起 split，PD 更新元信息，调度新 Region。
- **Merge**：合并小 Region（可后推）。
- **Replica 调整**：扩容/缩容 Store 时，PD 下发 add/remove peer 指令，驱动 Raft ConfChange。

### 4. 分布式事务（MVCC + 2PC）
- 扩展 MVCC：支持跨 Region 的 TS 分配、锁信息（Primary/Secondary）记录。
- 协调器：实现两阶段提交流程（Prewrite、Commit），参考 Percolator/TiDB：
  1. Prewrite：写锁 + 数据（Primary/Secondary）。
  2. Commit：先提交 Primary，再提交 Secondary。
  3. 提供 Rollback/ResolveLock 机制，处理超时/失败。
- 事务 API：先从 KV 层提供 `TxnBegin/TxnCommit/TxnRollback` 接口，逐步接入 SQL 层。

### 5. 观测与调试
- 指标：Region 数量、Leader 分布、调度次数、事务成功率/延迟、锁等待、Split 触发次数。
- CLI：增强 `nyxdb-cli` 支持 Region/PD 查询（列出 Region、Store 状态、调度历史）。
- 日志：结构化输出 Region/PD/Txn 关键事件，便于排障。

## 技术难点与风险
- **多 Region 存储隔离**：如何避免不同 Region 互相污染（目录结构、WAL、快照），保证恢复时可独立重建。
- **Raft 性能与调度**：多实例 Raft 并发带来的资源竞争，需要在 Apply 阶段做好限流与监控。
- **PD 可用性**：需考虑 PD 单点风险，可能需要引入 PD 自身的多副本/选举机制（可先从单节点 PD 入手）。
- **事务冲突处理**：锁 TTL、重试策略、死锁检测需要设计好，否则容易造成长尾。

## 第一阶段建议执行顺序
1. 编写 Region/Store 数据结构及静态多 Region Demo：
   - 手工配置两个 Region（0-9, 10-~），在单 Store 上运行两个 Raft 节点实例；
   - 在 CLI 上提供 `put/get` 时根据 key 选择 Region。
2. 定义并实现 PD API（最小化：`GetRegion`, `StoreHeartbeat`, `RegionHeartbeat`）与内存元信息管理；
   - Store 周期性向 PD 汇报一组 Region 的状态；
   - PD 把当前 Region Leader 信息回传给 Store。
3. 完成基础指标与日志输出，便于观察多 Region 运作。
4. 迭代实现 Split/Merge、调度策略。
5. 引入事务协调器与 MVCC 扩展。

## 输出要求
- 随 Phase 3 实现推进，更新本文件或拆分为详细的 ADR 文档（如 `docs/phase3/adr_001_region.md`）。
- 在代码层保持模块隔离：`internal/pd`、`internal/region`、`internal/txn`、`pkg/pdapi` 等。
- 与 Phase 2 后续工作保持并行，确保核心稳定性不会受 Phase 3 大改动影响。

---

此文档作为 Phase 3 的起点，请在实际开发前逐条细化任务与时序，并与团队达成一致后执行。
