# NyxDB 开发状态与后续路线（继任 AI 指南）

本文档帮助你快速了解当前代码已完成功能、待办清单（按 MVP 优先级），以及验证方法与注意事项，便于你接力迭代。

## 总览

- 架构要点：
  - Region = 键空间分片；每个 Region 一一对应一个 Raft group（单机/多机多 group 已可运行）。
  - PD（Placement Driver）：持久化 Region/Peer 元数据，支持按 Store 查询 Region；新增 TSO（分配全局单调递增时间戳）。
- 引擎层（Bitcask + MVCC）：支持多版本读取（按 ReadTS），暴露 `ApplyReplicated(commitTs, ops)` 的复制提交入口；写入路径统一走受控组提交（按时间/条数/字节聚合一次 fsync，兼容 `SyncWrites` 与 `BytesPerSync`）。
  - 事务（Percolator 最小实现）：TSO + 单 Region 2PC 骨架（预写锁/写冲突检测/读遇锁），尚未与多副本复制/跨 Region 串联。
  - KV/ADMIN gRPC：KV 路由按 key → Region；Admin 提供成员管理、快照、以及 Region 副本创建/删除。

## 已完成（关键里程碑）

1) PD 元数据与查询
- 新增按 Store 过滤的 Region 查询：`GetRegionsByStore`（`api/nyxdb.proto` + server/client 实现）。
- Region/Peers 持久化与重启恢复：`internal/layers/pd/region_store.go`（Bolt `pd.regions`）。
- PD 服务重建 Region 视图：使用 Region 表为真源，心跳仅补充 `AppliedIndex`。

2) PD ↔ Cluster 同步
- `AttachPD` 启动后同步：Cluster 从 PD 拉取 `RegionsByStore`，以 PD 为准对齐本地 `regionMgr` + `regions.json`。
- 创建/删除 Region 后触发心跳与 Best-effort Register/Update 到 PD；`EnsureRegionReplica` 会在本地创建副本并尝试更新 PD。

3) KV 路径多 group 路由
- 写入/线性一致读按 key → Region → 对应 `Replica.Node` 执行 `Propose/ReadIndex/WaitApplied`。
- 非 leader 返回该 Region 的 leader 地址（`ErrNotLeader: leader=<addr>`）。

4) Admin 管理与副本广播
- Admin gRPC 新增：`CreateRegionReplica` / `DeleteRegionReplica`（本地创建/删除 Region 副本）。
- `CreateStaticRegion` 成功后，异步广播到其他节点创建本地副本（最佳努力，依赖 `members` 中的地址拨号）。

5) TSO 与事务层（Percolator 最小原型）
- PD TSO：`AllocateTimestamp` RPC，Bolt `meta` bucket 持久化，重启继续单调分配。
- 事务管理：`internal/layers/txn/percolator` 中实现 `Manager/Transaction`+简化 2PC（单 Region）：
  - Prewrite：检查写冲突（基于 `LatestCommitTs`），在进程内存锁表加锁；读遇锁返回 `LockError`。
  - Commit：TSO 分配 CommitTS → `EngineStore.Apply(commitTs, ops)` 落盘 → 释放锁。
- 引擎层扩展：`LatestCommitTs`、`GetVersion`（按 ReadTS 读版本）对接 MVCC。

6) 集成测试（关键）
- 单机多 group：`TestClusterSingleNodeMultipleRegions`。
- 多机多 group：`TestClusterMultiNodeMultipleRegions`（chaos 网络，3 节点，Region leader 选举 & 读写验证）。
- 其他：线性一致读、成员持久化、快照触发、混沌稳定性等均有覆盖。

## 待办清单（按 MVP 优先级）

P0（强烈建议优先完成）
- 事务对外 API：
  - 在 KV/gRPC 增加最小事务接口（Begin/TxnPut/TxnGet/Commit/Rollback），或封装在现有 KV 中携带 Txn 上下文。
  - 统一错误语义：LockError（含 key/primary/startTS）、WriteConflict（建议明确 Status code 便于客户端策略）。
- 事务并发与清理：
  - 锁 TTL 与定时清理（当前为内存锁，需避免长时间阻塞/泄漏）。
  - 读遇锁策略：返回可重试错误，简单 backoff 建议值。
- 客户端路由（最小实现）：
  - Region Cache：`GetRegionByKey` 初次定位；遇 not leader 时根据返回 leader 刷新并重试。
  - CLI 示例：基于 PD 路由的 `kv put/get`。
- PD/Region 同步收敛：
  - 增强启动同步的冲突日志（范围冲突、Epoch 倒退、Peers 不一致），在必要场景（例如 Range 重叠）阻止启动或只读。

P1（增强与可用性）
- 错误返回完善：
  - Wrong Epoch/Wrong Region：KV 请求需携带 (regionID, epoch) 后服务端校验返回。
- 调度占位（最小）：
  - 在 PD 中根据简单规则生成建议（如缺 leader 时 TransferLeader），Cluster 仅记录/打印，为后续闭环做准备。
- 观测：
  - 事务指标（TSO 请求数、锁数量、写冲突次数），Prometheus 暴露；PD/Cluster 关键事件结构化日志。
- 广播可靠性：
  - `members` 地址用于 gRPC 拨号的前提说明（MVP 要求 `nodeAddress == grpc.address`），必要时引入独立 gRPC 地址字段或由 PD 提供。

P2（后续扩展）
- 跨 Region 2PC：
  - 协调器分组（按 Region 批次 Prewrite/Commit）、并行化、错误回滚与补偿；
  - 与 Raft 复制闭环：2PC 操作通过 Raft 日志复制到所有 Peers（当前是引擎直接落盘）。
- Region Split/Merge：
  - 手动 Split 的最小实现（按 key 范围拆分 + 新建 Raft group），必要的 Epoch/Peers 变更流程。
- PD 调度闭环：
  - TransferLeader/AddPeer/RemovePeer 执行与回报，调度策略逐步完善。

## 验证方法

1) 单机多 group（集成测试）
- 运行：
  - `go test ./internal/cluster -run TestClusterSingleNodeMultipleRegions -v`
- 逻辑：创建 `Region ["a","m")` 后对 `"b"`、`"z"` 写入，并在线性一致读中验证各自路由到对应 group。

2) 多机多 group（集成测试）
- 运行：
  - `go test ./internal/cluster -run TestClusterMultiNodeMultipleRegions -v`
- 逻辑：3 节点 chaos 网络，Leader 创建新 Region 后在其他节点 `EnsureRegionReplica`，等待该 Region leader 选举并验证写读与 not leader 错误。

3) PD/TSO 验证
- PD TSO 单测：`internal/layers/pd/service_test.go` 覆盖单调递增与重启恢复；运行 `go test ./internal/layers/pd -run Allocate -v`。
- PD 同步验证：`TestClusterSyncRegionsFromPD`，拉取 PD 元数据并对齐本地。

4) gRPC 手工验证（可选）
- 启动 nyxdb-server 后，用 `grpcurl` 调用：
  - 创建 Region 副本：`nyxdb.api.Admin/CreateRegionReplica`（`RegionDescriptor{region_id,start_key,end_key,...}`）。
  - 读写：`nyxdb.api.KV/Put`、`KV/Get`。
  - 查看 Region 列表（如 PD 已接入）：`nyxdb.api.PD/ListRegions` / `GetRegionsByStore`。

## 注意事项与已知限制

- KV 请求暂未携带 Region Epoch，服务端无法返回 Wrong Epoch，当前走 key→Region 路由；客户端需根据 not leader 错误做重试。
- 广播建副本默认用 `members` 中的地址拨号，MVP 需保证 `cluster.nodeAddress == grpc.address`；否则可在每个节点手动调用 `CreateRegionReplica`。
- `RegionsByStore` 的返回在代码中已简化为切片（无 error），gRPC 层仍是标准 RPC 错误语义。
- 事务锁表为本进程内存数据结构，缺少 TTL/持久化/分布式协商；P0 里需要加清理与最小回滚策略。
- engine的index有个读锁可能是冗余
- 我不确定现有的MVCC是否为最佳方案

## 代码入口与关键文件

- PD 服务：
  - `internal/layers/pd/service.go`（Region/Peers 管理、TSO 分配）
  - `internal/layers/pd/region_store.go`（Bolt 持久化）
  - `internal/layers/pd/grpc/server.go|client.go`（gRPC 适配）
- Cluster 管理：
  - `internal/cluster/manager.go`（KV 路由、线性一致读；Admin 快照/成员等）
  - `internal/cluster/region_manager.go`（CreateStaticRegion、EnsureRegionReplica、广播）
  - `internal/cluster/pd_integration.go`（AttachPD、同步/心跳）
- 事务层：
  - `internal/layers/txn/percolator`（Manager/Transaction、锁与写冲突、EngineStore 适配）
  - `internal/layers/engine/engine.go`（`ApplyReplicated`、`LatestCommitTs`、`GetVersion`）
- gRPC 服务：
  - `internal/server/grpc/kv_admin.go`（KV/ADMIN 服务端，含 Region 副本创建/删除 RPC）

## 开发/测试指引

- 全量测试：`go test ./...`
- 定向测试：
  - 单机多 group：`go test ./internal/cluster -run TestClusterSingleNodeMultipleRegions -v`
  - 多机多 group：`go test ./internal/cluster -run TestClusterMultiNodeMultipleRegions -v`
  - PD/TSO：`go test ./internal/layers/pd -v`

若需要，我可以继续为事务对外 API、锁 TTL/清理、客户端 Region Cache、Wrong Epoch 校验等 P0 任务出具体 Patch 草案与测试计划。
