# NyxDB · Minimal MVP (Strongly‑Consistent KV)

NyxDB 当前聚焦一个可运行的最小 MVP：单 Raft Group 的强一致 KV 存储，支持线性一致读、快照/截断、成员管理与基础健康检查。PD 调度、多 Region 与分布式事务是下一阶段目标。

**开箱即用：默认关闭可选依赖（PD、Prometheus）→ 一条命令起服务，CLI 直接读写。**

---

**你可以做什么**
- 强一致写：写入通过 Raft 提案，多数派持久化后提交。
- 线性一致读：ReadIndex + WaitApplied 确保读取到最新提交点。
- 快照与截断：手动/自动快照，按阈值与时间间隔控制日志增长。
- 成员管理：Join/Leave/Members，成员落盘、重启恢复。
- 健康探针：gRPC Health；Prometheus 可按需开启。

---

**快速开始**
- 需求：Go 1.21+（仅当重新生成 gRPC 代码时需要 `protoc`）
- 启动服务：
  ```bash
  go run cmd/nyxdb-server/main.go -config configs/server.example.yaml
  ```
- 写入/读取（线性一致）：
  ```bash
  nyxdb-cli kv put --addr 127.0.0.1:10001 --key a --value b
  nyxdb-cli kv get --addr 127.0.0.1:10001 --key a
  ```
- 触发快照（可选）：
  ```bash
  nyxdb-cli admin snapshot --addr 127.0.0.1:10001 --force
  ```

> 默认配置中 `pd.address=""`、`observability.metricsAddress=""`，无需额外进程即可启动；当你需要 PD/指标时再填地址即可。

---

**最小配置（摘自 configs/server.example.yaml）**
- `cluster.autoSnapshot`: 启用自动快照
- `cluster.snapshotInterval / snapshotThreshold`: 快照触发的时间/entries 阈值
- `grpc.address`: gRPC 服务地址（CLI 连接所用）
- `pd.address`: 为空则禁用 PD；需要时填 `host:port`
- `observability.metricsAddress`: 为空则禁用 Prometheus；需要时填 `host:port`

---

**命令行（CLI）**
- KV：`kv put/get/delete`
- 只读事务：`kv begin/read/end`（引擎快照读）
- 管理：`admin members/join/leave/snapshot/merge`

遇到 `not leader` 时，服务端会返回 `leader=<addr>` 提示，CLI 会自动重试到 Leader（只对幂等/读请求生效）。

---

**实现要点（怎么工作的）**
- 写入：`KV/Put` → `internal/cluster/manager.go:Propose` → Raft → 提交后 `internal/layers/txn/replication.Applier` 写入引擎。
- 读取：`KV/Get` → `Node.ReadIndex`（`internal/layers/raft/node/node.go:ReadIndex`）→ `Node.WaitApplied` → 引擎 `DB.Get`。
- 快照：`Cluster.TriggerSnapshot`（`internal/cluster/manager.go:612`）打包引擎目录，持久到 `internal/layers/raft/storage`，并按 catch‑up 窗口截断日志；Follower 收到 Snapshot 自动恢复并 reopen 引擎。
- 传输：gRPC RaftTransport（`internal/layers/raft/transport/transport_grpc.go`）。
- 健康：`grpc_health_v1`（`internal/server/grpc/server.go`）。

---

**目录结构（简）**
- `cmd/nyxdb-server`：服务入口
- `cmd/nyxdb-cli`：命令行工具
- `internal/cluster`：读写入口、成员管理、快照/自动化
- `internal/layers/raft/node`：etcd/raft 封装（Ready/Apply/ReadIndex）
- `internal/layers/raft/storage`：自研 Raft 持久化
- `internal/layers/raft/transport`：传输抽象与 gRPC 实现
- `internal/layers/engine`：Bitcask+MVCC 引擎
- `internal/server/grpc`：KV/Admin gRPC
- `internal/layers/pd`：PD 原型（bbolt 持久化，默认不开）
- `pkg/api`：gRPC 生成代码

---

**可选组件（默认关闭）**
- PD（控制面原型）：
  - 启动：`go run cmd/nyxdb-pd/main.go -addr 0.0.0.0:18080 -data /tmp/nyxdb-pd`
  - 节点：在配置中填写 `pd.address` 后，节点会自动上报 Store/Region 心跳
  - API：`PD/ListStores`、`PD/GetRegionByKey`（便于观测与路由）
- Prometheus 指标：设置 `observability.metricsAddress: 127.0.0.1:2112` 后暴露 `/metrics`

---

**后续路线（分布式托底 + 能力演进）**
- 托底：
  - 联合共识（ConfChangeV2），Leader Transfer；
  - 快照分块/限速与应用前校验；
  - Leader‑Lease 快读、Propose/Apply 背压、慢副本限速。
- 调度（PD）：
  - Region/Store 视图与 epoch；Add/Remove/Transfer/Split/Merge 调度闭环；
  - 多 Region 路由（KV 根据 key 选择 Region Leader）。
- 事务：
  - 时间戳服务；2PC（Prewrite/Commit/ResolveLock），锁冲突处理与恢复；
  - 跨 Region 快照读与简单 DML。

---

**测试**
- 全量：`go test ./...`
- 仅集群：`go test ./internal/cluster -run TestCluster`
- Chaos/快照追赶：`internal/cluster/cluster_integration_test.go`

欢迎就 PD 调度、多 Region、分布式事务与观测一起讨论与共建。
