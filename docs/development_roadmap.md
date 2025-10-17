# NyxDB 开发路线（Phase 2）

本文档概述当前阶段（Phase 2：高可用集群）的已完成工作、现有能力以及后续迭代方向，供团队对齐研发节奏与优先级。

## 已完成（Phase 2 核心能力）

- **集群与一致性**
  - 单 Raft Group 强一致写入与线性一致读（ReadIndex + WaitApplied）。
  - 读事务 API（`BeginReadTxn/ReadTxnGet/EndReadTxn`）与 TTL 清理线程。
  - 成员管理：`admin join/leave/members`，成员信息持久化至 `cluster/members.json` 并在重启时恢复。
  - gRPC 传输（`internal/raft/transport_grpc.go`）与默认 stub 传输，为集成测试提供可选后端。

- **存储与快照**
  - Bitcask+MVCC 存储引擎，合并（merge）与备份能力。
  - 快照/截断：支持手动与自动触发，将引擎目录打包、写入 Raft snapshot 并按 Catch-Up 策略截断日志。
  - 自动快照策略：基于日志增长、时间间隔、目录大小（`snapshotDirSizeThreshold`），可配置最低间隔、最大 Applied Lag。
  - Follower 接收快照自动恢复目录并 reopen 引擎；新增 IT 覆盖“积压 → 快照 → 落后副本 catch-up”。
  - Snapshot 诊断：记录每次快照的耗时、payload 大小，暴露在 `/metrics` 与 `admin snapshot-status` 里。

- **运维与观测**
  - CLI：`kv put/get/delete/begin/read/end` & 自动解析 `NotLeader`，`admin snapshot/merge/join/leave/members/snapshot-status`。
  - gRPC Health 服务，便于 K8s/脚本探活。
  - Prometheus `/metrics`：输出 Raft term/commit/applied、成员数量、读事务数量、快照进度、快照耗时/大小等指标。
  - CLI 多节点配置脚本 `scripts/generate_cluster_configs.py`，加速本地 3 节点体验。
  - README 增补观测说明与快照调优项。

- **测试与 CI 基础**
  - `go test ./...` 全绿；集成测试涵盖 Chaos（网络抖动、断连、重启）、快照追赶等场景。
  - 存在 `TestClusterSnapshotCatchUp`、`TestClusterDiagnostics` 等针对快照和诊断的覆盖。

## 待完善 / 下一步（Phase 2 后续迭代）

- **快照策略 & 指标深化**
  - 将快照触发次数、耗时、大小暴露为 counter/histogram；增加合并/写入的指标。
  - 进一步完善自动快照 guard，支持磁盘剩余空间阈值、最大 snapshot 文件大小等可选约束。
  - CLI `admin snapshot` / `snapshot-status` 输出更详细上下文（例如最近 N 次快照历史）。

- **观测与日志**
  - 引入结构化日志（zap/logrus）并统一日志前缀；支持将诊断输出写到独立通道或接入集中日志系统。
  - 暴露 `pprof`、trace 等调试端点，完善 README 的调试指引。

- **自动化脚本与体验**
  - 在 scripts/Makefile 中增加“一键启动多节点 + 验证 + 收尾”脚本，方便本地 smoke test。
  - 编写 doc/指南说明如何在 docker/k8s 环境部署多节点。

- **SQL MVP 准备（并行进行）**
  - 在 `api/nyxdb.proto` 扩展 SQL Service RPC（`Query/Exec`），生成 stub。
  - 搭建 `internal/sql/` 目录结构（parser/planner/executor/catalog/rowcodec 等），先实现主键点查/范围扫描骨架。
  - MVC：定义最小 Row/Schema 表示，与现有 KV 绑定策略。

- **Phase 3 展望（提前调研）**
  - Region/Shard 管理、PD 角色（路由 + 调度）。
  - 多 Raft Group、分布式事务（MVCC+2PC）。
  - 更丰富的运维能力：自动选主、滚动升级、集群重平衡。

## 使用指引（简要）

1. 生成示例配置：`./scripts/generate_cluster_configs.py --nodes 3`
2. 启动节点：
   ```bash
   go run cmd/nyxdb-server/main.go -config configs/node1.yaml
   go run cmd/nyxdb-server/main.go -config configs/node2.yaml
   go run cmd/nyxdb-server/main.go -config configs/node3.yaml
   ```
3. CLI 验证：`nyxdb-cli admin members --addr 127.0.0.1:10001`、`nyxdb-cli kv put/get`。
4. 观测：`grpc.health.v1.Health/Check`、`curl http://127.0.0.1:2112/metrics`、`nyxdb-cli admin snapshot-status`。

欢迎在 issue/PR 中补充需求与实现建议，Phase 2 会持续聚焦核心稳定性和观测完善，同时为 Phase 3（Region/PD、事务）奠定基础。
