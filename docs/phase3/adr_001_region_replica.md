# ADR-001: Region Replica Lifecycle Skeleton

## Context

Phase 3 将在单节点内托管多个 Raft Group（Region）。当前 Cluster 仅管理单个 raft 节点/存储，需要一个统一入口为新增 region 配置存储目录、Raft 节点，便于后续挂接 PD 调度与分片读写。

## Decision

1. 定义 `RegionReplica`，绑定 Region 元数据、Raft Node、`RaftStorage`，Cluster 维护 `regionReplicas` 映射。
2. 在 Cluster 添加 `createRegionReplica`：
   - 基于 regionID 生成 `regions/<id>/raft/` 目录；
   - 创建 `RaftStorage`，构造 `raftnode.NodeConfig`，实例化并返回新的 `raftnode.Node`；
   - 注册 replica 到 `regionReplicas`，供路由/调度使用。
3. 默认 Region (ID=1) 同样使用该机制，替换旧的 `raft/` 根目录存储。
4. 暂时共用单一 commit/error 通道，仅托管主 Region；后续为多 Region fan-in 做准备。
5. 暂不实现 PD 调度或 Region 心跳，仅提供静态创建能力供后续调用。

## Status

Accepted — Scaffolding stage

## Consequences

- Cluster 增加 Region replica 映射管理，利于后续 Region 拆分与调度。
- 快照/恢复需兼容新的 `regions/` 目录结构。
- 为下一步实现多 Region Commit fan-in、PD 调度打基础。
- Cluster 停止时需要按 Region 关闭各自的 Raft Node（后续工作）。
