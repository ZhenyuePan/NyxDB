# Phase 3 性能对标计划（etcd）

目标：在云原生场景中，将 NyxDB 的集群写入/读取性能与 etcd 相当，并保持易用的扩展能力。

## 当前基准状况

| Benchmark | NyxDB（单节点 Raft） | 纯引擎（无 Raft） |
| --- | --- | --- |
| `Benchmark_Put` | ~6.8µs/次 | 同左 |
| `BenchmarkClusterPut` | ~4.6ms/次 | - |
| `Benchmark_Get` | ~0.29µs/次 | 同左 |
| `BenchmarkClusterGet` | ~2.1µs/次 | - |

- 与 etcd（默认 pipeline/100ms fsync）相比，单节点 Raft 写入慢一个量级。
- 读路径约 2µs，接近 etcd 的线性一致读。

## 性能对标路线

### 1. WAL / Sync 策略
- 支持批量写和 `async` 模式：让连续的 Raft 日志在内存堆积后统一 fsync（类似 etcd 的 `Commit` pipeline）。
- 增加可配置 `SyncWrites`（默认延迟 fsync）、`BatchSize`，提供 Redis 模式（risk on）与 etcd 模式（fsync on commit）。

### 2. Raft 复制优化
- 引入批量发送/接收，利用 etcd/raft 的 pipeline；减少每条日志的 encode/alloc。
- 单节点场景启用 `SkipWAL` 或 memory-only 写，验证上限性能。

### 3. 缓存与读路径优化
- 提供一致性等级选项：serializable（无需 ReadIndex） vs linearizable。
- 减少 `Get`/`Put` 中的内存分配；引入对象池、buffer 池。

### 4. 多节点真实基准
- 编写多节点集群 benchmark（Raft 复制、网络 RTT 模拟），与 etcd 的 `put/get` 进行对比。
- 记录不同配置（fsync=on/off、batch size）下的延迟/吞吐。

## 相比 etcd 的潜在优势
- **可扩展性**：作为自研内核，可深入定制 MVCC、事务、Region 调度；etcd 聚焦 KV/配置服务，扩展空间有限。
- **内聚 SQL/事务层**：Phase 3 之后会原生提供 SQL/2PC 打通，对复杂业务更友好；etcd 需外部组件整合。
- **灵活部署**：可根据需求选择 memory-only、磁盘混合、写策略，方便在同一产品内调节性能与可靠性。
- **开发可控性**：内部掌握全部代码与架构，容易针对特定云场景做优化与定制功能。

## 近期执行计划
1. 实现 WAL 异步/批量写配置，新增基准验证；与 etcd 做单节点对比。
2. 在 `benchmark` 中扩展多节点测试，加入网络延迟、复制写；提供对比报告。
3. 完善日志/指标，以行为反馈性能热点（如 fsync 次数、WAL 队列长度）。
4. 文档化性能基准流程，并定期运行 `go test ./benchmark/...` 生成结果日志。

随着这些优化落地，我们可以周期性用 etcd 的 `benchmarkctl` 等工具做对照测试，确保 NyxDB 在云原生场景中的性能指标稳定提升。EOF
