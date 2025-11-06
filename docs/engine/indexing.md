# 索引实现与选择指南（Indexing）

本文介绍引擎内置的内存索引实现、适用场景与选择建议，并给出测试与调优指引。

- 代码位置：`internal/layers/engine/index/*`
- 可选实现：B-Tree、SkipList、ART、Sharded B-Tree（默认）
- 配置项：`Options.IndexType`（见 `internal/layers/engine/options.go`）

## 实现概览
- B-Tree（`btree.go`）
  - 特点：结构紧凑、实现稳定；读写性能均衡。
  - 适用：通用场景；对并发写要求不极端的模块。
- SkipList（`skiplist.go`）
  - 特点：结构简单、易于范围遍历；写入 hop 多、分配较频繁。
  - 适用：偏读取、存在范围扫描需求但写吞吐不高的元数据/辅助索引。
- ART（`art.go`）
  - 特点：基于前缀压缩，查找路径短；读/写单次延迟低；内存占用相对较高。
  - 适用：追求极致读/写延迟、键空间适合前缀压缩的负载。
- Sharded B-Tree（`sharded.go`）
  - 特点：按范围分片到多棵 B-Tree，降低锁竞争；读路径略增路由开销。
  - 适用：读写混合、存在写热点但仍需保持范围有序的场景（默认）。

## 基准参考
参考 `test/index_structures_comparison.txt`（Intel i7-14700，Go 1.21）：

- 写入（Put）：ART ≈149 ns/op（最佳）；Sharded B-Tree ≈225 ns/op；B-Tree ≈249 ns/op；SkipList ≈372 ns/op（最慢）。
- 读取（Get）：ART ≈35 ns/op（最佳）；SkipList/B-Tree ≈42 ns/op；Sharded B-Tree ≈45 ns/op。

解读：
- 追求极致性能 → ART；
- 写热点/范围有序 → Sharded B-Tree；
- 实现/兼容优先 → B-Tree；
- 轻量可遍历 → SkipList（接受写入吞吐偏低）。

## 选择建议
- 默认：Sharded B-Tree（读写均衡、热点友好、维护成本低）。
- 内存充裕且延迟敏感：ART。
- 简洁稳定：B-Tree。
- 简单范围遍历且写少：SkipList。

## 配置与切换
- 通过 `Options.IndexType` 选择：`Btree`/`SkipList`/`AdaptiveRadix`/`ShardedBtree`。
- 例：
  - `opts := db.DefaultOptions; opts.IndexType = db.AdaptiveRadix`

## 范围遍历与迭代器
- 统一迭代器接口：`index/iterator.go`
- 前缀过滤/正反序：`IteratorOptions{ Prefix, Reverse }`

## 调优与注意事项
- 分片：Sharded B-Tree 的分片策略当前固定，后续可考虑开放配置。
- 分配与 GC：SkipList/ART 在某些键分布下小对象较多；结合 `pprof` 评估。
- 线程安全：实现内部处理并发访问；与引擎全局锁配合。

## 测试与评估
- 基准：`go test ./internal/layers/engine/index -bench BenchmarkIndex -benchmem -run ^$`
- 性能剖析：结合 `-cpuprofile/-memprofile`，关注 `allocs/op` 与 `B/op`。

