# 性能调优（Perf Tuning）

本文提供引擎侧常见热点的定位方法与优化方向。

## 基准与剖析
- 单项基准：`go test -run=^$ -bench=Benchmark_Put -benchmem ./benchmark/engine -count=1 -cpuprofile=put.cpu.prof -memprofile=put.mem.prof`
- 交互式：`go tool pprof -alloc_space put.mem.prof` / `go tool pprof put.cpu.prof`
- 常用命令：`top`、`top -cum`、`list <func>`、`weblist <func>`、`-http :0`（Flame Graph）

## 常见热点（示例）
- `EncodeLogEntry` 大块分配：连续缓冲（header+key+value）带来明显的 `B/op` 与 `allocs/op`。
  - 方向A：分段写（header 小缓冲 + 递增 CRC + 分三次 Write），避免构造大切片。
  - 方向B：`sync.Pool` 复用编码缓冲（收益次之）。
- 缓冲池（Buffer Pool）：见 `./buffer-pool.md`，通过分桶池化热点大切片，减少内存抖动与 GC 压力。
- key/值的拷贝：`Put` 的防御性 `append`、`string(entry.key)`（map key）带来额外分配。
  - 方向：提供 `PutOwned/PutNoCopy`（调用方转移所有权）、或以稳定 hash 做临时映射键。
- 组提交参数：
  - 吞吐优先：增大 `maxBatchEntries/maxBatchBytes`、适当 `maxWait`；
  - 延迟优先：减小聚合阈值或关闭组提交（但 fsync 更频繁）。
- 索引选择：不同实现对写入/读取延迟影响明显（见 `indexing.md`）。

## 典型场景建议
- 写密集：开启组提交，适当放大批量；索引优先 Sharded B-Tree 或 ART；避免每次随机值分配。
- 读延迟敏感：ART + 合理 `maxWait`（甚至关闭组提交）；注意 value/编码拷贝。
- 快速回归：缩小 value/批量、关闭随机生成，先聚焦引擎内部热点。

## 注意事项
- pprof Graph 视图依赖 Graphviz（`dot`）；可用 Flame Graph/Top 代替。
- 生产环境谨慎调整 CRC、fsync 行为；可靠性优先。
