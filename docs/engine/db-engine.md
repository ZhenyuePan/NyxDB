# 引擎核心（DB Engine）

目标

- 负责日志追加、内存索引更新、MVCC 可见性、快照与组提交的协调。

源码位置

- `internal/layers/engine/engine.go`

核心结构

- 活跃/旧文件：`activeFile`、`olderFiles`
- 可插拔索引：`index.Indexer`（B-Tree/SkipList/ART/Sharded-BTree）
- 提交时间：`seqNo`（最新发号）、`maxCommittedTs`（新读事务可见上界）

提交流程（单键）

1) 构造 `logEntry`
2) 解析该 key 的上一版本位置（批内缓存 → 全局索引）
3) 编码 `LogEntry`（写入 `CommitTs` 与 `Prev*`）
4) 追加到活跃文件（必要时轮转）
5) 追加该提交时间的 `TxnFinished` 标记
6) 更新内存索引与回收统计
7) 入队 flush（组提交决定 fsync 时机）

读取流程

- `BeginReadTxn` 捕获快照时间（`ReadTs = maxCommittedTs`）
- `Get`：索引定位 → 按 `ReadTs` 沿版本链回溯 → 命中/不存在

快照与索引加载

- 启动扫描数据文件重建索引；识别合并产物（`MergeFinished`）与事务记录。
- `loadIndexFromDataFiles` 应用已提交记录；读到 `TxnFinished` 时一次性应用该事务内的写入。

配置触点

- `Options`：DataFileSize、SyncWrites、BytesPerSync、IndexType、MMapAtStartup、GroupCommit、DataFileMergeRatio

TODO

- 补充提交流程时序图。
- 总结启动期错误处理与恢复路径。
