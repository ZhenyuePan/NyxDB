# 批量写（Write Batch）

目标

- 在引擎层将多次 put/delete 聚合成一次逻辑提交，提升吞吐并提供批量原子性。

源码

- `internal/layers/engine/batch.go`

选项与语义

- `WriteBatchOptions{ MaxBatchNum, SyncWrites }`
- 批内同键采用“后写覆盖”；对于同键多次写入保持用户顺序对应的序列号。

流程

- `WriteBatch.Put/Delete`：按顺序暂存记录。
- `Commit`：把暂存项转为 `logEntry`，分配提交时间，写入记录与 `TxnFinished`，更新索引，入队 flush。
- 成功后清空暂存缓冲。

关键辅助

- 带序列号的 key 编码：`logRecordKeyWithSeq`
- 恢复/加载时的解析：`parseLogRecordKey`

TODO

- 对比单次写与批量写在组提交开启下的时延与吞吐差异。
- 说明错误传播与推荐重试策略。
