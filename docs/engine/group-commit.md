# 组提交（Group Commit）

目标

- 聚合多次提交，共享一次 fsync，降低延迟抖动、提升吞吐。

配置

- `Options.GroupCommit`：`enabled`、`maxBatchEntries`、`maxBatchBytes`、`maxWait`
- 交互项：`Options.SyncWrites`、`Options.BytesPerSync`

流程

- 写路径入队 `flushRequest{bytes, requireSync, done}`。
- 后台循环批量聚合，触发条件任一满足即执行：
  - `maxBatchEntries` 或 `maxBatchBytes` 或 `maxWait`。
- 触发后：对活跃文件执行一次 Sync；唤醒等待的写入方；重置计数。

正确性要点

- 当 `SyncWrites=true` 时，`requireSync` 必为真，调用方会等待 `done` 后返回。
- `BytesPerSync` 达阈值也会强制一次同步。

实现线索

- 入队：`enqueueFlush`
- 循环：`runFlushLoop`
- 同步：`syncActiveFileLocked`

TODO

- 小批次/大批次的时序示意。
- 针对写密集与低延迟场景的调优建议。
