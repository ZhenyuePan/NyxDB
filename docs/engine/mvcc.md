# MVCC（多版本并发控制）

目标

- 通过为每个 key 维护提交时间与前向回链，提供快照级读取能力。

基本概念

- 提交时间（CommitTs）：单调递增的逻辑时间（`DB.seqNo`/`maxCommittedTs`）。
- 版本链：每个版本记录 `PrevFid/PrevOffset` 指向前一版本（同文件时 `PrevFid` 可省）。

可见性

- 读事务开始时捕获 `readTs = maxCommittedTs`。
- 针对某 key，自索引位置起沿链回溯：
  - 若 `CommitTs == 0 或 CommitTs <= readTs`：版本可见；若为删除标记则返回未命中；否则返回值。
  - 否则继续按 `Prev*` 回溯；遇负偏移或保护阈值终止。

相关 API

- `DB.BeginReadTxn()` / `ReadTxn.ReadTs()` / `ReadTxn.Get()` / `ReadTxn.Close()`
- `DB.GetVersion(key, readTs)`、`DB.LatestCommitTs(key)`

实现线索

- 版本链遍历：`internal/layers/engine/engine.go`（`getValueByPositionWithReadTs` 等）
- 读事务追踪与安定点：`beginReadTxn`、`endReadTxn`、`safePoint`

TODO

- 给出两个并发写+一个读的示例，验证快照隔离。
- 列举链过深保护与异常分支，并标注代码引用。
