# 引擎总览（Engine Overview）

要点

- 存储模型：Bitcask 风格的追加写日志，配合 MVCC 元数据维护多版本。
- 一致性：引擎本身是单机存储，线性一致性由集群层（Raft）提供。
- 读路径：根据索引定位，再按快照时间戳沿版本链回溯。
- 写路径：编码日志条目（必要时追加 TxnFinished 标记）→ 追加到活跃文件 → 更新内存索引 → 进入组提交队列刷盘。

范围

- 本文提供整体脉络，并指向具体子模块文档：格式、数据文件、提交/MVCC、组提交、合并、批量写等。

关键模块

- 日志格式与编码：`internal/layers/engine/data/log_record.go`
- 数据文件与 I/O：`internal/layers/engine/data/data_file.go`、`internal/layers/engine/fio/*`
- 引擎核心（提交、读取、快照、索引加载）：`internal/layers/engine/engine.go`
- 批量写：`internal/layers/engine/batch.go`
- 合并/压缩：`internal/layers/engine/merge.go`
- 索引实现：`internal/layers/engine/index/*`

数据路径（概览）

- Put/Delete：构造 `LogEntry`（含 MVCC 元信息）→ 追加写 → 更新索引 → 入队 flush → 由组提交决定 fsync 时机。
- Get：索引查找 → 依据 `CommitTs / PrevFid / PrevOffset` 沿链回溯，判断对快照是否可见。

延伸阅读

- 日志格式：`./log-record.md`
- 数据文件：`./data-file.md`
- 引擎核心：`./db-engine.md`
- MVCC：`./mvcc.md`
- 组提交：`./group-commit.md`
- 合并/压缩：`./merge-compaction.md`
- 批量写：`./write-batch.md`
 - 缓冲池：`./buffer-pool.md`

TODO

- 补充 Put/Get 的时序图。
- 增加整体结构图（文件、索引、提交流程）。
