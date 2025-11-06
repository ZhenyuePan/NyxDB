# 日志记录格式（Log Record）

概念

- 逻辑负载：`LogRecord { Key, Value, Type }`
- 物理元信息：`LogMeta { CommitTs, PrevFid, PrevOffset }`
- 编解码载体：`LogEntry { Record, Meta }`

源码位置

- `internal/layers/engine/data/log_record.go`

头部布局（Header）

- 字段顺序：CRC(4LE)、Type(1B，最高位作为 meta 标志)、KeySize(varint)、ValueSize(varint)、[CommitTs(uvarint)、PrevFid(uvarint)、PrevOffset(varint) 当 meta=1 时存在]
- 当 Type 的 meta 标志置位时，后续跟随事务元数据（提交时间与前序位置）。

Key + Seq 前缀

- 为表达提交顺序，写盘时会在 key 前加变长序列号（seq）。
- `logRecordKeyWithSeq` 负责 `seq||key` 编码，`parseLogRecordKey` 负责解析；定义在 `internal/layers/engine/batch.go`。

编码

- `EncodeLogEntry(entry)` 生成连续缓冲区（header + key + value），然后对 CRC 字段之后的内容计算校验并写回。
- `getLogEntryCRC` 支持分段计算 CRC（header 与 KV 分开发生时）。

缓冲池与零拷贝（可选）

- 若启用缓冲池，可用 `sync.Pool` 复用编码缓冲，或采用“分段写 + 递增 CRC”避免构造大缓冲；详见 `./buffer-pool.md`。

解码

- `decodeLogRecordHeader` 解析头部，返回尺寸与元数据。
- `DataFile.ReadLogRecord` 可读取 payload 并校验 CRC。
- `DataFile.ReadLogEntryMeta` 仅读取 header/meta，便于低开销遍历版本链。

兼容性与演进

- 通过 Type 的 meta 标志控制新增元字段；长度均使用 varint 以保持扩展弹性。
- 变更磁盘布局需保证前后向可读，便于恢复与合并。

TODO

- 补充常见操作（普通/删除/事务完成）的头部示例表。
- 记录 key/value 的实践上限与约束。
