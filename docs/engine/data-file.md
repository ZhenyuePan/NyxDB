# 数据文件与 I/O（Data File & I/O）

文件与轮转

- 数据文件命名为 `<9位id>.data`，位于引擎数据目录下。
- 活跃文件（active）用于追加写；旧文件只读。
- 当 `WriteOff + size > Options.DataFileSize` 时触发轮转。

源码位置

- `internal/layers/engine/data/data_file.go`
- I/O 抽象：`internal/layers/engine/fio/*`

I/O 模式

- 启动阶段：可用 MMap 读取加速索引加载（`Options.MMapAtStartup`）。
- 运行阶段：写入采用标准文件 I/O，按策略 fsync。

读取

- `ReadLogRecord(offset)` 读取头部与负载，校验 CRC，返回 `LogEntry` 与大小。
- `ReadLogEntryMeta(offset)` 仅读取 header/meta，便于低开销遍历版本链。

写入与同步

- `Write([]byte)` 追加并推进 `WriteOff`。
- 同步策略：
  - `Options.SyncWrites`：每次提交都强制同步；
  - `Options.BytesPerSync`：按累计字节周期性同步；
  - 组提交（见相关文档）聚合多次 fsync 以摊薄开销。

异常处理

- CRC 不一致返回 `ErrInvalidCRC`。
- 解析索引时遇到 Header/Payload EOF 视为文件结束。

TODO

- 增加 active/older 轮转与进展示意图。
- 记录 mmap 读取的限制与平台差异。
