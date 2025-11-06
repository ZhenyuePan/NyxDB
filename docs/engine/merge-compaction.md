# 合并 / 压缩（Merge / Compaction）

目标

- 仅保留每个 key 的最新可见版本，丢弃过期/删除记录，从而回收磁盘空间。

触发与度量

- `DB.reclaimSize` 记录可回收字节数。
- 可手动触发，或当 `Options.DataFileMergeRatio` 指示垃圾比例足够时触发。

流程（概览）

- 重写一组新文件，仅包含最新已提交版本。
- 需要时写入 hint/index 文件；创建 `merge-finished` 标记代表合并完成。
- 切换到新文件；旧文件成为过期，可删除。

启动考虑

- 启动加载时若检测到 `merge-finished`，跳过已合并文件的重复应用；基于 hint 与剩余文件重建索引。

源码

- `internal/layers/engine/merge.go`

TODO

- 明确触发阈值与安全校验。
- 给出前/后文件集与回收量的例子。
