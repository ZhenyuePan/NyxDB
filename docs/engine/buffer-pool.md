# 缓冲池（Buffer Pool）

目标

- 降低编码/写入路径中的临时分配与 GC 压力，平滑写入延迟。
- 典型热点：日志编码（header+key+value 的大切片）、合并/Hint 写入时的临时缓冲。

适用路径

- 日志编码：`EncodeLogEntry`/`appendLogEntry`
- 批量写：`WriteBatch.Commit` 内部构造 `logEntry` 的缓冲
- 合并/Hint：`merge.go`、`DataFile.WriteHintRecord`

设计建议

- 使用 `sync.Pool` 做对象缓存；按大小分桶（如 1KB、2KB、4KB…），避免巨大切片长期驻留。
- 控制切片容量：`b = b[:0]` 复用容量；`Put` 前如需收缩可 `make` 小片复制或仅置空指针交回。
- 避免跨异步边界持有：不要把池中的缓冲交到异步 goroutine/长期结构；使用完即 `Put`。
- 池化范围：仅池化“够大且热点”的缓冲（例如 value≈1KB~64KB），极小对象用栈或栈上临时切片更划算。

示例（伪代码）

```go
var bufPools = [...]sync.Pool{ /* 按 1KB, 2KB, 4KB ... 分桶 */ }

func getBuf(n int) []byte {
    bsz := bucketSize(n)
    if v := bufPools[bsz].Get(); v != nil {
        return v.([]byte)[:0]
    }
    return make([]byte, 0, bsz)
}

func putBuf(b []byte) {
    b = b[:0]
    if cap(b) > maxCap { return } // 避免巨型对象回池
    bufPools[bucketSize(cap(b))].Put(b)
}

// 编码路径（示意）
func encodeEntry(e *LogEntry) []byte {
    need := headerEstimate(e) + len(e.Record.Key) + len(e.Record.Value)
    buf := getBuf(need)
    // 追加 header、key、value
    buf = appendHeader(buf, e)
    buf = append(buf, e.Record.Key...)
    buf = append(buf, e.Record.Value...)
    // 写完后尽快 put（若外部需要所有权，可 copy-out 再 put）
    out := make([]byte, len(buf))
    copy(out, buf)
    putBuf(buf)
    return out
}
```

验证与度量

- 结合 `-memprofile` 观察 `B/op`、`allocs/op` 变化；`pprof top -cum` 审视累计影响。
- 可加内部诊断指标：池命中率、回收量、逃逸/大对象比例。

注意事项

- 防止“复用后被外部持有”：若上层需要长期持有字节切片，必须 copy-out 再 `Put`。
- 避免复用后写入同一底层数组造成数据串扰；在并发路径尤其警惕。
- 不建议池化 header 的小缓冲（`maxLogRecordHeaderSize` 级别通常在栈上足够）。

与组提交的关系

- 缓冲池主要降低分配/GC；组提交主要摊薄 fsync。两者互补，不冲突。

何时不建议启用

- 负载极小、value 尺寸变化剧烈且逃逸拷贝不可避免时；
- 运行时内存足够、目标是简化实现而非极致性能时。

后续

- 根据实际 value 分布微调桶划分；为合并路径单独设置只增不缩的写缓冲，以减少重复扩容。

