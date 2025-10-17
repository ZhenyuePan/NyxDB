package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc type keySize valueSize commitTs prevOffset
// 4 + 1 + 5 + 5 + 10 + 10 (varint upper bounds)
const (
	maxLogRecordHeaderSize = binary.MaxVarintLen32*3 + binary.MaxVarintLen64*2 + 5
	logRecordMetaFlag      = byte(1 << 7)
)

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
// LogRecord carries the user payload stored in the append-only log.
// It only contains logical data (key/value/type) without any physical metadata.
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogMeta contains the physical metadata persisted alongside a LogRecord.
// It represents commit ordering and linkage to previous versions.
type LogMeta struct {
	CommitTs   uint64
	PrevFid    uint32
	PrevOffset int64
}

// LogEntry groups a LogRecord with its metadata for encode/decode purposes.
type LogEntry struct {
	Record LogRecord
	Meta   LogMeta
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
	meta       LogMeta       // 元数据
}

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
	Size   uint32 // 标识数据在磁盘上的大小
}

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Entry *LogEntry
	Pos   *LogRecordPos
}

// EncodeLogEntry 将日志条目编码为二进制格式，并返回字节数组及其总长度。
// 编码布局如下：
//
// 当没有事务元信息 (hasMeta=0) 时：
// ┌───────────┬───────────┬────────────────┬────────────────  ┬───────────┬───────────┐
// │ CRC(4LE)  │ Type(1B)  │ KeySize(varint)│ ValueSize(varint)│  Key      │  Value    │
// └───────────┴───────────┴────────────────┴────────────────  ┴───────────┴───────────┘
// CRC 计算范围：从 Type 开始到 Value 末尾（不含 CRC 自身）
//
// 当包含事务元信息 (hasMeta=1) 时：
// ┌───────────┬────────────┬────────────────┬────────────────  ┬────────────────  ┬──────────────── ┬────────────────   ┬───────────┬───────────┐
// │ CRC(4LE)  │ Type(1B)*  │ KeySize(varint)│ ValueSize(varint)| CommitTs(uvarint)| PrevFid(uvarint)| PrevOffset(varint)|  Key      │  Value    │
// └───────────┴────────────┴────────────────┴────────────────  ┴────────────────  ┴──────────────── ┴────────────────   ┴───────────┴───────────┘
// * Type 的 bit7 用作 hasMeta 标志位（1<<7），其余位为 LogRecordType。
//
// CommitTs:   uint64 → Uvarint 编码
// PrevFid:    uint32 → Uvarint 编码
// PrevOffset: int64  → Varint 编码
// KeySize/ValueSize: int64 → Varint 编码

func EncodeLogEntry(entry *LogEntry) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Type
	typeByte := byte(entry.Record.Type)
	useMeta := entry.Meta.CommitTs != 0 || entry.Meta.PrevOffset != 0 || entry.Meta.PrevFid != 0 || entry.Record.Type == LogRecordTxnFinished
	if useMeta {
		typeByte |= logRecordMetaFlag
	}
	header[4] = typeByte
	var index = 5
	// 5 字节之后，存储的是 key 和 value 的长度信息
	// 使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(entry.Record.Key)))
	index += binary.PutVarint(header[index:], int64(len(entry.Record.Value)))
	if useMeta {
		index += binary.PutUvarint(header[index:], entry.Meta.CommitTs)
		index += binary.PutUvarint(header[index:], uint64(entry.Meta.PrevFid))
		index += binary.PutVarint(header[index:], entry.Meta.PrevOffset)
	}

	var size = index + len(entry.Record.Key) + len(entry.Record.Value)
	encBytes := make([]byte, size)

	// 将 header 部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 数据拷贝到字节数组中
	copy(encBytes[index:], entry.Record.Key)
	copy(encBytes[index+len(entry.Record.Key):], entry.Record.Value)

	// 对整个 LogRecord 的数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// Deprecated: retained for compatibility with older call sites. Prefer EncodeLogEntry.
func EncodeLogRecord(entry *LogEntry) ([]byte, int64) {
	return EncodeLogEntry(entry)
}

// EncodeLogRecordPos 对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

// DecodeLogRecordPos 解码 LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:]) //buf是有三个元素的数组
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])
	return &LogRecordPos{Fid: uint32(fileId), Offset: offset, Size: uint32(size)}
}

// 对字节数组中的 Header 信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc: binary.LittleEndian.Uint32(buf[:4]),
	}

	typeByte := buf[4]
	header.recordType = LogRecordType(typeByte &^ logRecordMetaFlag)
	hasMeta := (typeByte & logRecordMetaFlag) != 0

	var index = 5
	// 取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出实际的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	if hasMeta {
		commitTs, n := binary.Uvarint(buf[index:])
		if n <= 0 {
			return nil, 0
		}
		header.meta.CommitTs = commitTs
		index += n

		prevFid, n := binary.Uvarint(buf[index:])
		if n <= 0 {
			return nil, 0
		}
		header.meta.PrevFid = uint32(prevFid)
		index += n

		prevOffset, n := binary.Varint(buf[index:])
		if n <= 0 {
			return nil, 0
		}
		header.meta.PrevOffset = prevOffset
		index += n
	}

	return header, int64(index)
}
func getLogEntryCRC(entry *LogEntry, header []byte) uint32 {
	if entry == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, entry.Record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, entry.Record.Value)
	return crc
}
