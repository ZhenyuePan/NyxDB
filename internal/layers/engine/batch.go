package db

import (
	"encoding/binary"
	"nyxdb/internal/layers/engine/data"
	"sync"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch 原子批量写数据，保证原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex //不是读写锁了，这里只涉及写操作
	db            *DB
	pendingWrites map[string]*logEntry // 暂存用户写入的数据
	order         []string             // 记录写入顺序
}

// NewWriteBatch 初始化 WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if !db.isInitial && !db.seqNoFileExists {
		panic("cannot use write batch, seq no file not exists")
	}
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*logEntry),
		order:         make([]string, 0),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 暂存 LogRecord
	keyStr := string(key)
	if _, exists := wb.pendingWrites[keyStr]; !exists {
		wb.order = append(wb.order, keyStr)
	}
	logRecord := &logEntry{key: append([]byte(nil), key...), value: append([]byte(nil), value...), typ: data.LogRecordNormal}
	wb.pendingWrites[keyStr] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在则直接返回
	keyStr := string(key)
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[keyStr] != nil {
			delete(wb.pendingWrites, keyStr)
		}
		return nil
	}

	// 暂存 LogRecord
	if _, exists := wb.pendingWrites[keyStr]; !exists {
		wb.order = append(wb.order, keyStr)
	}
	logRecord := &logEntry{key: append([]byte(nil), key...), typ: data.LogRecordDeleted}
	wb.pendingWrites[keyStr] = logRecord
	return nil
}

// Commit 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	entries := make([]*logEntry, 0, len(wb.pendingWrites))
	for _, keyStr := range wb.order {
		if entry, ok := wb.pendingWrites[keyStr]; ok {
			entries = append(entries, entry)
		}
	}
	if len(entries) == 0 {
		wb.pendingWrites = make(map[string]*logEntry)
		wb.order = wb.order[:0]
		return nil
	}

	if err := wb.db.commitInternal(entries, wb.options.SyncWrites); err != nil {
		return err
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*logEntry)
	wb.order = wb.order[:0]

	return nil
}

// key+Seq Number 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 的 key，获取实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
