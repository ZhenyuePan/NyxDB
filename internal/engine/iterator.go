package db

import (
	"bytes"
	"nyxdb/internal/engine/index"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorOptions
	readTs    uint64
	readTxn   *readTxn
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	readTxn := db.beginReadTxn()
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
		readTs:    readTxn.ts,
		readTxn:   readTxn,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个 key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历位置的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的 Value 数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	return it.db.getValueByPositionWithReadTs(logRecordPos, it.readTs)
}

// Close 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.db.endReadTxn(it.readTxn)
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen > 0 {
			if prefixLen > len(key) || !bytes.Equal(it.options.Prefix, key[:prefixLen]) {
				continue
			}
		}
		valuePos := it.indexIter.Value()
		if valuePos == nil {
			continue
		}
		value, err := it.db.getValueByPositionWithReadTs(valuePos, it.readTs)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			continue
		}
		_ = value
		break
	}
}
