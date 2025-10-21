package index

import (
	"sync/atomic"
	"testing"

	"nyxdb/internal/layers/engine/data"
)

func benchmarkIndexerPut(b *testing.B, factory func() Indexer) {
	idx := factory()
	defer idx.Close()

	var counter uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := atomic.AddUint64(&counter, 1)
			key := []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
			pos := &data.LogRecordPos{Fid: 0, Offset: int64(v)}
			idx.Put(key, pos)
		}
	})
}

func BenchmarkIndexPutBTree(b *testing.B) {
	benchmarkIndexerPut(b, func() Indexer { return NewBTree() })
}

func BenchmarkIndexPutSkipList(b *testing.B) {
	benchmarkIndexerPut(b, func() Indexer { return NewSkipList() })
}

func BenchmarkIndexPutART(b *testing.B) {
	benchmarkIndexerPut(b, func() Indexer { return NewART() })
}

func BenchmarkIndexPutSharded(b *testing.B) {
	benchmarkIndexerPut(b, func() Indexer {
		return NewSharded(defaultShardCount, func() Indexer { return NewBTree() })
	})
}

func benchmarkIndexerGet(b *testing.B, factory func() Indexer) {
	idx := factory()
	defer idx.Close()

	// preload
	total := b.N
	for i := 0; i < total; i++ {
		key := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		idx.Put(key, &data.LogRecordPos{Fid: 0, Offset: int64(i)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var local int
		for pb.Next() {
			local++
			i := local % total
			key := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
			_ = idx.Get(key)
		}
	})
}

func BenchmarkIndexGetBTree(b *testing.B) {
	benchmarkIndexerGet(b, func() Indexer { return NewBTree() })
}

func BenchmarkIndexGetSkipList(b *testing.B) {
	benchmarkIndexerGet(b, func() Indexer { return NewSkipList() })
}

func BenchmarkIndexGetART(b *testing.B) {
	benchmarkIndexerGet(b, func() Indexer { return NewART() })
}

func BenchmarkIndexGetSharded(b *testing.B) {
	benchmarkIndexerGet(b, func() Indexer {
		return NewSharded(defaultShardCount, func() Indexer { return NewBTree() })
	})
}
