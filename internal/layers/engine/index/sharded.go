package index

import "nyxdb/internal/layers/engine/data"

// Sharded wraps multiple Indexer instances to reduce contention.
type Sharded struct {
	shards []Indexer
}

// NewSharded creates a sharded index with the provided factory.
func NewSharded(shardCount int, factory func() Indexer) *Sharded {
	if shardCount <= 0 {
		shardCount = 16
	}
	shards := make([]Indexer, shardCount)
	for i := range shards {
		shards[i] = factory()
	}
	return &Sharded{shards: shards}
}

func (s *Sharded) shardFor(key []byte) int {
	count := len(s.shards)
	if count == 1 {
		return 0
	}
	if len(key) == 0 {
		return 0
	}
	// Use the first up to two bytes to preserve key ordering while routing.
	used := 2
	if len(key) < used {
		used = len(key)
	}
	var space int
	for i := 0; i < used; i++ {
		space = (space << 8) | int(key[i])
	}
	// Normalize into [0, count).
	maxValue := 1 << (8 * used)
	idx := space * count / maxValue
	if idx >= count {
		idx = count - 1
	}
	return idx
}

func (s *Sharded) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	idx := s.shardFor(key)
	return s.shards[idx].Put(key, pos)
}

func (s *Sharded) Get(key []byte) *data.LogRecordPos {
	idx := s.shardFor(key)
	return s.shards[idx].Get(key)
}

func (s *Sharded) Delete(key []byte) (*data.LogRecordPos, bool) {
	idx := s.shardFor(key)
	return s.shards[idx].Delete(key)
}

func (s *Sharded) Size() int {
	total := 0
	for _, shard := range s.shards {
		total += shard.Size()
	}
	return total
}

func (s *Sharded) Iterator(reverse bool) Iterator {
	iters := make([]Iterator, len(s.shards))
	for i := range s.shards {
		iters[i] = s.shards[i].Iterator(reverse)
	}
	return newMergeIterator(iters, reverse)
}

func (s *Sharded) Close() error {
	for _, shard := range s.shards {
		_ = shard.Close()
	}
	return nil
}
