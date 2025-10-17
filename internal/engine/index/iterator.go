package index

import (
	"bytes"
	"sort"

	"nyxdb/internal/engine/data"
)

func newSliceIterator(items []*Item, reverse bool) Iterator {
	if len(items) == 0 {
		it := &sliceIterator{reverse: reverse, index: -1}
		it.Rewind()
		return it
	}
	copied := make([]*Item, len(items))
	copy(copied, items)
	sort.Slice(copied, func(i, j int) bool {
		return bytes.Compare(copied[i].key, copied[j].key) < 0
	})
	it := &sliceIterator{values: copied, reverse: reverse}
	it.Rewind()
	return it
}

type sliceIterator struct {
	values  []*Item
	reverse bool
	index   int
}

func (s *sliceIterator) Rewind() {
	if len(s.values) == 0 {
		s.index = -1
		return
	}
	if s.reverse {
		s.index = len(s.values) - 1
	} else {
		s.index = 0
	}
}

func (s *sliceIterator) Seek(key []byte) {
	if len(s.values) == 0 {
		s.index = -1
		return
	}
	if s.reverse {
		idx := sort.Search(len(s.values), func(i int) bool {
			return bytes.Compare(s.values[i].key, key) > 0
		})
		s.index = idx - 1
	} else {
		idx := sort.Search(len(s.values), func(i int) bool {
			return bytes.Compare(s.values[i].key, key) >= 0
		})
		s.index = idx
	}
}

func (s *sliceIterator) Next() {
	if !s.Valid() {
		return
	}
	if s.reverse {
		s.index--
	} else {
		s.index++
	}
}

func (s *sliceIterator) Valid() bool {
	return s.index >= 0 && s.index < len(s.values)
}

func (s *sliceIterator) Key() []byte {
	if !s.Valid() {
		return nil
	}
	return s.values[s.index].key
}

func (s *sliceIterator) Value() *data.LogRecordPos {
	if !s.Valid() {
		return nil
	}
	return s.values[s.index].pos
}

func (s *sliceIterator) Close() {
	s.values = nil
}

func newMergeIterator(iters []Iterator, reverse bool) Iterator {
	if len(iters) == 0 {
		return newSliceIterator(nil, reverse)
	}
	items := make([]*Item, 0)
	for _, it := range iters {
		if it == nil {
			continue
		}
		it.Rewind()
		for it.Valid() {
			keyCopy := append([]byte(nil), it.Key()...)
			items = append(items, &Item{key: keyCopy, pos: it.Value()})
			it.Next()
		}
		it.Close()
	}
	return newSliceIterator(items, reverse)
}
