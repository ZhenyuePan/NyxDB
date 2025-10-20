package index

import (
	"sync"

	"github.com/plar/go-adaptive-radix-tree"

	"nyxdb/internal/layers/engine/data"
)

// ART wraps github.com/plar/go-adaptive-radix-tree to satisfy Indexer.
type ART struct {
	tree art.Tree
	lock sync.RWMutex
}

// NewART builds a new adaptive radix tree index.
func NewART() *ART {
	return &ART{
		tree: art.New(),
	}
}

// Put inserts or updates key->pos. Returns previous position if present.
func (a *ART) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	a.lock.Lock()
	defer a.lock.Unlock()
	prev, _ := a.tree.Insert(key, pos)
	if prev == nil {
		return nil
	}
	return prev.(*data.LogRecordPos)
}

// Get fetches key position.
func (a *ART) Get(key []byte) *data.LogRecordPos {
	a.lock.RLock()
	defer a.lock.RUnlock()
	val, found := a.tree.Search(key)
	if !found || val == nil {
		return nil
	}
	return val.(*data.LogRecordPos)
}

// Delete removes key and returns previous position.
func (a *ART) Delete(key []byte) (*data.LogRecordPos, bool) {
	a.lock.Lock()
	defer a.lock.Unlock()
	val, found := a.tree.Delete(key)
	if !found || val == nil {
		return nil, false
	}
	return val.(*data.LogRecordPos), true
}

// Size returns number of keys.
func (a *ART) Size() int {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.tree.Size()
}

// Iterator creates forward or reverse iterator by snapshotting keys.
func (a *ART) Iterator(reverse bool) Iterator {
	a.lock.RLock()
	defer a.lock.RUnlock()
	if a.tree.Size() == 0 {
		return newSliceIterator(nil, reverse)
	}
	items := make([]*Item, 0, a.tree.Size())
	a.tree.ForEach(func(node art.Node) bool {
		if node.Kind() == art.Leaf {
			key := append([]byte(nil), node.Key()...)
			items = append(items, &Item{key: key, pos: node.Value().(*data.LogRecordPos)})
		}
		return true
	})
	return newSliceIterator(items, reverse)
}

func (a *ART) Close() error {
	return nil
}
