package index

import (
	"bytes"
	"nyxdb/internal/engine/data"
	"sync"

	"github.com/huandu/skiplist"
)

// SkipList 索引结构，基于跳表实现
// https://github.com/huandu/skiplist
type SkipList struct {
	list *skiplist.SkipList
	lock *sync.RWMutex
}

// NewSkipList 新建 SkipList 索引结构
func NewSkipList() *SkipList {
	return &SkipList{
		list: skiplist.New(skiplist.Bytes),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储 key 对应的数据位置信息
func (sl *SkipList) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	// 先用读锁获取旧值
	elem := sl.list.Get(key)
	var oldPos *data.LogRecordPos
	if elem != nil {
		oldPos, _ = elem.Value.(*data.LogRecordPos)
	}
	// 然后用写锁设置新值
	sl.lock.Lock()
	sl.list.Set(key, pos)
	sl.lock.Unlock()

	return oldPos
}

// Get 根据 key 取出对应的索引位置信息
func (sl *SkipList) Get(key []byte) *data.LogRecordPos {
	sl.lock.RLock()
	defer sl.lock.RUnlock()

	elem := sl.list.Get(key)
	if elem == nil {
		return nil
	}

	pos, _ := elem.Value.(*data.LogRecordPos)
	return pos
}

// Delete 根据 key 删除对应的索引位置信息
func (sl *SkipList) Delete(key []byte) (*data.LogRecordPos, bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	elem := sl.list.Remove(key)
	if elem == nil {
		return nil, false
	}

	pos, _ := elem.Value.(*data.LogRecordPos)
	return pos, true
}

// Size 索引中的数据量
func (sl *SkipList) Size() int {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	return sl.list.Len()
}

// Iterator 索引迭代器
func (sl *SkipList) Iterator(reverse bool) Iterator {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	return newSkipListIterator(sl.list, reverse)
}

// Close 关闭索引
func (sl *SkipList) Close() error {
	return nil
}

// SkipList 索引迭代器
type skiplistIterator struct {
	currElem *skiplist.Element
	reverse  bool
	list     *skiplist.SkipList
}

func newSkipListIterator(list *skiplist.SkipList, reverse bool) *skiplistIterator {
	var currElem *skiplist.Element
	if reverse {
		currElem = list.Back()
	} else {
		currElem = list.Front()
	}

	return &skiplistIterator{
		currElem: currElem,
		reverse:  reverse,
		list:     list,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (sli *skiplistIterator) Rewind() {
	if sli.reverse {
		sli.currElem = sli.list.Back()
	} else {
		sli.currElem = sli.list.Front()
	}
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (sli *skiplistIterator) Seek(key []byte) {
	if sli.reverse {
		// 找到第一个小于等于key的元素
		elem := sli.list.Back()
		for elem != nil {
			if bytes.Compare(elem.Key().([]byte), key) <= 0 {
				break
			}
			elem = elem.Prev()
		}
		sli.currElem = elem
	} else {
		// 找到第一个大于等于key的元素
		elem := sli.list.Front()
		for elem != nil {
			if bytes.Compare(elem.Key().([]byte), key) >= 0 {
				break
			}
			elem = elem.Next()
		}
		sli.currElem = elem
	}
}

// Next 跳转到下一个 key
func (sli *skiplistIterator) Next() {
	if sli.currElem == nil {
		return
	}

	if sli.reverse {
		sli.currElem = sli.currElem.Prev()
	} else {
		sli.currElem = sli.currElem.Next()
	}
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (sli *skiplistIterator) Valid() bool {
	return sli.currElem != nil
}

// Key 当前遍历位置的 Key 数据
func (sli *skiplistIterator) Key() []byte {
	if sli.currElem == nil {
		return nil
	}
	return sli.currElem.Key().([]byte)
}

// Value 当前遍历位置的 Value 数据
func (sli *skiplistIterator) Value() *data.LogRecordPos {
	if sli.currElem == nil {
		return nil
	}
	pos, _ := sli.currElem.Value.(*data.LogRecordPos)
	return pos
}

// Close 关闭迭代器，释放相应资源
func (sli *skiplistIterator) Close() {
	sli.currElem = nil
	sli.list = nil
}
