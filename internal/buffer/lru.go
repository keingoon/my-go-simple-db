package buffer

import "sync"

type LRUBufferItem struct {
	buff *Buffer
	prev *LRUBufferItem
	next *LRUBufferItem
}

func NewLRUBufferItem(buff *Buffer, prev *LRUBufferItem, next *LRUBufferItem) *LRUBufferItem {
	return &LRUBufferItem{buff: buff, prev: prev, next: next}
}

type LRUList struct {
	size int32
	head *LRUBufferItem
	tail *LRUBufferItem
	mu   sync.Mutex
}

func NewLRUList(bufferList []*Buffer) *LRUList {
	size := len(bufferList)
	var head, tail, prev *LRUBufferItem
	for _, buff := range bufferList {
		current := NewLRUBufferItem(buff, prev, nil)
		if head == nil {
			head = current
		}
		tail = current
		prev.next = current

		prev = current
	}
	return &LRUList{size: int32(size), head: head, tail: tail}
}

func (lruList *LRUList) ChooseVictimBuffer() *Buffer {
	head := lruList.head
	var item = head
	for item != nil {
		buff := item.buff
		if !buff.IsPinned() {
			lruList.moveToTail(item)
			return buff
		}
		item = item.next
	}
	return nil
}

func (lruList *LRUList) moveToTail(item *LRUBufferItem) {
	lruList.mu.Lock()
	defer lruList.mu.Unlock()
	prev, next := item.prev, item.next
	head, tail := lruList.head, lruList.tail
	if item == tail {
		return
	} else if item == head {
		lruList.head = next
	} else {
		prev.next, next.prev = next, prev
	}
	tailOld := tail
	tailOld.next, item.prev = item, tailOld
	lruList.tail = item
}
