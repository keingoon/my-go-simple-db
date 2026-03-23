package buffer

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type Buffer struct {
	fm       *file.FileMgr
	lm       *log.LogMgr
	contents *file.Page
	blk      *file.BlockId
	pins     int32
	txnum    int32
	recLSN   int32
	pageLSN  int32
	dpt      *DirtyPageTable
}

func NewBuffer(fm *file.FileMgr, lm *log.LogMgr, dpt *DirtyPageTable) *Buffer {
	p := file.NewPage(fm.BlockSize())
	return &Buffer{fm, lm, p, nil, 0, -1, -1, -1, dpt}
}

func (buff *Buffer) Contents() *file.Page {
	return buff.contents
}

func (buff *Buffer) Block() *file.BlockId {
	return buff.blk
}

func (buff *Buffer) SetModified(txnum int32, lsn int32) {
	buff.txnum = txnum
	if lsn <= 0 {
		return
	}
	buff.contents.SetPageLSN(lsn)
	buff.pageLSN = lsn

	if buff.recLSN == -1 {
		buff.recLSN = lsn
		buff.dpt.MarkDirty(buff.blk, lsn)
	}
}

func (buff *Buffer) IsPinned() bool {
	return buff.pins > 0
}

func (buff *Buffer) ModifyingTx() int32 {
	return buff.txnum
}

func (buff *Buffer) assignToBlock(blk *file.BlockId) error {
	if err := buff.flush(); err != nil {
		return err
	}

	contents := buff.contents
	if err := buff.fm.Read(blk, contents); err != nil {
		return err
	}

	buff.blk = blk
	buff.pins = 0
	return nil
}

func (buff *Buffer) flush() error {
	fm, lm, contents, blk, txnum, pageLSN := buff.fm, buff.lm, buff.contents, buff.blk, buff.txnum, buff.pageLSN
	if txnum >= 0 {
		if err := lm.Flush(pageLSN); err != nil {
			return err
		}
		if err := fm.Write(blk, contents); err != nil {
			return err
		}
		buff.txnum = -1
		buff.recLSN = -1
		buff.dpt.Clean(blk)
	}
	return nil
}

func (buff *Buffer) pin() {
	buff.pins += 1
}

func (buff *Buffer) unpin() {
	buff.pins -= 1
}

func (buff *Buffer) isDirty() bool {
	return buff.blk != nil && buff.txnum >= 0 && buff.recLSN > 0
}

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
		if prev != nil {
			prev.next = current
		}

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
		// tailなら何もしない
		return
	} else if item == head {
		// headならheadを新しい向き先に変更
		next.prev = nil
		lruList.head = next
	} else {
		// headでもtailでもなければprevとnextを繋ぎ直す
		prev.next, next.prev = next, prev
	}

	// tailにぶら下げる
	tailOld := tail
	tailOld.next, item.prev = item, tailOld
	item.next = nil
	lruList.tail = item
}

type waitToken struct{}

const Maxtime = 10000

type BufferMgr struct {
	bufferpool    []*Buffer
	blkBufferMap  map[uint64]*Buffer
	lruBufferList *LRUList
	numBuffs      int32
	numAvailable  int32
	Maxtime       int64
	mu            sync.Mutex
	numWaits      int32
	waitCh        chan *waitToken
	dpt           *DirtyPageTable
}

func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numbuffs int32, numwaits int32, dpt *DirtyPageTable) *BufferMgr {
	bufferpool := make([]*Buffer, numbuffs)
	for i := 0; i < int(numbuffs); i++ {
		bufferpool[i] = NewBuffer(fm, lm, dpt)
	}

	blkBufferMap := make(map[uint64]*Buffer, numbuffs)
	lruBufferList := NewLRUList(bufferpool)

	return &BufferMgr{bufferpool: bufferpool, blkBufferMap: blkBufferMap, lruBufferList: lruBufferList, numBuffs: numbuffs, numAvailable: numbuffs, Maxtime: Maxtime, numWaits: numwaits, waitCh: make(chan *waitToken, numwaits)}
}

func (buffMgr *BufferMgr) Available() int32 {
	return buffMgr.numAvailable
}

func (buffMgr *BufferMgr) FlushAll(ctx context.Context, txnum int32) error {
	buffMgr.mu.Lock()
	defer buffMgr.mu.Unlock()
	bufferpool := buffMgr.bufferpool
	for _, buff := range bufferpool {
		if buff.ModifyingTx() == txnum {
			if err := buff.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (buffMgr *BufferMgr) Unpin(ctx context.Context, buff *Buffer) {
	buffMgr.mu.Lock()

	buff.unpin()
	if !buff.IsPinned() {
		buffMgr.numAvailable += 1
		// INFO: bloadcastしてpin待ちgoroutineを起こす
		close(buffMgr.waitCh)
		buffMgr.waitCh = make(chan *waitToken, buffMgr.numWaits)
	}
	buffMgr.mu.Unlock()
}

func (buffMgr *BufferMgr) Pin(ctx context.Context, blk *file.BlockId) (*Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(buffMgr.Maxtime)*time.Millisecond)
	defer cancel()

	buffMgr.mu.Lock()

	var buff *Buffer
	buff, err := buffMgr.tryToPin(blk)

	for buff == nil && err == nil {
		// ロック中に現在のwaitChを確定
		waitCh := buffMgr.waitCh
		buffMgr.mu.Unlock()

		select {
		case <-waitCh:
			buffMgr.mu.Lock()
			buff, err = buffMgr.tryToPin(blk)
		case <-ctx.Done():
			return nil, errors.New("buffer abort exception")
		}
	}
	if err != nil {
		buffMgr.mu.Unlock()
		return nil, err
	}
	buffMgr.mu.Unlock()
	return buff, nil
}

func (buffMgr *BufferMgr) tryToPin(blk *file.BlockId) (*Buffer, error) {
	var buff *Buffer
	buff = buffMgr.findExistingBuffer(blk)
	if buff == nil {
		lruBufferList := buffMgr.lruBufferList
		buff = lruBufferList.ChooseVictimBuffer()
		if buff == nil {
			return nil, nil
		}

		oldBlk := buff.blk
		if err := buff.assignToBlock(blk); err != nil {
			return nil, err
		}
		if oldBlk != nil {
			delete(buffMgr.blkBufferMap, oldBlk.HashCode())
		}
		buffMgr.blkBufferMap[blk.HashCode()] = buff
	}
	if !buff.IsPinned() {
		buffMgr.numAvailable -= 1
	}
	buff.pin()
	return buff, nil
}

func (buffMgr *BufferMgr) findExistingBuffer(blk *file.BlockId) *Buffer {
	blkBufferMap := buffMgr.blkBufferMap
	blkUniqKey := blk.HashCode()
	buff, found := blkBufferMap[blkUniqKey]
	if found {
		return buff
	}
	return nil
}

type dirtyBufferCandidate struct {
	buff   *Buffer
	recLSN int32
}

func (buffMgr *BufferMgr) getDirtyBufferCandidates() []*dirtyBufferCandidate {
	candidates := make([]*dirtyBufferCandidate, 0, len(buffMgr.bufferpool))
	for _, buff := range buffMgr.bufferpool {
		if buff == nil {
			continue
		}
		if buff.IsPinned() {
			continue
		}
		if !buff.isDirty() {
			continue
		}
		candidates = append(candidates, &dirtyBufferCandidate{
			buff:   buff,
			recLSN: buff.recLSN,
		})
	}
	return candidates
}

func (buffMgr *BufferMgr) FlushPage(ctx context.Context, blk *file.BlockId) (bool, error) {
	buffMgr.mu.Lock()
	defer buffMgr.mu.Unlock()

	buff := buffMgr.findExistingBuffer(blk)
	if buff == nil {
		return false, nil
	}
	if buff.IsPinned() {
		return false, nil
	}
	if !buff.isDirty() {
		return false, nil
	}
	if err := buff.flush(); err != nil {
		return false, err
	}
	return true, nil
}

func (buffMgr *BufferMgr) FlushDirtyPages(ctx context.Context, limit int32) (int32, error) {
	if limit <= 0 {
		return 0, nil
	}

	buffMgr.mu.Lock()
	defer buffMgr.mu.Unlock()

	candidates := buffMgr.getDirtyBufferCandidates()

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].recLSN < candidates[j].recLSN
	})

	flushed := int32(0)
	for _, candidate := range candidates {
		if flushed >= limit {
			break
		}
		if err := candidate.buff.flush(); err != nil {
			return flushed, err
		}
		flushed += 1
	}
	return flushed, nil
}
