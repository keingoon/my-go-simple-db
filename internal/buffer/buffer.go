package buffer

import (
	"context"
	"errors"
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
	lsn      int32
}

func NewBuffer(fm *file.FileMgr, lm *log.LogMgr) *Buffer {
	p := file.NewPage(fm.BlockSize())
	return &Buffer{fm, lm, p, nil, 0, -1, -1}
}

func (buff *Buffer) Contents() *file.Page {
	return buff.contents
}

func (buff *Buffer) Block() *file.BlockId {
	return buff.blk
}

func (buff *Buffer) SetModified(txnum int32, lsn int32) {
	buff.txnum = txnum
	if lsn > 0 {
		buff.lsn = lsn
	}
}

func (buff *Buffer) IsPinned() bool {
	return buff.pins > 0
}

func (buff *Buffer) ModifyingTx() int32 {
	return buff.txnum
}

func (buff *Buffer) assignToBlock(blk *file.BlockId) {
	buff.flush()
	fm, contents := buff.fm, buff.contents
	buff.blk = blk
	fm.Read(blk, contents)
	buff.pins = 0
}

func (buff *Buffer) flush() {
	fm, lm, contents, blk, txnum, lsn := buff.fm, buff.lm, buff.contents, buff.blk, buff.txnum, buff.lsn
	if txnum >= 0 {
		lm.Flush(lsn)
		fm.Write(blk, contents)
		buff.txnum -= 1
	}
}

func (buff *Buffer) pin() {
	buff.pins += 1
}

func (buff *Buffer) unpin() {
	buff.pins -= 1
}

type waitToken struct{}

const Maxtime = 10000

type BufferMgr struct {
	bufferpool    []*Buffer
	blkBufferMap  map[uint64]*Buffer
	lruBufferList *LRUList
	numAvailable  int32
	Maxtime       int64
	mu            sync.Mutex
	waitCh        chan *waitToken
}

func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numbuffs int32, numwaits int32) *BufferMgr {
	bufferpool := make([]*Buffer, numbuffs)
	for i := 0; i < int(numbuffs); i++ {
		bufferpool[i] = NewBuffer(fm, lm)
	}

	blkBufferMap := make(map[uint64]*Buffer, numbuffs)
	lruBufferList := NewLRUList(bufferpool)

	return &BufferMgr{bufferpool: bufferpool, blkBufferMap: blkBufferMap, lruBufferList: lruBufferList, numAvailable: numbuffs, Maxtime: Maxtime, waitCh: make(chan *waitToken, numwaits)}
}

func (buffMgr *BufferMgr) Available() int32 {
	return buffMgr.numAvailable
}

func (buffMgr *BufferMgr) FlushAll(ctx context.Context, txnum int32) {
	buffMgr.mu.Lock()
	defer buffMgr.mu.Unlock()
	bufferpool := buffMgr.bufferpool
	for _, buff := range bufferpool {
		if buff.ModifyingTx() == txnum {
			buff.flush()
		}
	}
}

func (buffMgr *BufferMgr) Unpin(ctx context.Context, buff *Buffer) {
	buffMgr.mu.Lock()

	buff.unpin()
	if !buff.IsPinned() {
		buffMgr.numAvailable += 1
		// TODO: UnpinがPinよりcall数を超過した時のgoroutine deadlock考慮する
		buffMgr.waitCh <- &waitToken{}
	}
	buffMgr.mu.Unlock()
}

func (buffMgr *BufferMgr) Pin(ctx context.Context, blk *file.BlockId) (*Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(buffMgr.Maxtime)*time.Millisecond)
	defer cancel()

	buffMgr.mu.Lock()

	var buff *Buffer
	buff = buffMgr.tryToPin(blk)

	for buff == nil {
		buffMgr.mu.Unlock()
		select {
		case <-buffMgr.waitCh:
			buffMgr.mu.Lock()
			buff = buffMgr.tryToPin(blk)
		case <-ctx.Done():
			return nil, errors.New("buffer abort exception")
		}
	}
	buffMgr.mu.Unlock()
	return buff, nil
}

func (buffMgr *BufferMgr) tryToPin(blk *file.BlockId) *Buffer {
	var buff *Buffer
	buff = buffMgr.findExistingBuffer(blk)
	if buff == nil {
		lruBufferList := buffMgr.lruBufferList
		buff = lruBufferList.ChooseVictimBuffer()
		if buff == nil {
			return nil
		}

		if buff.blk != nil {
			buffMgr.blkBufferMap[buff.blk.HashCode()] = nil
		}
		buff.assignToBlock(blk)
		buffMgr.blkBufferMap[blk.HashCode()] = buff
	}
	if !buff.IsPinned() {
		buffMgr.numAvailable -= 1
	}
	buff.pin()
	return buff
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
