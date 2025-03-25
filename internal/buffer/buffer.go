package buffer

import (
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
		txnum -= 1
	}
}

func (buff *Buffer) pin() {
	buff.pins += 1
}

func (buff *Buffer) unpin() {
	buff.pins -= 1
}

type BufferMgr struct {
	bufferpool   []*Buffer
	numAvailable int32
	Maxtime      int64
	cond         sync.Cond
}

func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numbuffs int32) *BufferMgr {
	bufferpool := make([]*Buffer, numbuffs)
	for i := 0; i < int(numbuffs); i++ {
		bufferpool[i] = NewBuffer(fm, lm)
	}

	return &BufferMgr{bufferpool, numbuffs, 10000, sync.Cond{}}
}

func (buffMgr *BufferMgr) available() int32 {
	return buffMgr.numAvailable
}

func (buffMgr *BufferMgr) flushAll(txnum int32) {
	buffMgr.cond.L.Lock()
	defer buffMgr.cond.L.Unlock()
	bufferpool := buffMgr.bufferpool
	for _, buff := range bufferpool {
		if buff.ModifyingTx() == txnum {
			buff.flush()
		}
	}
}

func (buffMgr *BufferMgr) unpin(buff *Buffer) {
	buffMgr.cond.L.Lock()
	defer buffMgr.cond.L.Unlock()

	buff.unpin()
	if !buff.IsPinned() {
		buffMgr.numAvailable += 1
	}
}

func (buffMgr *BufferMgr) Pin(blk *file.BlockId) error {
	buffMgr.cond.L.Lock()
	defer buffMgr.cond.L.Unlock()

	var buff *Buffer
	timestamp := time.Now()
	buff = buffMgr.tryToPin(blk)
	for buff == nil && !buffMgr.waitingTooLong(timestamp) {
		buffMgr.cond.Wait()
		time.Sleep(time.Duration(buffMgr.Maxtime) * time.Second)
		buff = buffMgr.tryToPin(blk)
	}
	if buff == nil {
		return errors.New("buffer abort exception")
	}
	return nil
}

func (buffMgr *BufferMgr) waitingTooLong(starttime time.Time) bool {
	// use wall clock
	currentTimestamp := time.Now().Round(0)
	return currentTimestamp.Sub(starttime).Seconds() > float64(buffMgr.Maxtime)
}

func (buffMgr *BufferMgr) tryToPin(blk *file.BlockId) *Buffer {
	var buff *Buffer
	buff = buffMgr.findExistingBuffer(blk)
	if buff == nil {
		buff = buffMgr.chooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		buff.assignToBlock(blk)
	}
	if !buff.IsPinned() {
		buffMgr.numAvailable -= 1
	}
	buff.pin()
	return buff
}

func (buffMgr *BufferMgr) findExistingBuffer(blk *file.BlockId) *Buffer {
	bufferPool := buffMgr.bufferpool
	for _, buff := range bufferPool {
		b := buff.Block()
		if b != nil && b.Equals(blk) {
			return buff
		}
	}
	return nil
}

func (buffMgr *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	bufferpool := buffMgr.bufferpool
	for _, buff := range bufferpool {
		if !buff.IsPinned() {
			return buff
		}
	}
	return nil
}
