package log

import (
	"fmt"
	"sync"

	"github.com/keingoon/simpledb/internal/file"
)

const (
	int32Size = 4
)

type LogMgr struct {
	fm           *file.FileMgr
	logfile      string
	logpage      *file.Page
	currentblk   *file.BlockId
	latestLSN    int32
	lastSavedLSN int32
	mu           sync.Mutex
}

func NewLogMgr(fm *file.FileMgr, logfile string) (*LogMgr, error) {
	logMgr := &LogMgr{fm, logfile, nil, nil, 0, 0, sync.Mutex{}}

	b := make([]byte, fm.BlockSize())
	logpage := file.NewLogPage(b)
	logsize, err := fm.Length(logfile)
	if err != nil {
		return nil, fmt.Errorf("could not new log mgr: %w", err)
	}
	logMgr.logpage = logpage

	var currentblk *file.BlockId
	if logsize == 0 {
		currentblk, err = logMgr.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("could not new log mgr: %w", err)
		}
	} else {
		currentblk = file.NewBlockId(logfile, logsize-1)
		fm.Read(currentblk, logpage)
	}
	logMgr.currentblk = currentblk

	return logMgr, nil
}

func (LogMgr *LogMgr) Flush(lsn int32) {
	if lsn > LogMgr.lastSavedLSN {
		LogMgr.flush()
	}
}

func (logMgr *LogMgr) Iterater() *LogIterator {
	logMgr.flush()
	return NewLogIterator(logMgr.fm, logMgr.currentblk)
}

func (logMgr *LogMgr) Append(logrec []byte) (int32, error) {
	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()
	logpage := logMgr.logpage
	boundary := logpage.GetInt32(0)
	recsize := len(logrec)
	bytesneeded := recsize + int32Size
	if boundary-int32(bytesneeded) < int32Size {
		logMgr.flush()
		currentblk, err := logMgr.appendNewBlock()
		if err != nil {
			return 0, fmt.Errorf("could not append logrec: %w", err)
		}
		logMgr.currentblk = currentblk
		boundary = logpage.GetInt32(0)
	}
	recpos := boundary - int32(bytesneeded)

	logpage.SetBytes(recpos, logrec)
	logpage.SetInt32(0, recpos)
	logMgr.latestLSN += 1
	return logMgr.latestLSN, nil
}

func (logMgr *LogMgr) appendNewBlock() (*file.BlockId, error) {
	fileMgr, logfile, logpage := logMgr.fm, logMgr.logfile, logMgr.logpage
	blk, err := fileMgr.Append(logfile)
	if err != nil {
		return nil, fmt.Errorf("could not append new log block: %w", err)
	}
	logpage.SetInt32(0, fileMgr.BlockSize())
	fileMgr.Write(blk, logpage)
	return blk, nil
}

func (logMgr *LogMgr) flush() {
	fileMgr, currentblk, logpage := logMgr.fm, logMgr.currentblk, logMgr.logpage
	fileMgr.Write(currentblk, logpage)
	logMgr.lastSavedLSN = logMgr.latestLSN
}

type LogIterator struct {
	fm         *file.FileMgr
	blk        *file.BlockId
	p          *file.Page
	currentpos int32
	boundary   int32
}

func NewLogIterator(fm *file.FileMgr, blk *file.BlockId) *LogIterator {
	b := make([]byte, fm.BlockSize())
	p := file.NewLogPage(b)

	logIterator := &LogIterator{fm, blk, p, 0, 0}
	logIterator.moveToBlock(blk)
	return logIterator
}

func (logIter *LogIterator) HasNext() bool {
	fileMgr, blk, currentpos := logIter.fm, logIter.blk, logIter.currentpos
	return currentpos < fileMgr.BlockSize() || blk.Number() > 0
}

func (logIter *LogIterator) Next() []byte {
	fileMgr, blk, p, currentpos := logIter.fm, logIter.blk, logIter.p, logIter.currentpos
	if currentpos == fileMgr.BlockSize() {
		newBlk := file.NewBlockId(blk.FileName(), blk.Number()-1)
		logIter.blk = newBlk
		logIter.moveToBlock(newBlk)
	}
	rec := p.GetBytes(logIter.currentpos)
	logIter.currentpos += int32Size + int32(len(rec))
	return rec
}

func (logIter *LogIterator) moveToBlock(blk *file.BlockId) {
	fileMgr, p := logIter.fm, logIter.p
	fileMgr.Read(blk, p)
	logIter.boundary = p.GetInt32(0)
	logIter.currentpos = logIter.boundary
}
