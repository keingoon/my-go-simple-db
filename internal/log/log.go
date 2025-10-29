package log

import (
	"fmt"
	"sync"

	"github.com/keingoon/simpledb/internal/file"
)

const (
	int32Size = 4
	int16Size = 2
)

// Log file header (block 0) layout:
// magic(4) | version(2) | pageSize(4) | lastCheckpointLSN(4) | reserved(...)
const (
	headerMagicOffset             = 0
	headerVersionOffset           = headerMagicOffset + int32Size
	headerPageSizeOffset          = headerVersionOffset + int16Size
	headerLastCheckpointLSNOffset = headerPageSizeOffset + int32Size
)

const (
	// "LOGH" as bytes in little-endian: 'L','O','G','H'
	logHeaderMagic   int32 = 0x48474F4C
	logHeaderVersion int16 = 1
)

type LogMgr struct {
	fm           *file.FileMgr
	logfile      string
	logpage      *file.Page
	currentblk   *file.BlockId
	latestLSN    int32
	lastSavedLSN int32
	mu           sync.RWMutex
}

func NewLogMgr(fm *file.FileMgr, logfile string) (*LogMgr, error) {
	logMgr := &LogMgr{fm: fm, logfile: logfile, logpage: nil, currentblk: nil, latestLSN: 0, lastSavedLSN: 0}

	b := make([]byte, fm.BlockSize())
	logpage := file.NewLogPage(b)
	logsize, err := fm.Length(logfile)
	if err != nil {
		return nil, fmt.Errorf("could not new log mgr: %w", err)
	}
	logMgr.logpage = logpage

	if logsize == 0 {
		// ログヘッダーブロックの初期化
		hdrBlk, err := fm.Append(logfile)
		if err != nil {
			return nil, fmt.Errorf("could not append header block: %w", err)
		}
		if hdrBlk.Number() != 0 {
			return nil, fmt.Errorf("unexpected header block number: %d", hdrBlk.Number())
		}
		hdr := file.NewPage(fm.BlockSize())
		if err := hdr.SetInt32(headerMagicOffset, logHeaderMagic); err != nil {
			return nil, fmt.Errorf("could not write header magic: %w", err)
		}
		if err := hdr.SetInt16(headerVersionOffset, logHeaderVersion); err != nil {
			return nil, fmt.Errorf("could not write header version: %w", err)
		}
		if err := hdr.SetInt32(headerPageSizeOffset, fm.BlockSize()); err != nil {
			return nil, fmt.Errorf("could not write header page size: %w", err)
		}
		if err := hdr.SetInt32(headerLastCheckpointLSNOffset, 0); err != nil {
			return nil, fmt.Errorf("could not write header last checkpoint lsn: %w", err)
		}
		if err := fm.Write(hdrBlk, hdr); err != nil {
			return nil, fmt.Errorf("could not write header block: %w", err)
		}
	} else {
		// ログヘッダーブロックの値チェックバリデーション
		hdrBlk := file.NewBlockId(logfile, 0)
		hdr := file.NewPage(fm.BlockSize())
		if err := fm.Read(hdrBlk, hdr); err != nil {
			return nil, fmt.Errorf("could not read header block: %w", err)
		}
		if hdr.GetInt32(headerMagicOffset) != logHeaderMagic {
			return nil, fmt.Errorf("invalid log header magic")
		}
		if hdr.GetInt32(headerPageSizeOffset) != fm.BlockSize() {
			return nil, fmt.Errorf("mismatched page size in header")
		}
	}

	// ログデータブロックの追加
	if logsize <= 1 {
		currentblk, err := logMgr.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("could not create first data block: %w", err)
		}
		logMgr.currentblk = currentblk
	} else {
		currentblk := file.NewBlockId(logfile, logsize-1)
		if err := fm.Read(currentblk, logpage); err != nil {
			return nil, fmt.Errorf("could not load current log block: %w", err)
		}
		logMgr.currentblk = currentblk
	}

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
	// ログのheaderブロックを除いてnext判定
	return currentpos < fileMgr.BlockSize() || blk.Number() > 1
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

func (logMgr *LogMgr) ReadMasterLSN() (int32, error) {
	logMgr.mu.RLock()
	defer logMgr.mu.RUnlock()

	hdrBlk := file.NewBlockId(logMgr.logfile, 0)
	hdr := file.NewPage(logMgr.fm.BlockSize())
	if err := logMgr.fm.Read(hdrBlk, hdr); err != nil {
		return 0, fmt.Errorf("could not read header block: %w", err)
	}
	if hdr.GetInt32(headerMagicOffset) != logHeaderMagic {
		return 0, fmt.Errorf("invalid log header magic")
	}
	return hdr.GetInt32(headerLastCheckpointLSNOffset), nil
}

func (logMgr *LogMgr) WriteMasterLSN(lsn int32) error {
	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()

	hdrBlk := file.NewBlockId(logMgr.logfile, 0)
	hdr := file.NewPage(logMgr.fm.BlockSize())
	if err := logMgr.fm.Read(hdrBlk, hdr); err != nil {
		return fmt.Errorf("could not read header block: %w", err)
	}
	if hdr.GetInt32(headerMagicOffset) != logHeaderMagic {
		return fmt.Errorf("invalid log header magic")
	}
	if err := hdr.SetInt32(headerLastCheckpointLSNOffset, lsn); err != nil {
		return fmt.Errorf("could not set master LSN: %w", err)
	}
	if err := logMgr.fm.Write(hdrBlk, hdr); err != nil {
		return fmt.Errorf("could not write header block: %w", err)
	}
	return nil
}
