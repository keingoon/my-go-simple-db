package logrecord

import (
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type AbortRecord struct {
	lsn     int32
	prevLSN int32
	txnum   int32
}

func NewAbortRecord(p *file.Page, lsn int32) *AbortRecord {
	prevPos := int32Size
	prevLSN := p.GetInt32(int32(prevPos))
	txnumPos := prevPos + int32Size
	txnum := p.GetInt32(int32(txnumPos))

	return &AbortRecord{lsn, prevLSN, txnum}
}

func (r *AbortRecord) Op() int32 {
	return abort
}

func (r *AbortRecord) PrevLSN() int32 {
	return r.prevLSN
}

func (r *AbortRecord) TxNumber() int32 {
	return r.txnum
}

func (r *AbortRecord) String() string {
	return fmt.Sprintf("<ABORT %d>", r.txnum)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32]
func WriteAbortToLog(lm *log.LogMgr, prevLSN int32, txnum int32) (int32, error) {
	enc := newRecordEncoder(int32Size * 3)
	if err := enc.PutInt32(abort); err != nil {
		return -1, fmt.Errorf("could not encode abort record: %w", err)
	}
	if err := enc.PutInt32(prevLSN); err != nil {
		return -1, fmt.Errorf("could not encode abort record: %w", err)
	}
	if err := enc.PutInt32(txnum); err != nil {
		return -1, fmt.Errorf("could not encode abort record: %w", err)
	}
	lsn, err := lm.Append(enc.Bytes())
	if err != nil {
		return -1, fmt.Errorf("could not write rollback record to log: %w", err)
	}
	return lsn, nil
}
