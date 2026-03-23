package logrecord

import (
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type EndRecord struct {
	lsn     int32
	prevLSN int32
	txnum   int32
}

func NewEndRecord(p *file.Page, lsn int32) *EndRecord {
	prevPos := int32Size
	prevLSN := p.GetInt32(int32(prevPos))
	txnumPos := prevPos + int32Size
	txnum := p.GetInt32(int32(txnumPos))

	return &EndRecord{lsn, prevLSN, txnum}
}

func (r *EndRecord) Op() int32 {
	return end
}

func (r *EndRecord) PrevLSN() int32 {
	return r.prevLSN
}

func (r *EndRecord) TxNumber() int32 {
	return r.txnum
}

func (r *EndRecord) String() string {
	return fmt.Sprintf("<END %d>", r.txnum)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32]
func WriteEndToLog(lm *log.LogMgr, prevLSN int32, txnum int32) (int32, error) {
	enc := newRecordEncoder(int32Size * 3)
	if err := enc.PutInt32(end); err != nil {
		return -1, fmt.Errorf("could not encode end record: %w", err)
	}
	if err := enc.PutInt32(prevLSN); err != nil {
		return -1, fmt.Errorf("could not encode end record: %w", err)
	}
	if err := enc.PutInt32(txnum); err != nil {
		return -1, fmt.Errorf("could not encode end record: %w", err)
	}
	lsn, err := lm.Append(enc.Bytes())
	if err != nil {
		return -1, fmt.Errorf("could not write end record to log: %w", err)
	}
	return lsn, nil
}
