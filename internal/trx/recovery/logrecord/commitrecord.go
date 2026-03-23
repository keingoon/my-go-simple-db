package logrecord

import (
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type CommitRecord struct {
	lsn     int32
	prevLSN int32
	txnum   int32
}

func NewCommitRecord(p *file.Page, lsn int32) *CommitRecord {
	prevPos := int32Size
	prevLSN := p.GetInt32(int32(prevPos))

	tPos := prevPos + int32Size
	txnum := p.GetInt32(int32(tPos))

	return &CommitRecord{
		lsn,
		prevLSN,
		txnum,
	}
}

func (r *CommitRecord) Op() int32 {
	return commit
}

func (r *CommitRecord) PrevLSN() int32 {
	return r.prevLSN
}

func (r *CommitRecord) TxNumber() int32 {
	return r.txnum
}

func (r *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txnum)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32]
func WriteCommitToLog(lm *log.LogMgr, prevLSN int32, txnum int32) (int32, error) {
	enc := newRecordEncoder(int32Size * 3)
	if err := enc.PutInt32(commit); err != nil {
		return -1, fmt.Errorf("could not encode commit record: %w", err)
	}
	if err := enc.PutInt32(prevLSN); err != nil {
		return -1, fmt.Errorf("could not encode commit record: %w", err)
	}
	if err := enc.PutInt32(txnum); err != nil {
		return -1, fmt.Errorf("could not encode commit record: %w", err)
	}
	lsn, err := lm.Append(enc.Bytes())
	if err != nil {
		return -1, fmt.Errorf("could not write commit record to log: %w", err)
	}
	return lsn, nil
}
