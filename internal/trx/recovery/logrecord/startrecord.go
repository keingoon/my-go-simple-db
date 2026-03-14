package logrecord

import (
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type StartRecord struct {
	lsn     int32
	prevLSN int32
	txnum   int32
}

func NewStartRecord(p *file.Page, lsn int32) *StartRecord {
	prevPos := int32Size
	prevLSN := p.GetInt32(int32(prevPos))
	tPos := prevPos + int32Size
	txnum := p.GetInt32(int32(tPos))

	return &StartRecord{
		lsn,
		prevLSN,
		txnum,
	}
}

func (r *StartRecord) Op() int32 {
	return start
}

func (r *StartRecord) PrevLSN() int32 { return r.prevLSN }

func (r *StartRecord) TxNumber() int32 {
	return r.txnum
}

func (r *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", r.txnum)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32]
func WriteStartToLog(lm *log.LogMgr, txnum int32) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	rec := make([]byte, tPos+int32Size)
	p := file.NewLogPage(rec)
	if err := p.SetInt32(0, start); err != nil {
		return -1, fmt.Errorf("could not encode start record: %w", err)
	}
	if err := p.SetInt32(int32(prevPos), -1); err != nil {
		return -1, fmt.Errorf("could not encode start record: %w", err)
	}
	if err := p.SetInt32(int32(tPos), txnum); err != nil {
		return -1, fmt.Errorf("could not encode start record: %w", err)
	}
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write start record to log: %w", err)
	}
	return lsn, nil
}
