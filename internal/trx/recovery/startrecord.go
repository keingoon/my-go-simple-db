package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type StartRecord struct {
	txnum int32
}

func NewStartRecord(p *file.Page) *StartRecord {
	tpos := int32Size
	txnum := p.GetInt32(int32(tpos))

	return &StartRecord{
		txnum: txnum,
	}
}

func (r *StartRecord) Op() int32 {
	return start
}

func (r *StartRecord) TxNumber() int32 {
	return r.txnum
}

func (r *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", r.txnum)
}

func (r *StartRecord) Undo(ctx context.Context, tr undoContext) {}

func WriteStartToLog(lm *log.LogMgr, txnum int32) (int32, error) {
	tpos := int32Size
	rec := make([]byte, tpos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, start)
	p.SetInt32(int32(tpos), txnum)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write start record to log: %w", err)
	}
	return lsn, nil
}
