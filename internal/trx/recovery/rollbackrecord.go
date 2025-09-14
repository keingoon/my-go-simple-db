package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type RollbackRecord struct {
	txnum int32
}

func NewRollbackRecord(p *file.Page) *RollbackRecord {
	tpos := int32Size
	txnum := p.GetInt32(int32(tpos))

	return &RollbackRecord{
		txnum: txnum,
	}
}

func (r *RollbackRecord) Op() int32 {
	return rollback
}

func (r *RollbackRecord) TxNumber() int32 {
	return r.txnum
}

func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txnum)
}

func (r *RollbackRecord) Undo(ctx context.Context, tx undoContext) {}

func WriteRollbackToLog(lm *log.LogMgr, txnum int32) (int32, error) {
	tpos := int32Size
	rec := make([]byte, tpos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, rollback)
	p.SetInt32(int32(tpos), txnum)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write rollback record to log: %w", err)
	}
	return lsn, nil
}
