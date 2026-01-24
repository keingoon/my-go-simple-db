package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type RollbackRecord struct {
	lsn     int32
	prevLSN int32
	txnum   int32
}

func NewRollbackRecord(p *file.Page, lsn int32) *RollbackRecord {
	prevPos := int32Size
	prevLSN := p.GetInt32(int32(prevPos))
	txnumPos := prevPos + int32Size
	txnum := p.GetInt32(int32(txnumPos))

	return &RollbackRecord{lsn, prevLSN, txnum}
}

func (r *RollbackRecord) Op() int32 {
	return rollback
}

func (r *RollbackRecord) PrevLSN() int32 {
	return r.prevLSN
}

func (r *RollbackRecord) TxNumber() int32 {
	return r.txnum
}

func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txnum)
}

func (r *RollbackRecord) Undo(ctx context.Context, txAccess *access.Transaction) {}

func (r *RollbackRecord) Redo(ctx context.Context, txAccess *access.Transaction) {}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32]
func WriteRollbackToLog(lm *log.LogMgr, prevLSN int32, txnum int32) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	rec := make([]byte, tPos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, rollback)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write rollback record to log: %w", err)
	}
	return lsn, nil
}
