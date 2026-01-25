package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
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

func (r *AbortRecord) Undo(ctx context.Context, txAccess *access.Transaction) {}

func (r *AbortRecord) Redo(ctx context.Context, txAccess *access.Transaction) {}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32]
func WriteAbortToLog(lm *log.LogMgr, prevLSN int32, txnum int32) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	rec := make([]byte, tPos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, abort)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write rollback record to log: %w", err)
	}
	return lsn, nil
}
