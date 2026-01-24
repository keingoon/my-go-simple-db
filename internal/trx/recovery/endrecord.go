package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
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

func (r *EndRecord) Undo(ctx context.Context, txAccess *access.Transaction) {}

func (r *EndRecord) Redo(ctx context.Context, txAccess *access.Transaction) {}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32]
func WriteEndToLog(lm *log.LogMgr, prevLSN int32, txnum int32) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	rec := make([]byte, tPos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, end)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write end record to log: %w", err)
	}
	return lsn, nil
}
