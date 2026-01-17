package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
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

func (r *CommitRecord) TxNumber() int32 {
	return r.txnum
}

func (r *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txnum)
}

func (r *CommitRecord) Undo(ctx context.Context, txAccess *access.Transaction) {}

func (r *CommitRecord) Redo(ctx context.Context, txAccess *access.Transaction) {}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32]
func WriteCommitToLog(lm *log.LogMgr, prevLSN int32, txnum int32) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	rec := make([]byte, tPos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, commit)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write commit record to log: %w", err)
	}
	return lsn, nil
}
