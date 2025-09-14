package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type CommitRecord struct {
	txnum int32
}

func NewCommitRecord(p *file.Page) *CommitRecord {
	tpos := int32Size
	txnum := p.GetInt32(int32(tpos))

	return &CommitRecord{
		txnum: txnum,
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

func (r *CommitRecord) Undo(ctx context.Context, tx undoContext) {}

func WriteCommitToLog(lm *log.LogMgr, txnum int32) (int32, error) {
	tpos := int32Size
	rec := make([]byte, tpos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, commit)
	p.SetInt32(int32(tpos), txnum)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write commit record to log: %w", err)
	}
	return lsn, nil
}
