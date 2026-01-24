package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type CheckpointBeginRecord struct {
	lsn int32
}

func NewCheckpointBeginRecord(lsn int32) *CheckpointBeginRecord {
	return &CheckpointBeginRecord{lsn}
}

func (r *CheckpointBeginRecord) Op() int32 {
	return checkpointBegin
}

func (r *CheckpointBeginRecord) PrevLSN() int32 {
	return -1
}

func (r *CheckpointBeginRecord) TxNumber() int32 {
	return -1
}

func (r *CheckpointBeginRecord) String() string {
	return "<CHECKPOINT-BEGIN>"
}

func (r *CheckpointBeginRecord) Undo(ctx context.Context, txAccess *access.Transaction) {}

func (r *CheckpointBeginRecord) Redo(ctx context.Context, txAccess *access.Transaction) {}

// Layout:
// [op:int32]
func WriteCheckpointBeginToLog(lm *log.LogMgr) (int32, error) {
	rec := make([]byte, int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, checkpointBegin)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write checkpoint record to log: %w", err)
	}
	return lsn, nil
}
