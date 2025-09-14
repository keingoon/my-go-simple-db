package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type CheckpointRecord struct{}

func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (r *CheckpointRecord) Op() int32 {
	return checkpoint
}

func (r *CheckpointRecord) TxNumber() int32 {
	return -1
}

func (r *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

func (r *CheckpointRecord) Undo(ctx context.Context, tx undoContext) {}

func WriteCheckpointToLog(lm *log.LogMgr) (int32, error) {
	rec := make([]byte, int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, checkpoint)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write checkpoint record to log: %w", err)
	}
	return lsn, nil
}
