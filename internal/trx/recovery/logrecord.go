package recovery

import (
	"context"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type LogRecord interface {
	Op() int32
	PrevLSN() int32
	TxNumber() int32
	Undo(ctx context.Context, txAccess *access.Transaction)
	Redo(ctx context.Context, txAccess *access.Transaction)
}

type UpdateLogRecord interface {
	LogRecord
	Blk() *file.BlockId
}

type CompensationLogRecord interface {
	UpdateLogRecord
	UndoNextLSN() int32
}

func CreateLogRecord(b []byte, lsn int32) LogRecord {
	p := file.NewLogPage(b)
	switch p.GetInt32(0) {
	case checkpointBegin:
		return NewCheckpointBeginRecord(lsn)
	case checkpointEnd:
		return NewCheckpointEndRecord(p, lsn)
	case start:
		return NewStartRecord(p, lsn)
	case end:
		return NewEndRecord(p, lsn)
	case commit:
		return NewCommitRecord(p, lsn)
	case abort:
		return NewAbortRecord(p, lsn)
	case setInt16:
		return NewSetInt16Record(p, lsn)
	case setInt32:
		return NewSetInt32Record(p, lsn)
	case setStr:
		return NewSetStrRecord(p, lsn)
	case setBool:
		return NewSetBoolRecord(p, lsn)
	case setDate:
		return NewSetDateRecord(p, lsn)
	case compensationSetInt16:
		return NewCompensationSetInt16Record(p, lsn)
	case compensationSetInt32:
		return NewCompensationSetInt32Record(p, lsn)
	case compensationSetStr:
		return NewCompensationSetStrRecord(p, lsn)
	case compensationSetBool:
		return NewCompensationSetBoolRecord(p, lsn)
	case compensationSetDate:
		return NewCompensationSetDateRecord(p, lsn)
	}
	return nil
}
