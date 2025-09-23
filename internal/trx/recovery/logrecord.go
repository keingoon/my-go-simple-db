package recovery

import (
	"context"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type LogRecord interface {
	Op() int32
	TxNumber() int32
	Undo(ctx context.Context, txAccess *access.Transaction)
}

func CreateLogRecord(b []byte) LogRecord {
	p := file.NewLogPage(b)
	switch p.GetInt32(0) {
	case checkpoint:
		return NewCheckpointRecord()
	case start:
		return NewStartRecord(p)
	case commit:
		return NewCommitRecord(p)
	case rollback:
		return NewRollbackRecord(p)
	case setInt16:
		return NewSetInt16Record(p)
	case setInt32:
		return NewSetInt32Record(p)
	case setStr:
		return NewSetStrRecord(p)
	case setBool:
		return NewSetBoolRecord(p)
	case setDate:
		return NewSetDateRecord(p)
	}
	return nil
}
