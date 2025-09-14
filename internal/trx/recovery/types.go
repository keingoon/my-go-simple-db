package recovery

import (
	"context"
	"time"

	"github.com/keingoon/simpledb/internal/file"
)

type undoContext interface {
	Pin(ctx context.Context, blk *file.BlockId)
	Unpin(ctx context.Context, blk *file.BlockId)
	SetInt16(ctx context.Context, blk *file.BlockId, offset int32, val int16, okToLog bool) error
	SetInt32(ctx context.Context, blk *file.BlockId, offset int32, val int32, okToLog bool) error
	SetStr(ctx context.Context, blk *file.BlockId, offset int32, val string, okToLog bool) error
	SetBool(ctx context.Context, blk *file.BlockId, offset int32, val bool, okToLog bool) error
	SetDate(ctx context.Context, blk *file.BlockId, offset int32, val time.Time, okToLog bool) error
}
