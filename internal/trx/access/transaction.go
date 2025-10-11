package access

import (
	"context"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/bufferlist"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
)

type Transaction struct {
	concurMgr *concurrency.ConcurrencyMgr
	bm        *buffer.BufferMgr
	fm        *file.FileMgr
	txnum     int32
	mybuffers *bufferlist.BufferList
}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, txnum int32, mybuffers *bufferlist.BufferList) *Transaction {
	tx := &Transaction{
		concurMgr: concurrency.NewConcurrencyMgr(),
		bm:        bm,
		fm:        fm,
		txnum:     txnum,
		mybuffers: mybuffers,
	}
	return tx
}

func (tx *Transaction) Pin(ctx context.Context, blk *file.BlockId) {
	tx.mybuffers.Pin(ctx, blk)
}

func (tx *Transaction) Unpin(ctx context.Context, blk *file.BlockId) {
	tx.mybuffers.Unpin(ctx, blk)
}

func (tx *Transaction) SLock(ctx context.Context, blk *file.BlockId) error {
	return tx.concurMgr.SLock(ctx, blk)
}

func (tx *Transaction) XLock(ctx context.Context, blk *file.BlockId) error {
	return tx.concurMgr.XLock(ctx, blk)
}

func (tx *Transaction) GetInt16(ctx context.Context, blk *file.BlockId, offset int32) int16 {
	tx.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetInt16(offset)
}

func (tx *Transaction) GetInt32(ctx context.Context, blk *file.BlockId, offset int32) int32 {
	tx.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetInt32(offset)
}

func (tx *Transaction) GetStr(ctx context.Context, blk *file.BlockId, offset int32) string {
	tx.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetStr(offset)
}

func (tx *Transaction) GetBool(ctx context.Context, blk *file.BlockId, offset int32) bool {
	tx.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetBool(offset)
}

func (tx *Transaction) GetDate(ctx context.Context, blk *file.BlockId, offset int32) time.Time {
	tx.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetDate(offset)
}

func (tx *Transaction) SetInt16(
	ctx context.Context,
	blk *file.BlockId,
	offset int32,
	val int16,
	okToLog bool,
	logFn func(buff *buffer.Buffer, offset int32, oldVal int16) (int32, error),
) error {
	tx.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okToLog {
		loglsn, err := logFn(buff, offset, val)
		if err != nil {
			return fmt.Errorf("trx could not setint16: %w", err)
		}
		lsn = loglsn
	}
	p := buff.Contents()
	p.SetInt16(offset, val)
	buff.SetModified(tx.txnum, lsn)
	return nil
}

func (tx *Transaction) SetInt32(
	ctx context.Context,
	blk *file.BlockId,
	offset int32,
	val int32,
	okToLog bool,
	logFn func(buff *buffer.Buffer, offset int32, oldVal int32) (int32, error),
) error {
	tx.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okToLog {
		loglsn, err := logFn(buff, offset, val)
		if err != nil {
			return fmt.Errorf("trx could not setint32: %w", err)
		}
		lsn = loglsn
	}
	p := buff.Contents()
	p.SetInt32(offset, val)
	buff.SetModified(tx.txnum, int32(lsn))
	return nil
}

func (tx *Transaction) SetStr(
	ctx context.Context,
	blk *file.BlockId,
	offset int32,
	val string,
	okToLog bool,
	logFn func(buff *buffer.Buffer, offset int32, oldVal string) (int32, error),
) error {
	tx.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okToLog {
		loglsn, err := logFn(buff, offset, val)
		if err != nil {
			return fmt.Errorf("trx could not setstr: %w", err)
		}
		lsn = loglsn
	}
	p := buff.Contents()
	p.SetStr(offset, val)
	buff.SetModified(tx.txnum, int32(lsn))
	return nil
}

func (tx *Transaction) SetBool(
	ctx context.Context,
	blk *file.BlockId,
	offset int32,
	val bool,
	okToLog bool,
	logFn func(buff *buffer.Buffer, offset int32, oldVal bool) (int32, error),
) error {
	tx.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okToLog {
		loglsn, err := logFn(buff, offset, val)
		if err != nil {
			return fmt.Errorf("trx could not setbool: %w", err)
		}
		lsn = loglsn
	}
	p := buff.Contents()
	p.SetBool(offset, val)
	buff.SetModified(tx.txnum, int32(lsn))
	return nil
}

func (tx *Transaction) SetDate(
	ctx context.Context,
	blk *file.BlockId,
	offset int32,
	val time.Time,
	okToLog bool,
	logFn func(buff *buffer.Buffer, offset int32, oldVal time.Time) (int32, error),
) error {
	tx.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okToLog {
		loglsn, err := logFn(buff, offset, val)
		if err != nil {
			return fmt.Errorf("trx could not setdate: %w", err)
		}
		lsn = loglsn
	}
	p := buff.Contents()
	p.SetDate(offset, val)
	buff.SetModified(tx.txnum, int32(lsn))
	return nil
}

func (tx *Transaction) Unlock(ctx context.Context, blk *file.BlockId) {
	tx.concurMgr.Unlock(ctx, blk)
}

func (tx *Transaction) Release(ctx context.Context) error {
	return tx.concurMgr.Release(ctx)
}
