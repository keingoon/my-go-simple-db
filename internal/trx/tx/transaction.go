package tx

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/bufferlist"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
	"github.com/keingoon/simpledb/internal/trx/recovery"
)

const (
	EndOfFile = -1
)

var atomicNextTxNum atomic.Int32

type Transaction struct {
	recoveryMgr *recovery.RecoveryMgr
	concurMgr   *concurrency.ConcurrencyMgr
	bm          *buffer.BufferMgr
	fm          *file.FileMgr
	txnum       int32
	mybuffers   *bufferlist.BufferList
}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, txnum int32, mybuffers *bufferlist.BufferList) *Transaction {
	tx := &Transaction{
		concurMgr: concurrency.NewConcurrencyMgr(),
		bm:        bm,
		fm:        fm,
		txnum:     txnum,
		mybuffers: bufferlist.NewBufferList(bm),
	}
	tx.recoveryMgr = recovery.NewRecoveryMgr(lm, bm, tx, txnum)
	return tx
}

func (tx *Transaction) Commit(ctx context.Context) {
	tx.recoveryMgr.Commit(ctx)
	fmt.Println("transaction ", tx.txnum, " commited")
	tx.concurMgr.Release(ctx)
	tx.mybuffers.UnpinAll(ctx)
}

func (tx *Transaction) Rollback(ctx context.Context) {
	tx.recoveryMgr.Rollback(ctx)
	fmt.Println("transaction ", tx.txnum, " rolled back")
	tx.concurMgr.Release(ctx)
	tx.mybuffers.UnpinAll(ctx)
}

func (tx *Transaction) Recover(ctx context.Context) {
	tx.bm.FlushAll(ctx, tx.txnum)
	tx.recoveryMgr.Recover(ctx)
}

func (tx *Transaction) Pin(ctx context.Context, blk *file.BlockId) {
	tx.mybuffers.Pin(ctx, blk)
}

func (tx *Transaction) Unpin(ctx context.Context, blk *file.BlockId) {
	tx.mybuffers.Unpin(ctx, blk)
}

func (tx *Transaction) GetInt16(ctx context.Context, blk *file.BlockId, offset int32) int16 {
	tx.concurMgr.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetInt16(offset)
}

func (tx *Transaction) GetInt32(ctx context.Context, blk *file.BlockId, offset int32) int32 {
	tx.concurMgr.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetInt32(offset)
}

func (tx *Transaction) GetStr(ctx context.Context, blk *file.BlockId, offset int32) string {
	tx.concurMgr.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetStr(offset)
}

func (tx *Transaction) GetBool(ctx context.Context, blk *file.BlockId, offset int32) bool {
	tx.concurMgr.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetBool(offset)
}

func (tx *Transaction) GetDate(ctx context.Context, blk *file.BlockId, offset int32) time.Time {
	tx.concurMgr.SLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetDate(offset)
}

func (tx *Transaction) SetInt16(ctx context.Context, blk *file.BlockId, offset int32, val int16, okToLog bool) error {
	tx.concurMgr.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okToLog {
		loglsn, err := tx.recoveryMgr.SetInt16(buff, offset, val)
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

func (tx *Transaction) SetInt32(ctx context.Context, blk *file.BlockId, offset int32, val int32, okTolog bool) error {
	tx.concurMgr.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okTolog {
		loglsn, err := tx.recoveryMgr.SetInt32(buff, offset, val)
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

func (tx *Transaction) SetStr(ctx context.Context, blk *file.BlockId, offset int32, val string, okTolog bool) error {
	tx.concurMgr.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okTolog {
		loglsn, err := tx.recoveryMgr.SetStr(buff, offset, val)
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

func (tx *Transaction) SetBool(ctx context.Context, blk *file.BlockId, offset int32, val bool, okTolog bool) error {
	tx.concurMgr.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okTolog {
		loglsn, err := tx.recoveryMgr.SetBool(buff, offset, val)
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

func (tx *Transaction) SetDate(ctx context.Context, blk *file.BlockId, offset int32, val time.Time, okTolog bool) error {
	tx.concurMgr.XLock(ctx, blk)
	buff := tx.mybuffers.GetBuffer(blk)
	var lsn = int32(-1)
	if okTolog {
		loglsn, err := tx.recoveryMgr.SetDate(buff, offset, val)
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

func NextTxNumber() int32 {
	atomicNextTxNum.Add(1)
	return atomicNextTxNum.Load()
}
