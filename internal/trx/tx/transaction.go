package tx

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
	"github.com/keingoon/simpledb/internal/trx/bufferlist"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
	"github.com/keingoon/simpledb/internal/trx/recovery"
)

var atomicNextTxNum int32 = 0

type TransactionMgr struct {
	recoveryMgr *recovery.RecoveryMgr
	bm          *buffer.BufferMgr
	fm          *file.FileMgr
	txnum       int32
	mybuffers   *bufferlist.BufferList
	txAccess    *access.Transaction
}

func NewTransactionMgr(locktbl *concurrency.LockTable, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, atTbl *recovery.ActiveTrxTable, dptTbl *buffer.DirtyPageTable) (*TransactionMgr, error) {
	mybuffers := bufferlist.NewBufferList(bm)
	txnum := nextTxNumber()
	txAccess := access.NewTransaction(locktbl, fm, lm, bm, txnum, mybuffers)
	recoveryMgr, err := recovery.NewRecoveryMgr(locktbl, fm, lm, bm, txAccess, txnum, atTbl, dptTbl)
	if err != nil {
		return nil, fmt.Errorf("could not create recovery manager: %w", err)
	}
	txmgr := &TransactionMgr{
		recoveryMgr,
		bm,
		fm,
		txnum,
		mybuffers,
		txAccess,
	}
	return txmgr, nil
}

func NewRecoveryTransactionMgr(locktbl *concurrency.LockTable, fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, atTbl *recovery.ActiveTrxTable, dptTbl *buffer.DirtyPageTable) (*TransactionMgr, error) {
	mybuffers := bufferlist.NewBufferList(bm)
	txAccess := access.NewRecoveryTransaction(locktbl, fm, lm, bm)
	recoveryMgr := recovery.NewRecoveryMgrForRecover(locktbl, fm, lm, bm, atTbl, dptTbl)
	txmgr := &TransactionMgr{
		recoveryMgr: recoveryMgr,
		bm:          bm,
		fm:          fm,
		txnum:       access.RecoveryTxNum,
		mybuffers:   mybuffers,
		txAccess:    txAccess,
	}
	return txmgr, nil
}

func (txmgr *TransactionMgr) Commit(ctx context.Context) error {
	if _, err := txmgr.recoveryMgr.Commit(ctx); err != nil {
		return fmt.Errorf("could not commit: %w", err)
	}
	fmt.Println("transaction ", txmgr.txnum, " commited")
	if err := txmgr.txAccess.Release(ctx); err != nil {
		return fmt.Errorf("could not release transaction resources after commit: %w", err)
	}
	txmgr.mybuffers.UnpinAll(ctx)
	return nil
}

func (txmgr *TransactionMgr) Rollback(ctx context.Context) error {
	if _, err := txmgr.recoveryMgr.Rollback(ctx); err != nil {
		return fmt.Errorf("could not rollback: %w", err)
	}
	fmt.Println("transaction ", txmgr.txnum, " rolled back")
	if err := txmgr.txAccess.Release(ctx); err != nil {
		return fmt.Errorf("could not release transaction resources after rollback: %w", err)
	}
	txmgr.mybuffers.UnpinAll(ctx)
	return nil
}

func (txmgr *TransactionMgr) Recover(ctx context.Context) error {
	if err := txmgr.bm.FlushAll(ctx, txmgr.txnum); err != nil {
		return fmt.Errorf("could not flush buffers before recover: %w", err)
	}
	if err := txmgr.recoveryMgr.Recover(ctx); err != nil {
		return fmt.Errorf("could not recover: %w", err)
	}
	return nil
}

func (txmgr *TransactionMgr) Checkpoint(ctx context.Context) (int32, error) {
	return txmgr.recoveryMgr.Checkpoint(ctx)
}

func (txmgr *TransactionMgr) Pin(ctx context.Context, blk *file.BlockId) error {
	if err := txmgr.txAccess.Pin(ctx, blk); err != nil {
		return fmt.Errorf("could not pin block: %w", err)
	}
	return nil
}

func (txmgr *TransactionMgr) Unpin(ctx context.Context, blk *file.BlockId) {
	txmgr.txAccess.Unpin(ctx, blk)
}

func (txmgr *TransactionMgr) GetInt16(ctx context.Context, blk *file.BlockId, offset int32) (int16, error) {
	return txmgr.txAccess.GetInt16(ctx, blk, offset)
}

func (txmgr *TransactionMgr) GetInt32(ctx context.Context, blk *file.BlockId, offset int32) (int32, error) {
	return txmgr.txAccess.GetInt32(ctx, blk, offset)
}

func (txmgr *TransactionMgr) GetStr(ctx context.Context, blk *file.BlockId, offset int32) (string, error) {
	return txmgr.txAccess.GetStr(ctx, blk, offset)
}

func (txmgr *TransactionMgr) GetBool(ctx context.Context, blk *file.BlockId, offset int32) (bool, error) {
	return txmgr.txAccess.GetBool(ctx, blk, offset)
}

func (txmgr *TransactionMgr) GetDate(ctx context.Context, blk *file.BlockId, offset int32) (time.Time, error) {
	return txmgr.txAccess.GetDate(ctx, blk, offset)
}

func (txmgr *TransactionMgr) SetInt16(ctx context.Context, blk *file.BlockId, offset int32, val int16, okToLog bool) error {
	return txmgr.txAccess.SetInt16(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetInt16)
}

func (txmgr *TransactionMgr) SetInt32(ctx context.Context, blk *file.BlockId, offset int32, val int32, okToLog bool) error {
	return txmgr.txAccess.SetInt32(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetInt32)
}

func (txmgr *TransactionMgr) SetStr(ctx context.Context, blk *file.BlockId, offset int32, val string, okToLog bool) error {
	return txmgr.txAccess.SetStr(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetStr)
}

func (txmgr *TransactionMgr) SetBool(ctx context.Context, blk *file.BlockId, offset int32, val bool, okToLog bool) error {
	return txmgr.txAccess.SetBool(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetBool)
}

func (txmgr *TransactionMgr) SetDate(ctx context.Context, blk *file.BlockId, offset int32, val time.Time, okToLog bool) error {
	return txmgr.txAccess.SetDate(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetDate)
}

func nextTxNumber() int32 {
	atomic.AddInt32(&atomicNextTxNum, 1)
	return atomicNextTxNum
}
