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
	"github.com/keingoon/simpledb/internal/trx/recovery"
)

const (
	EndOfFile = -1
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

func NewTransactionMgr(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *TransactionMgr {
	mybuffers := bufferlist.NewBufferList(bm)
	txnum := nextTxNumber()
	txAccess := access.NewTransaction(fm, lm, bm, txnum, mybuffers)

	txmgr := &TransactionMgr{
		recoveryMgr: recovery.NewRecoveryMgr(lm, bm, txAccess, txnum),
		bm:          bm,
		fm:          fm,
		txnum:       txnum,
		mybuffers:   mybuffers,
		txAccess:    txAccess,
	}
	return txmgr
}

func (txmgr *TransactionMgr) Commit(ctx context.Context) {
	txmgr.recoveryMgr.Commit(ctx)
	fmt.Println("transaction ", txmgr.txnum, " commited")
	txmgr.txAccess.Release(ctx)
	txmgr.mybuffers.UnpinAll(ctx)
}

func (txmgr *TransactionMgr) Rollback(ctx context.Context) {
	txmgr.recoveryMgr.Rollback(ctx)
	fmt.Println("transaction ", txmgr.txnum, " rolled back")
	txmgr.txAccess.Release(ctx)
	txmgr.mybuffers.UnpinAll(ctx)
}

func (txmgr *TransactionMgr) Recover(ctx context.Context) {
	txmgr.bm.FlushAll(ctx, txmgr.txnum)
	txmgr.recoveryMgr.Recover(ctx)
}

func (txmgr *TransactionMgr) Pin(ctx context.Context, blk *file.BlockId) {
	txmgr.txAccess.Pin(ctx, blk)
}

func (txmgr *TransactionMgr) Unpin(ctx context.Context, blk *file.BlockId) {
	txmgr.txAccess.Unpin(ctx, blk)
}

func (txmgr *TransactionMgr) GetInt16(ctx context.Context, blk *file.BlockId, offset int32) int16 {
	txmgr.txAccess.SLock(ctx, blk)
	return txmgr.txAccess.GetInt16(ctx, blk, offset)
}

func (txmgr *TransactionMgr) GetInt32(ctx context.Context, blk *file.BlockId, offset int32) int32 {
	txmgr.txAccess.SLock(ctx, blk)
	return txmgr.txAccess.GetInt32(ctx, blk, offset)
}

func (txmgr *TransactionMgr) GetStr(ctx context.Context, blk *file.BlockId, offset int32) string {
	txmgr.txAccess.SLock(ctx, blk)
	return txmgr.txAccess.GetStr(ctx, blk, offset)
}

func (txmgr *TransactionMgr) GetBool(ctx context.Context, blk *file.BlockId, offset int32) bool {
	txmgr.txAccess.SLock(ctx, blk)
	return txmgr.txAccess.GetBool(ctx, blk, offset)
}

func (txmgr *TransactionMgr) GetDate(ctx context.Context, blk *file.BlockId, offset int32) time.Time {
	txmgr.txAccess.SLock(ctx, blk)
	return txmgr.txAccess.GetDate(ctx, blk, offset)
}

func (txmgr *TransactionMgr) SetInt16(ctx context.Context, blk *file.BlockId, offset int32, val int16, okToLog bool) error {
	txmgr.txAccess.XLock(ctx, blk)
	return txmgr.txAccess.SetInt16(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetInt16)
}

func (txmgr *TransactionMgr) SetInt32(ctx context.Context, blk *file.BlockId, offset int32, val int32, okToLog bool) error {
	txmgr.txAccess.XLock(ctx, blk)
	return txmgr.txAccess.SetInt32(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetInt32)
}

func (txmgr *TransactionMgr) SetStr(ctx context.Context, blk *file.BlockId, offset int32, val string, okToLog bool) error {
	txmgr.txAccess.XLock(ctx, blk)
	return txmgr.txAccess.SetStr(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetStr)
}

func (txmgr *TransactionMgr) SetBool(ctx context.Context, blk *file.BlockId, offset int32, val bool, okToLog bool) error {
	txmgr.txAccess.XLock(ctx, blk)
	return txmgr.txAccess.SetBool(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetBool)
}

func (txmgr *TransactionMgr) SetDate(ctx context.Context, blk *file.BlockId, offset int32, val time.Time, okToLog bool) error {
	txmgr.txAccess.XLock(ctx, blk)
	return txmgr.txAccess.SetDate(ctx, blk, offset, val, okToLog, txmgr.recoveryMgr.SetDate)
}

func nextTxNumber() int32 {
	atomic.AddInt32(&atomicNextTxNum, 1)
	return atomicNextTxNum
}
