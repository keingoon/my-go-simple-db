package access

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/bufferlist"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
)

type LockPolicy int

const (
	// 通常実行: strict 2PL（S/Xとも commit/abort まで保持）
	LockStrict2PL LockPolicy = iota
	// 通常実行: Read Committed（Sは都度解放、Xは保持）
	LockReadCommitted
	// restart recovery 用（S/Xとも no-op）
	LockNoLock
)

type Transaction struct {
	concurMgr  *concurrency.ConcurrencyMgr
	bm         *buffer.BufferMgr
	fm         *file.FileMgr
	txnum      int32
	mybuffers  *bufferlist.BufferList
	lockPolicy LockPolicy
}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, txnum int32, mybuffers *bufferlist.BufferList) *Transaction {
	concurMgr := concurrency.NewConcurrencyMgr()
	// デフォルトは Strict2PL
	lockPolicy := LockStrict2PL
	tx := &Transaction{
		concurMgr,
		bm,
		fm,
		txnum,
		mybuffers,
		lockPolicy,
	}
	return tx
}

func NewRecoveryTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, txnum int32) *Transaction {
	// recovery 用は no-lock
	concurMgr := concurrency.NewConcurrencyMgr()
	mybuffers := bufferlist.NewBufferList(bm)
	lockPolicy := LockNoLock
	tx := &Transaction{
		concurMgr,
		bm,
		fm,
		txnum,
		mybuffers,
		lockPolicy,
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
	if tx.lockPolicy == LockNoLock {
		return nil
	}
	if err := tx.concurMgr.SLock(ctx, blk); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("slock wait timeout error: %w", err)
		}
		return fmt.Errorf("slock error: %w", err)
	}
	return nil
}

func (tx *Transaction) XLock(ctx context.Context, blk *file.BlockId) error {
	if tx.lockPolicy == LockNoLock {
		return nil
	}
	if err := tx.concurMgr.XLock(ctx, blk); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("xlock wait timeout error: %w", err)
		}
		return fmt.Errorf("xlock error: %w", err)
	}
	return nil
}

func (tx *Transaction) WithXLock(ctx context.Context, blk *file.BlockId, fn func() error) error {
	if err := tx.XLock(ctx, blk); err != nil {
		return err
	}
	return fn()
}

func (tx *Transaction) GetInt16(ctx context.Context, blk *file.BlockId, offset int32) (int16, error) {
	if err := tx.SLock(ctx, blk); err != nil {
		return 0, err
	}
	if tx.lockPolicy == LockReadCommitted {
		defer tx.Unlock(ctx, blk)
	}
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetInt16(offset), nil
}

func (tx *Transaction) GetInt32(ctx context.Context, blk *file.BlockId, offset int32) (int32, error) {
	if err := tx.SLock(ctx, blk); err != nil {
		return 0, err
	}
	if tx.lockPolicy == LockReadCommitted {
		defer tx.Unlock(ctx, blk)
	}
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetInt32(offset), nil
}

func (tx *Transaction) GetStr(ctx context.Context, blk *file.BlockId, offset int32) (string, error) {
	if err := tx.SLock(ctx, blk); err != nil {
		return "", err
	}
	if tx.lockPolicy == LockReadCommitted {
		defer tx.Unlock(ctx, blk)
	}
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetStr(offset), nil
}

func (tx *Transaction) GetBool(ctx context.Context, blk *file.BlockId, offset int32) (bool, error) {
	if err := tx.SLock(ctx, blk); err != nil {
		return false, err
	}
	if tx.lockPolicy == LockReadCommitted {
		defer tx.Unlock(ctx, blk)
	}
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetBool(offset), nil
}

func (tx *Transaction) GetDate(ctx context.Context, blk *file.BlockId, offset int32) (time.Time, error) {
	if err := tx.SLock(ctx, blk); err != nil {
		return time.Time{}, err
	}
	if tx.lockPolicy == LockReadCommitted {
		defer tx.Unlock(ctx, blk)
	}
	buff := tx.mybuffers.GetBuffer(blk)
	return buff.Contents().GetDate(offset), nil
}

func (tx *Transaction) SetInt16(
	ctx context.Context,
	blk *file.BlockId,
	offset int32,
	val int16,
	okToLog bool,
	logFn func(buff *buffer.Buffer, offset int32, oldVal int16) (int32, error),
) error {
	if err := tx.XLock(ctx, blk); err != nil {
		return err
	}
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
	if err := tx.XLock(ctx, blk); err != nil {
		return err
	}
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
	if err := tx.XLock(ctx, blk); err != nil {
		return err
	}
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
	if err := tx.XLock(ctx, blk); err != nil {
		return err
	}
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
	if err := tx.XLock(ctx, blk); err != nil {
		return err
	}
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

func (tx *Transaction) ApplyInt16(
	ctx context.Context,
	lsn int32,
	txnum int32,
	blk *file.BlockId,
	offset int32,
	val int16,
) error {
	buff := tx.mybuffers.GetBuffer(blk)
	p := buff.Contents()
	p.SetInt16(offset, val)
	buff.SetModified(txnum, lsn)
	return nil
}

func (tx *Transaction) ApplyInt32(
	ctx context.Context,
	lsn int32,
	txnum int32,
	blk *file.BlockId,
	offset int32,
	val int32,
) error {
	buff := tx.mybuffers.GetBuffer(blk)
	p := buff.Contents()
	p.SetInt32(offset, val)
	buff.SetModified(txnum, lsn)
	return nil
}

func (tx *Transaction) ApplyStr(
	ctx context.Context,
	lsn int32,
	txnum int32,
	blk *file.BlockId,
	offset int32,
	val string,
) error {
	buff := tx.mybuffers.GetBuffer(blk)
	p := buff.Contents()
	p.SetStr(offset, val)
	buff.SetModified(txnum, lsn)
	return nil
}

func (tx *Transaction) ApplyBool(
	ctx context.Context,
	lsn int32,
	txnum int32,
	blk *file.BlockId,
	offset int32,
	val bool,
) error {
	buff := tx.mybuffers.GetBuffer(blk)
	p := buff.Contents()
	p.SetBool(offset, val)
	buff.SetModified(txnum, lsn)
	return nil
}

func (tx *Transaction) ApplyDate(
	ctx context.Context,
	lsn int32,
	txnum int32,
	blk *file.BlockId,
	offset int32,
	val time.Time,
) error {
	buff := tx.mybuffers.GetBuffer(blk)
	p := buff.Contents()
	p.SetDate(offset, val)
	buff.SetModified(txnum, lsn)
	return nil
}

func (tx *Transaction) Unlock(ctx context.Context, blk *file.BlockId) {
	if tx.lockPolicy == LockNoLock {
		return
	}
	tx.concurMgr.Unlock(ctx, blk)
}

func (tx *Transaction) Release(ctx context.Context) error {
	return tx.concurMgr.Release(ctx)
}
