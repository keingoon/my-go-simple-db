package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type RecoveryMgr struct {
	lm       *log.LogMgr
	bm       *buffer.BufferMgr
	txAccess *access.Transaction
	txnum    int32
}

func NewRecoveryMgr(lm *log.LogMgr, bm *buffer.BufferMgr, txAccess *access.Transaction, txnum int32) *RecoveryMgr {
	WriteStartToLog(lm, txnum)
	return &RecoveryMgr{lm: lm, bm: bm, txAccess: txAccess, txnum: txnum}
}

func (rMgr *RecoveryMgr) Commit(ctx context.Context) error {
	rMgr.bm.FlushAll(ctx, rMgr.txnum)
	var lsn int32
	lsn, err := WriteCommitToLog(rMgr.lm, rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not commit: %w", err)
	}
	rMgr.lm.Flush(lsn)
	return nil
}

func (rMgr *RecoveryMgr) Rollback(ctx context.Context) error {
	rMgr.doRollback(ctx)
	rMgr.bm.FlushAll(ctx, rMgr.txnum)
	var lsn int32
	lsn, err := WriteRollbackToLog(rMgr.lm, rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not rollback: %w", err)
	}
	rMgr.lm.Flush(lsn)
	return nil
}

func (rMgr *RecoveryMgr) Recover(ctx context.Context) error {
	rMgr.doRecover(ctx)
	rMgr.bm.FlushAll(ctx, rMgr.txnum)
	var lsn int32
	lsn, err := WriteCheckpointToLog(rMgr.lm)
	if err != nil {
		return fmt.Errorf("could not recover: %w", err)
	}
	rMgr.lm.Flush(lsn)
	return nil
}

func (rMgr *RecoveryMgr) SetInt16(buff *buffer.Buffer, offset int32, newVal int16) (int32, error) {
	oldVal := buff.Contents().GetInt16(offset)
	blk := buff.Block()
	lsn, err := WriteSetInt16ToLog(rMgr.lm, rMgr.txnum, blk, offset, oldVal)
	if err != nil {
		return -1, fmt.Errorf("could not setint16: %w", err)
	}
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetInt32(buff *buffer.Buffer, offset int32, newVal int32) (int32, error) {
	oldVal := buff.Contents().GetInt32(offset)
	blk := buff.Block()
	lsn, err := WriteSetInt32ToLog(rMgr.lm, rMgr.txnum, blk, offset, oldVal)
	if err != nil {
		return -1, fmt.Errorf("could not setint32: %w", err)
	}
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetStr(buff *buffer.Buffer, offset int32, newVal string) (int32, error) {
	oldVal := buff.Contents().GetStr(offset)
	blk := buff.Block()
	lsn, err := WriteSetStrToLog(rMgr.lm, rMgr.txnum, blk, offset, oldVal)
	if err != nil {
		return -1, fmt.Errorf("could not setstr: %w", err)
	}
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetBool(buff *buffer.Buffer, offset int32, newVal bool) (int32, error) {
	oldVal := buff.Contents().GetBool(offset)
	blk := buff.Block()
	lsn, err := WriteSetBoolToLog(rMgr.lm, rMgr.txnum, blk, offset, oldVal)
	if err != nil {
		return -1, fmt.Errorf("could not setbool: %w", err)
	}
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetDate(buff *buffer.Buffer, offset int32, newVal time.Time) (int32, error) {
	oldVal := buff.Contents().GetDate(offset)
	blk := buff.Block()
	lsn, err := WriteSetDateToLog(rMgr.lm, rMgr.txnum, blk, offset, oldVal)
	if err != nil {
		return -1, err
	}
	return lsn, nil
}

func (rMgr *RecoveryMgr) doRollback(ctx context.Context) {
	iter := rMgr.lm.Iterater()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		if rec.TxNumber() == rMgr.txnum {
			if rec.Op() == start {
				return
			}
			rec.Undo(ctx, rMgr.txAccess)
		}
	}
}

func (rMgr *RecoveryMgr) doRecover(ctx context.Context) {
	finishedTxs := make(map[int32]bool)

	iter := rMgr.lm.Iterater()
	for iter.HasNext() {
		bytes := iter.Next()
		rec := CreateLogRecord(bytes)
		if rec.Op() == checkpoint {
			return
		}
		if rec.Op() == commit || rec.Op() == rollback {
			finishedTxs[rec.TxNumber()] = true
		}
		if _, ok := finishedTxs[rec.TxNumber()]; !ok {
			rec.Undo(ctx, rMgr.txAccess)
		}
	}
}
