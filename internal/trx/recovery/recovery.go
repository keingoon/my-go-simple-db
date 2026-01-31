package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
	"github.com/keingoon/simpledb/internal/trx/recovery/logrecord"
)

type DirtyingLogRecord interface {
	logrecord.LogRecord
	Blk() *file.BlockId
}

type UndoableUpdateLogRecord interface {
	logrecord.LogRecord
	Blk() *file.BlockId
	UndoPage(ctx context.Context, txAccess *access.Transaction, clrLSN int32)
	WriteCLR(ctx context.Context, txAccess *access.Transaction, lm *log.LogMgr, prevLSN int32) (int32, error)
}

type UndoableCLRLogRecord interface {
	logrecord.LogRecord
	Blk() *file.BlockId
	UndoNextLSN() int32
}

type RedoableLogRecord interface {
	logrecord.LogRecord
	Blk() *file.BlockId
	RedoPage(ctx context.Context, txAccess *access.Transaction)
}

type RecoveryMgr struct {
	fm       *file.FileMgr
	lm       *log.LogMgr
	bm       *buffer.BufferMgr
	txAccess *access.Transaction
	txnum    int32
	atTbl    *ActiveTrxTable
	dptTbl   *buffer.DirtyPageTable
}

func NewRecoveryMgr(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr, txAccess *access.Transaction, txnum int32, atTbl *ActiveTrxTable, dptTbl *buffer.DirtyPageTable) (*RecoveryMgr, error) {
	lsn, err := logrecord.WriteStartToLog(lm, txnum)
	if err != nil {
		return nil, fmt.Errorf("could not write start to log: %w", err)
	}
	atTbl.setTrx(txnum, running, lsn)
	return &RecoveryMgr{fm, lm, bm, txAccess, txnum, atTbl, dptTbl}, nil
}

func (rMgr *RecoveryMgr) Commit(ctx context.Context) error {
	// TODO: ARIES style recoveryを検討する
	// rMgr.bm.FlushAll(ctx, rMgr.txnum)
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not get last LSN from table: %w", err)
	}

	commitLSN, err := logrecord.WriteCommitToLog(rMgr.lm, prevLSN, rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not commit: %w", err)
	}
	rMgr.lm.Flush(commitLSN)

	rMgr.atTbl.setTrx(rMgr.txnum, commited, commitLSN)

	if _, err := logrecord.WriteEndToLog(rMgr.lm, commitLSN, rMgr.txnum); err != nil {
		return fmt.Errorf("could not write end to log: %w", err)
	}
	rMgr.atTbl.removeTrx(rMgr.txnum)

	return nil
}

func (rMgr *RecoveryMgr) Rollback(ctx context.Context) error {
	// TODO: ARIES style recoveryを検討する
	// rMgr.bm.FlushAll(ctx, rMgr.txnum)
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not get last LSN from table: %w", err)
	}

	abortLSN, err := logrecord.WriteAbortToLog(rMgr.lm, prevLSN, rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not abort: %w", err)
	}
	rMgr.lm.Flush(abortLSN)
	clrLastLSN, err := rMgr.doRollback(ctx, rMgr.txAccess, rMgr.txnum, prevLSN, abortLSN)
	if err != nil {
		return fmt.Errorf("could not rollback: %w", err)
	}

	rMgr.atTbl.setTrx(rMgr.txnum, aborting, clrLastLSN)
	if _, err := logrecord.WriteEndToLog(rMgr.lm, clrLastLSN, rMgr.txnum); err != nil {
		return fmt.Errorf("could not write end to log: %w", err)
	}
	rMgr.atTbl.removeTrx(rMgr.txnum)

	return nil
}

func (rMgr *RecoveryMgr) Checkpoint(ctx context.Context) error {
	// TODO: ARIES style recoveryを検討する
	// rMgr.bm.FlushAll(ctx, rMgr.txnum)
	beginLSN, err := logrecord.WriteCheckpointBeginToLog(rMgr.lm)
	if err != nil {
		return fmt.Errorf("could not begincheckpoint: %w", err)
	}

	attSnapShot := rMgr.atTbl.getSnapshotTrxTable()
	dptSnapShot := rMgr.dptTbl.GetSnapshotTable()
	endLSN, err := logrecord.WriteCheckpointEndToLog(rMgr.lm, beginLSN, attSnapShot, dptSnapShot)
	if err != nil {
		return fmt.Errorf("could not end checkpoint: %w", err)
	}
	rMgr.lm.Flush(endLSN)

	if err := rMgr.lm.WriteMasterLSN(beginLSN); err != nil {
		return fmt.Errorf("could not write master LSN: %w", err)
	}
	return nil
}

func (rMgr *RecoveryMgr) Recover(ctx context.Context) error {
	// TODO: ARIES style recoveryを検討する
	// rMgr.bm.FlushAll(ctx, rMgr.txnum)
	var lsn int32
	lsn, err := logrecord.WriteCheckpointBeginToLog(rMgr.lm)
	if err != nil {
		return fmt.Errorf("could not recover: %w", err)
	}
	rMgr.lm.Flush(lsn)
	return rMgr.doRecover(ctx)
}

func (rMgr *RecoveryMgr) SetInt16(buff *buffer.Buffer, offset int32, newVal int16) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetInt16(offset)
	blk := buff.Block()
	lsn, err := logrecord.WriteSetInt16ToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, fmt.Errorf("could not setint16: %w", err)
	}
	rMgr.atTbl.setLastLSN(rMgr.txnum, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetInt32(buff *buffer.Buffer, offset int32, newVal int32) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetInt32(offset)
	blk := buff.Block()
	lsn, err := logrecord.WriteSetInt32ToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, fmt.Errorf("could not setint32: %w", err)
	}
	rMgr.atTbl.setLastLSN(rMgr.txnum, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetStr(buff *buffer.Buffer, offset int32, newVal string) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetStr(offset)
	blk := buff.Block()
	lsn, err := logrecord.WriteSetStrToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, fmt.Errorf("could not setstr: %w", err)
	}
	rMgr.atTbl.setLastLSN(rMgr.txnum, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetBool(buff *buffer.Buffer, offset int32, newVal bool) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetBool(offset)
	blk := buff.Block()
	lsn, err := logrecord.WriteSetBoolToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, fmt.Errorf("could not setbool: %w", err)
	}
	rMgr.atTbl.setLastLSN(rMgr.txnum, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetDate(buff *buffer.Buffer, offset int32, newVal time.Time) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetDate(offset)
	blk := buff.Block()
	lsn, err := logrecord.WriteSetDateToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, err
	}
	rMgr.atTbl.setLastLSN(rMgr.txnum, lsn)
	return lsn, nil
}

// Rollobackの開始地点LSNを引数に取り、順次Rollbackを行う
func (rMgr *RecoveryMgr) doRollback(ctx context.Context, txAccess *access.Transaction, txnum int32, undoLSN int32, prevLastLSN int32) (int32, error) {
	if undoLSN == -1 {
		return prevLastLSN, nil
	}

	bytes, err := rMgr.lm.ReadRecordAt(undoLSN)
	if err != nil {
		return -1, fmt.Errorf("could not get log record at lsn#%d: %w", undoLSN, err)
	}

	logRec := logrecord.CreateLogRecord(bytes, undoLSN)
	if logRec == nil {
		return -1, fmt.Errorf("could not create log record at lsn#%d", undoLSN)
	}
	if logRec.TxNumber() != txnum {
		return -1, fmt.Errorf("undo log txnum is invalid at lsn#%d", undoLSN)
	}
	if _, ok := logRec.(*logrecord.StartRecord); ok {
		return prevLastLSN, nil
	}

	var lastLSN int32
	var undoNextLSN int32
	switch setRec := logRec.(type) {
	case UndoableUpdateLogRecord:
		// CLRログを作成
		clrLSN, err := setRec.WriteCLR(ctx, txAccess, rMgr.lm, prevLastLSN)
		if err != nil {
			return -1, fmt.Errorf("could not write clr: %w", err)
		}
		rMgr.lm.Flush(clrLSN)

		setRec.UndoPage(ctx, txAccess, clrLSN)

		undoNextLSN = logRec.PrevLSN()
		lastLSN = clrLSN
	case UndoableCLRLogRecord:
		undoNextLSN = setRec.UndoNextLSN()
		lastLSN = prevLastLSN
	default:
		undoNextLSN = logRec.PrevLSN()
		lastLSN = prevLastLSN
	}

	return rMgr.doRollback(ctx, txAccess, txnum, undoNextLSN, lastLSN)
}

func (rMgr *RecoveryMgr) doRecover(ctx context.Context) error {
	recovAtTbl := newActiveTrxTable()
	recovDptTbl := buffer.NewDirtyPageTable()

	recovTxAccess := access.NewRecoveryTransaction(rMgr.fm, rMgr.lm, rMgr.bm, rMgr.txnum)

	// analyze phase
	if err := rMgr.doAnalyzePhase(ctx, recovAtTbl, recovDptTbl); err != nil {
		return fmt.Errorf("could not analyze phase: %w", err)
	}

	// redo phase
	if err := rMgr.doRedoPhase(ctx, recovTxAccess, recovAtTbl, recovDptTbl); err != nil {
		return fmt.Errorf("could not redo phase: %w", err)
	}

	// undo phase
	if err := rMgr.doUndoPhase(ctx, recovTxAccess, recovAtTbl); err != nil {
		return fmt.Errorf("could not redo phase: %w", err)
	}

	return nil
}

func (rMgr *RecoveryMgr) doAnalyzePhase(ctx context.Context, recovAtTbl *ActiveTrxTable, recovDptTbl *buffer.DirtyPageTable) error {
	masterLSN, err := rMgr.lm.ReadMasterLSN()
	if err != nil {
		return fmt.Errorf("could not read master LSN: %w", err)
	}

	iter, err := rMgr.lm.Iterater(masterLSN)
	if err != nil {
		return fmt.Errorf("could not get log iterator: %w", err)
	}
	for iter.HasNext() {
		lsn, bytes := iter.Next()
		rec := logrecord.CreateLogRecord(bytes, lsn)
		if rec == nil {
			return fmt.Errorf("could not create log record at lsn#%d", lsn)
		}

		switch r := rec.(type) {
		case *logrecord.CheckpointEndRecord:
			checkPtRec := r
			attSnapShot := checkPtRec.ActiveTrxTable()
			dptSnapShot := checkPtRec.DirtyPageTable()
			for txnum, entry := range attSnapShot {
				if _, ok := recovAtTbl.getTrx(txnum); !ok {
					recovAtTbl.setTrx(txnum, entry.Status, entry.LastLSN)
				}
			}
			for blk, entry := range dptSnapShot {
				if _, ok := recovDptTbl.GetPage(&blk); !ok {
					recovDptTbl.MarkDirty(&blk, entry.GetRecLSN())
				}
			}
		case *logrecord.StartRecord:
			recovAtTbl.setTrx(r.TxNumber(), running, lsn)
		case *logrecord.EndRecord:
			recovAtTbl.removeTrx(r.TxNumber())
		case *logrecord.CommitRecord:
			recovAtTbl.setTrx(r.TxNumber(), commited, lsn)
		case *logrecord.AbortRecord:
			recovAtTbl.setTrx(r.TxNumber(), aborting, lsn)
		case DirtyingLogRecord:
			recovAtTbl.setLastLSN(r.TxNumber(), lsn)
			recovDptTbl.MarkDirty(r.Blk(), lsn)
		}
	}
	return nil
}

func (rMgr *RecoveryMgr) doRedoPhase(ctx context.Context, recovTxAccess *access.Transaction, recovAtTbl *ActiveTrxTable, recovDptTbl *buffer.DirtyPageTable) error {
	minRecLSN, ok := recovDptTbl.GetMinRecLSN()
	if !ok {
		return nil
	}

	iter, err := rMgr.lm.Iterater(minRecLSN)
	if err != nil {
		return fmt.Errorf("could not get log iterator: %w", err)
	}

	// 順次開始時点からRedoする
	for iter.HasNext() {
		lsn, bytes := iter.Next()
		rec := logrecord.CreateLogRecord(bytes, lsn)
		if rec == nil {
			return fmt.Errorf("could not create log record at lsn#%d", lsn)
		}

		switch setRec := rec.(type) {
		case RedoableLogRecord:
			// LSNがDirty Page Tableに存在しない場合はRedoしない
			dptEntry, ok := recovDptTbl.GetPage(setRec.Blk())
			if !ok {
				continue
			}
			// LSNがDirty Page TableのLSN未満場合はRedoしない
			if lsn < dptEntry.GetRecLSN() {
				continue
			}
			// LSNがディスク上のPageのLSN以下の場合はRedoしない
			diskPageLSN, err := rMgr.pageLSN(ctx, rMgr.txnum, setRec.Blk())
			if err != nil {
				return fmt.Errorf("could not get disk page LSN: %w", err)
			}
			if lsn <= diskPageLSN {
				continue
			}

			setRec.RedoPage(ctx, recovTxAccess)
		default:
			continue
		}
	}

	// ATTでCommitしているトランザクションをENDする
	commitTrxs, ok := recovAtTbl.getTrxsByStatus(commited)
	if !ok {
		return nil
	}
	for _, commitTrx := range commitTrxs {
		lsn := commitTrx.getLastLSN()
		txnum := commitTrx.getTxnum()
		_, err := logrecord.WriteEndToLog(rMgr.lm, lsn, txnum)
		if err != nil {
			return fmt.Errorf("could not write end to log: %w", err)
		}
		recovAtTbl.removeTrx(txnum)
	}

	return nil
}

func (rMgr *RecoveryMgr) doUndoPhase(ctx context.Context, recovTxAccess *access.Transaction, recovAtTbl *ActiveTrxTable) error {
	attEnties := recovAtTbl.getTable()
	for _, entry := range attEnties {
		status := entry.getStatus()
		if status == commited {
			continue
		}

		txnum := entry.getTxnum()
		lsn := entry.getLastLSN()

		prevLastLSN := lsn
		if status == running {
			abortLSN, err := logrecord.WriteAbortToLog(rMgr.lm, lsn, txnum)
			if err != nil {
				return fmt.Errorf("could not abort: %w", err)
			}
			rMgr.lm.Flush(abortLSN)
			prevLastLSN = abortLSN
		}

		clrLastLSN, err := rMgr.doRollback(ctx, recovTxAccess, txnum, lsn, prevLastLSN)
		if err != nil {
			return fmt.Errorf("could not rollback: %w", err)
		}

		recovAtTbl.setTrx(txnum, aborting, clrLastLSN)
		if _, err := logrecord.WriteEndToLog(rMgr.lm, clrLastLSN, txnum); err != nil {
			return fmt.Errorf("could not write end to log: %w", err)
		}
		recovAtTbl.removeTrx(txnum)
	}
	return nil
}

func (rMgr *RecoveryMgr) pageLSN(ctx context.Context, txnum int32, blk *file.BlockId) (int32, error) {
	buff, err := rMgr.bm.Pin(ctx, blk)
	if err != nil {
		return -1, fmt.Errorf("could not pin buffer: %w", err)
	}
	defer rMgr.bm.Unpin(ctx, buff)
	return buff.Contents().GetPageLSN(), nil
}
