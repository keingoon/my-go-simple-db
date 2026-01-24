package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type RecoveryMgr struct {
	lm       *log.LogMgr
	bm       *buffer.BufferMgr
	txAccess *access.Transaction
	txnum    int32
	atTbl    *ActiveTrxTable
	dptTbl   *buffer.DirtyPageTable
}

func NewRecoveryMgr(lm *log.LogMgr, bm *buffer.BufferMgr, txAccess *access.Transaction, txnum int32, atTbl *ActiveTrxTable, dptTbl *buffer.DirtyPageTable) (*RecoveryMgr, error) {
	lsn, err := WriteStartToLog(lm, txnum)
	if err != nil {
		return nil, fmt.Errorf("could not write start to log: %w", err)
	}
	atTbl.addActiveTrx(txnum, running, lsn)
	return &RecoveryMgr{lm: lm, bm: bm, txAccess: txAccess, txnum: txnum, atTbl: atTbl, dptTbl: dptTbl}, nil
}

func (rMgr *RecoveryMgr) Commit(ctx context.Context) error {
	// TODO: ARIES style recoveryを検討する
	// rMgr.bm.FlushAll(ctx, rMgr.txnum)
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not get last LSN from table: %w", err)
	}

	commitLSN, err := WriteCommitToLog(rMgr.lm, prevLSN, rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not commit: %w", err)
	}
	rMgr.lm.Flush(commitLSN)

	rMgr.atTbl.addActiveTrx(rMgr.txnum, commited, commitLSN)

	if _, err := WriteEndToLog(rMgr.lm, commitLSN, rMgr.txnum); err != nil {
		return fmt.Errorf("could not write end to log: %w", err)
	}
	rMgr.atTbl.removeActiveTrx(rMgr.txnum)

	return nil
}

func (rMgr *RecoveryMgr) Rollback(ctx context.Context) error {
	// TODO: ARIES style recoveryを検討する
	// rMgr.bm.FlushAll(ctx, rMgr.txnum)
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not get last LSN from table: %w", err)
	}

	rollbackLSN, err := WriteRollbackToLog(rMgr.lm, prevLSN, rMgr.txnum)
	if err != nil {
		return fmt.Errorf("could not rollback: %w", err)
	}
	rMgr.lm.Flush(rollbackLSN)
	rMgr.doRollback(ctx, prevLSN)

	rMgr.atTbl.addActiveTrx(rMgr.txnum, undo, rollbackLSN)
	if _, err := WriteEndToLog(rMgr.lm, rollbackLSN, rMgr.txnum); err != nil {
		return fmt.Errorf("could not write end to log: %w", err)
	}
	rMgr.atTbl.removeActiveTrx(rMgr.txnum)

	return nil
}

func (rMgr *RecoveryMgr) Checkpoint(ctx context.Context) error {
	// TODO: ARIES style recoveryを検討する
	// rMgr.bm.FlushAll(ctx, rMgr.txnum)
	beginLSN, err := WriteCheckpointBeginToLog(rMgr.lm)
	if err != nil {
		return fmt.Errorf("could not begincheckpoint: %w", err)
	}

	attSnapShot := rMgr.atTbl.getSnapshotActiveTrxTable()
	dptSnapShot := rMgr.dptTbl.GetSnapshotDirtyPageTable()
	endLSN, err := WriteCheckpointEndToLog(rMgr.lm, beginLSN, attSnapShot, dptSnapShot)
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
	lsn, err := WriteCheckpointBeginToLog(rMgr.lm)
	if err != nil {
		return fmt.Errorf("could not recover: %w", err)
	}
	rMgr.lm.Flush(lsn)
	rMgr.doRecover(ctx)

	return nil
}

func (rMgr *RecoveryMgr) SetInt16(buff *buffer.Buffer, offset int32, newVal int16) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetInt16(offset)
	blk := buff.Block()
	lsn, err := WriteSetInt16ToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, fmt.Errorf("could not setint16: %w", err)
	}
	rMgr.atTbl.addActiveTrx(rMgr.txnum, running, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetInt32(buff *buffer.Buffer, offset int32, newVal int32) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetInt32(offset)
	blk := buff.Block()
	lsn, err := WriteSetInt32ToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, fmt.Errorf("could not setint32: %w", err)
	}
	rMgr.atTbl.addActiveTrx(rMgr.txnum, running, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetStr(buff *buffer.Buffer, offset int32, newVal string) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetStr(offset)
	blk := buff.Block()
	lsn, err := WriteSetStrToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, fmt.Errorf("could not setstr: %w", err)
	}
	rMgr.atTbl.addActiveTrx(rMgr.txnum, running, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetBool(buff *buffer.Buffer, offset int32, newVal bool) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetBool(offset)
	blk := buff.Block()
	lsn, err := WriteSetBoolToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, fmt.Errorf("could not setbool: %w", err)
	}
	rMgr.atTbl.addActiveTrx(rMgr.txnum, running, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) SetDate(buff *buffer.Buffer, offset int32, newVal time.Time) (int32, error) {
	prevLSN, err := rMgr.atTbl.getLastLSN(rMgr.txnum)
	if err != nil {
		return -1, fmt.Errorf("could not get last LSN from table: %w", err)
	}

	oldVal := buff.Contents().GetDate(offset)
	blk := buff.Block()
	lsn, err := WriteSetDateToLog(rMgr.lm, prevLSN, rMgr.txnum, blk, offset, oldVal, newVal)
	if err != nil {
		return -1, err
	}
	rMgr.atTbl.addActiveTrx(rMgr.txnum, running, lsn)
	return lsn, nil
}

func (rMgr *RecoveryMgr) doRollback(ctx context.Context, undoLSN int32) error {
	if undoLSN == -1 {
		return nil
	}

	bytes, err := rMgr.lm.ReadRecordAt(undoLSN)
	if err != nil {
		fmt.Errorf("could not get log record at lsn#%d: %w", undoLSN, err)
	}

	logRec := CreateLogRecord(bytes, undoLSN)
	if logRec.TxNumber() != rMgr.txnum {
		fmt.Errorf("undo log txnum is invalid at lsn#%d", undoLSN)
	}
	if logRec.Op() == start {
		fmt.Errorf("undo log sequence is invalid at lsn#%d", undoLSN)
	}

	undoNextLSN := logRec.PrevLSN()
	return rMgr.doRollback(ctx, undoNextLSN)
}

func (rMgr *RecoveryMgr) doRecover(ctx context.Context) error {
	recovAtTbl := newActiveTrxTable()
	recovDptTbl := buffer.NewDirtyPageTable()

	// analyze phase
	if err := rMgr.doAnalyzePhase(ctx, recovAtTbl, recovDptTbl); err != nil {
		return fmt.Errorf("could not analyze phase: %w", err)
	}

	// redo phase
	if err := rMgr.doRedoPhase(ctx, recovAtTbl, recovDptTbl); err != nil {
		return fmt.Errorf("could not redo phase: %w", err)
	}

	// undo phase
	if err := rMgr.doUndoPhase(ctx, recovAtTbl); err != nil {
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
		rec := CreateLogRecord(bytes, lsn)
		switch rec.Op() {
		case checkpointEnd:
			checkPtRec := rec.(*CheckpointEndRecord)
			attSnapShot := checkPtRec.ActiveTrxTable()
			dptSnapShot := checkPtRec.DirtyPageTable()
			for txnum, entry := range attSnapShot {
				if _, ok := recovAtTbl.getActiveTrx(txnum); !ok {
					recovAtTbl.addActiveTrx(txnum, entry.getStatus(), entry.getLastLSN())
				}
			}
			for blk, entry := range dptSnapShot {
				if _, ok := recovDptTbl.GetDirtyPage(&blk); !ok {
					recovDptTbl.MarkDirtyPageTable(&blk, entry.GetRecLSN())
				}
			}
		case start:
			recovAtTbl.addActiveTrx(rec.TxNumber(), running, lsn)
		case end:
			recovAtTbl.removeActiveTrx(rec.TxNumber())
		case commit:
			recovAtTbl.addActiveTrx(rec.TxNumber(), commited, lsn)
		case rollback:
			recovAtTbl.addActiveTrx(rec.TxNumber(), undo, lsn)
		case setInt16, setInt32, setStr, setBool, setDate:
			updateRec := rec.(UpdateLogRecord)
			recovAtTbl.addActiveTrx(updateRec.TxNumber(), running, lsn)
			recovDptTbl.MarkDirtyPageTable(updateRec.Blk(), lsn)
		}
	}
	return nil
}

func (rMgr *RecoveryMgr) doRedoPhase(ctx context.Context, recovAtTbl *ActiveTrxTable, recovDptTbl *buffer.DirtyPageTable) error {
	minRecLSN, ok := rMgr.dptTbl.GetMinRecLSN()
	if !ok {
		return fmt.Errorf("could not get min rec LSN: %w", minRecLSN)
	}

	iter, err := rMgr.lm.Iterater(minRecLSN)
	if err != nil {
		return fmt.Errorf("could not get log iterator: %w", err)
	}

	// 順次開始時点からRedoする
	for iter.HasNext() {
		lsn, bytes := iter.Next()
		rec := CreateLogRecord(bytes, lsn)

		updateRec, ok := rec.(UpdateLogRecord)
		if !ok {
			continue
		}

		dptEntry, ok := recovDptTbl.GetDirtyPage(updateRec.Blk())
		if !ok {
			continue
		}

		if lsn < dptEntry.GetRecLSN() {
			continue
		}

		diskPageLSN, err := rMgr.pageLSN(ctx, rMgr.txnum, updateRec.Blk())
		if err != nil {
			return fmt.Errorf("could not get disk page LSN: %w", err)
		}
		if lsn <= diskPageLSN {
			continue
		}
		rec.Redo(ctx, rMgr.txAccess)
	}

	// ATTでCommitしているトランザクションをENDする
	commitTrxs, ok := recovAtTbl.getActiveTrxsByStatus(commited)
	if !ok {
		return nil
	}
	for _, commitTrx := range commitTrxs {
		lsn := commitTrx.getLastLSN()
		txnum := commitTrx.getTxnum()
		_, err := WriteEndToLog(rMgr.lm, lsn, txnum)
		if err != nil {
			return fmt.Errorf("could not write end to log: %w", err)
		}
		recovAtTbl.removeActiveTrx(txnum)
	}

	return nil
}

func (rMgr *RecoveryMgr) doUndoPhase(ctx context.Context, recovAtTbl *ActiveTrxTable) error {
	attEnties := recovAtTbl.getTable()
	for _, entry := range attEnties {
		status := entry.getStatus()
		if status == commited {
			continue
		}

		lsn := entry.getLastLSN()
		if err := rMgr.doRollback(ctx, lsn); err != nil {
			return fmt.Errorf("could not rollback: %w", err)
		}
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
