package logrecord

import (
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

const (
	blocksize = int32(256)
	logfile   = "logfile"
	datafile  = "datafile"
)

func bytesWithOpPrevTx(t *testing.T, op, prevLSN, txnum int32) []byte {
	t.Helper()
	rec := make([]byte, int32Size*3)
	p := file.NewLogPage(rec)
	if err := p.SetInt32(0, op); err != nil {
		t.Fatalf("failed to set op: %v", err)
	}
	if err := p.SetInt32(int32Size, prevLSN); err != nil {
		t.Fatalf("failed to set prevLSN: %v", err)
	}
	if err := p.SetInt32(int32Size*2, txnum); err != nil {
		t.Fatalf("failed to set txnum: %v", err)
	}
	return rec
}

func newLogMgr(t *testing.T) (*file.FileMgr, *log.LogMgr) {
	t.Helper()
	fm, err := file.NewFileMgr(t.TempDir(), blocksize)
	if err != nil {
		t.Fatalf("failed to create FileMgr: %v", err)
	}
	lm, err := log.NewLogMgr(fm, logfile)
	if err != nil {
		t.Fatalf("failed to create LogMgr: %v", err)
	}
	return fm, lm
}

func readBack(t *testing.T, lm *log.LogMgr, lsn int32) []byte {
	t.Helper()
	lm.Flush(lsn)
	bytes, err := lm.ReadRecordAt(lsn)
	if err != nil {
		t.Fatalf("ReadRecordAt(lsn=%d) failed: %v", lsn, err)
	}
	return bytes
}

func TestLogRecord(t *testing.T) {
	t.Parallel()

	t.Run("CreateLogRecord: 全opが期待する型にdispatchされる", func(t *testing.T) {
		t.Run("opがcheckpointBeginの場合CheckpointBeginRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			lsn, err := WriteCheckpointBeginToLog(lm)
			if err != nil {
				t.Fatalf("WriteCheckpointBeginToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*CheckpointBeginRecord); !ok {
				t.Fatalf("CheckpointBeginRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがcheckpointEndの場合CheckpointEndRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const beginLSN = int32(1)
			lsn, err := WriteCheckpointEndToLog(lm, beginLSN, map[int32]CheckpointTxnEntry{}, map[file.BlockId]buffer.DirtyPageEntry{})
			if err != nil {
				t.Fatalf("WriteCheckpointEndToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*CheckpointEndRecord); !ok {
				t.Fatalf("CheckpointEndRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがstartの場合StartRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const txnum = int32(1)
			lsn, err := WriteStartToLog(lm, txnum)
			if err != nil {
				t.Fatalf("WriteStartToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*StartRecord); !ok {
				t.Fatalf("StartRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがendの場合EndRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
			)
			lsn, err := WriteEndToLog(lm, prevLSN, txnum)
			if err != nil {
				t.Fatalf("WriteEndToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*EndRecord); !ok {
				t.Fatalf("EndRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがcommitの場合CommitRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
			)
			lsn, err := WriteCommitToLog(lm, prevLSN, txnum)
			if err != nil {
				t.Fatalf("WriteCommitToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*CommitRecord); !ok {
				t.Fatalf("CommitRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがabortの場合AbortRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
			)
			lsn, err := WriteAbortToLog(lm, prevLSN, txnum)
			if err != nil {
				t.Fatalf("WriteAbortToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*AbortRecord); !ok {
				t.Fatalf("AbortRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがsetInt16の場合SetInt16Recordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
				offset  = int32(8)
				oldVal  = int16(1)
				newVal  = int16(2)
			)
			lsn, err := WriteSetInt16ToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal)
			if err != nil {
				t.Fatalf("WriteSetInt16ToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*SetInt16Record); !ok {
				t.Fatalf("SetInt16Recordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがsetInt32の場合SetInt32Recordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
				offset  = int32(8)
				oldVal  = int32(1)
				newVal  = int32(2)
			)
			lsn, err := WriteSetInt32ToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal)
			if err != nil {
				t.Fatalf("WriteSetInt32ToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*SetInt32Record); !ok {
				t.Fatalf("SetInt32Recordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがsetStrの場合SetStrRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
				offset  = int32(8)
			)
			lsn, err := WriteSetStrToLog(lm, prevLSN, txnum, blk, offset, "old", "new")
			if err != nil {
				t.Fatalf("WriteSetStrToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*SetStrRecord); !ok {
				t.Fatalf("SetStrRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがsetBoolの場合SetBoolRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
				offset  = int32(8)
				oldVal  = false
				newVal  = true
			)
			lsn, err := WriteSetBoolToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal)
			if err != nil {
				t.Fatalf("WriteSetBoolToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*SetBoolRecord); !ok {
				t.Fatalf("SetBoolRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがsetDateの場合SetDateRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
				offset  = int32(8)
			)
			oldVal := time.Unix(1_690_000_000, 0).UTC()
			newVal := time.Unix(1_690_000_100, 0).UTC()
			lsn, err := WriteSetDateToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal)
			if err != nil {
				t.Fatalf("WriteSetDateToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*SetDateRecord); !ok {
				t.Fatalf("SetDateRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがcompensationSetInt16の場合CompensationSetInt16Recordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(1)
				txnum       = int32(1)
				offset      = int32(8)
				oldVal      = int16(1)
				newVal      = int16(2)
				undoNextLSN = int32(3)
			)
			lsn, err := WriteCompensationSetInt16ToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetInt16ToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*CompensationSetInt16Record); !ok {
				t.Fatalf("CompensationSetInt16Recordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがcompensationSetInt32の場合CompensationSetInt32Recordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(1)
				txnum       = int32(1)
				offset      = int32(8)
				oldVal      = int32(1)
				newVal      = int32(2)
				undoNextLSN = int32(3)
			)
			lsn, err := WriteCompensationSetInt32ToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetInt32ToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*CompensationSetInt32Record); !ok {
				t.Fatalf("CompensationSetInt32Recordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがcompensationSetStrの場合CompensationSetStrRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(1)
				txnum       = int32(1)
				offset      = int32(8)
				undoNextLSN = int32(3)
			)
			lsn, err := WriteCompensationSetStrToLog(lm, prevLSN, txnum, blk, offset, "old", "new", undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetStrToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*CompensationSetStrRecord); !ok {
				t.Fatalf("CompensationSetStrRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがcompensationSetBoolの場合CompensationSetBoolRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(1)
				txnum       = int32(1)
				offset      = int32(8)
				oldVal      = false
				newVal      = true
				undoNextLSN = int32(3)
			)
			lsn, err := WriteCompensationSetBoolToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetBoolToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*CompensationSetBoolRecord); !ok {
				t.Fatalf("CompensationSetBoolRecordに変換できない: got=%T", rec)
			}
		})

		t.Run("opがcompensationSetDateの場合CompensationSetDateRecordを返す", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(1)
				txnum       = int32(1)
				offset      = int32(8)
				undoNextLSN = int32(3)
			)
			oldVal := time.Unix(1_690_000_000, 0).UTC()
			newVal := time.Unix(1_690_000_100, 0).UTC()
			lsn, err := WriteCompensationSetDateToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetDateToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("nilであるべきではない")
			}
			if _, ok := rec.(*CompensationSetDateRecord); !ok {
				t.Fatalf("CompensationSetDateRecordに変換できない: got=%T", rec)
			}
		})

		// bytesを直接組み立ててdispatchできることも確認する（LogMgr/WriteToLogに依存しない）
		t.Run("bytesを直接渡した場合も対応するレコードを返す", func(t *testing.T) {
			t.Run("opがstartの場合StartRecordを返す", func(t *testing.T) {
				const (
					lsn     = int32(1)
					prevLSN = int32(-1)
					txnum   = int32(1)
				)
				rec := CreateLogRecord(bytesWithOpPrevTx(t, start, prevLSN, txnum), lsn)
				if rec == nil {
					t.Fatalf("nilであるべきではない")
				}
				if _, ok := rec.(*StartRecord); !ok {
					t.Fatalf("StartRecordに変換できない: got=%T", rec)
				}
			})

			t.Run("opがendの場合EndRecordを返す", func(t *testing.T) {
				const (
					lsn     = int32(1)
					prevLSN = int32(1)
					txnum   = int32(1)
				)
				rec := CreateLogRecord(bytesWithOpPrevTx(t, end, prevLSN, txnum), lsn)
				if rec == nil {
					t.Fatalf("nilであるべきではない")
				}
				if _, ok := rec.(*EndRecord); !ok {
					t.Fatalf("EndRecordに変換できない: got=%T", rec)
				}
			})

			t.Run("opがcommitの場合CommitRecordを返す", func(t *testing.T) {
				const (
					lsn     = int32(1)
					prevLSN = int32(1)
					txnum   = int32(1)
				)
				rec := CreateLogRecord(bytesWithOpPrevTx(t, commit, prevLSN, txnum), lsn)
				if rec == nil {
					t.Fatalf("nilであるべきではない")
				}
				if _, ok := rec.(*CommitRecord); !ok {
					t.Fatalf("CommitRecordに変換できない: got=%T", rec)
				}
			})

			t.Run("opが未知の値の場合nilを返す", func(t *testing.T) {
				const (
					lsn     = int32(1)
					op      = int32(100)
					prevLSN = int32(1)
					txnum   = int32(1)
				)
				rec := CreateLogRecord(bytesWithOpPrevTx(t, op, prevLSN, txnum), lsn)
				if rec != nil {
					t.Fatalf("nilであるべきだがnilではない: got=%T", rec)
				}
			})
		})
	})

	t.Run("Write*ToLog -> ReadRecordAt -> CreateLogRecord で各レコードを復元できる", func(t *testing.T) {
		t.Run("CheckpointBeginRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			lsn, err := WriteCheckpointBeginToLog(lm)
			if err != nil {
				t.Fatalf("WriteCheckpointBeginToLog failed: %v", err)
			}
			bytes := readBack(t, lm, lsn)
			rec := CreateLogRecord(bytes, lsn)
			if rec == nil {
				t.Fatalf("expected not nil")
			}

			t.Run("OpはcheckpointBeginである", func(t *testing.T) {
				if rec.Op() != checkpointBegin {
					t.Errorf("Opが一致しない: want=%d got=%d", checkpointBegin, rec.Op())
				}
			})
			t.Run("PrevLSNは-1である", func(t *testing.T) {
				if rec.PrevLSN() != -1 {
					t.Errorf("PrevLSNが一致しない: want=%d got=%d", -1, rec.PrevLSN())
				}
			})
			t.Run("TxNumberは-1である", func(t *testing.T) {
				if rec.TxNumber() != -1 {
					t.Errorf("TxNumberが一致しない: want=%d got=%d", -1, rec.TxNumber())
				}
			})
		})

		t.Run("CheckpointEndRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const beginLSN = int32(1)
			attSnapShot := make(map[int32]CheckpointTxnEntry, 0)
			dptSnapShot := make(map[file.BlockId]buffer.DirtyPageEntry, 0)
			lsn, err := WriteCheckpointEndToLog(lm, beginLSN, attSnapShot, dptSnapShot)
			if err != nil {
				t.Fatalf("WriteCheckpointEndToLog failed: %v", err)
			}
			bytes := readBack(t, lm, lsn)
			rec := CreateLogRecord(bytes, lsn)
			if rec == nil {
				t.Fatalf("expected not nil")
			}

			t.Run("OpはcheckpointEndである", func(t *testing.T) {
				if rec.Op() != checkpointEnd {
					t.Errorf("Opが一致しない: want=%d got=%d", checkpointEnd, rec.Op())
				}
			})
			t.Run("PrevLSNは-1である", func(t *testing.T) {
				if rec.PrevLSN() != -1 {
					t.Errorf("PrevLSNが一致しない: want=%d got=%d", -1, rec.PrevLSN())
				}
			})
			t.Run("TxNumberは-1である", func(t *testing.T) {
				if rec.TxNumber() != -1 {
					t.Errorf("TxNumberが一致しない: want=%d got=%d", -1, rec.TxNumber())
				}
			})
			t.Run("CheckpointEndRecordにキャスト可能である", func(t *testing.T) {
				chckptEndRec, ok := rec.(*CheckpointEndRecord)
				if !ok {
					t.Fatalf("CheckpointEndRecordに変換できない: got=%T", rec)
				}

				t.Run("BeginLSNは引数で渡した値と一致する", func(t *testing.T) {
					if chckptEndRec.BeginLSN() != beginLSN {
						t.Errorf("BeginLSNが一致しない: want=%d got=%d", beginLSN, chckptEndRec.BeginLSN())
					}
				})

				t.Run("ActiveTrxTableは初期状態を返す", func(t *testing.T) {
					if !maps.Equal(chckptEndRec.ActiveTrxTable(), attSnapShot) {
						t.Errorf("ActiveTrxTableが一致しない: want=%v got=%v", attSnapShot, chckptEndRec.ActiveTrxTable())
					}
				})

				t.Run("DirtyPageTableは初期状態を返す", func(t *testing.T) {
					if !maps.Equal(chckptEndRec.DirtyPageTable(), dptSnapShot) {
						t.Errorf("DirtyPageTableが一致しない: want=%v got=%v", dptSnapShot, chckptEndRec.DirtyPageTable())
					}
				})
			})
		})

		t.Run("CheckpointEndRecord（ATT/DPTあり）を復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const (
				beginLSN = int32(1)
				txID     = int32(1)
				status   = int32(123)
				lastLSN  = int32(456)
				blkNum   = int32(1)
				recLSN   = int32(999)
			)

			attSnapShot := map[int32]CheckpointTxnEntry{
				txID: {Status: status, LastLSN: lastLSN},
			}
			blk := file.NewBlockId(datafile, blkNum)
			dptSnapShot := map[file.BlockId]buffer.DirtyPageEntry{
				*blk: buffer.NewDirtyPageEntry(*blk, recLSN),
			}

			lsn, err := WriteCheckpointEndToLog(lm, beginLSN, attSnapShot, dptSnapShot)
			if err != nil {
				t.Fatalf("WriteCheckpointEndToLog failed: %v", err)
			}

			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			if rec == nil {
				t.Fatalf("expected not nil")
			}
			chckptEndRec, ok := rec.(*CheckpointEndRecord)
			if !ok {
				t.Fatalf("CheckpointEndRecordに変換できない: got=%T", rec)
			}

			if got := chckptEndRec.BeginLSN(); got != beginLSN {
				t.Fatalf("BeginLSN expected %d, got %d", beginLSN, got)
			}
			if !maps.Equal(chckptEndRec.ActiveTrxTable(), attSnapShot) {
				t.Fatalf("ActiveTrxTable mismatch: want=%v got=%v", attSnapShot, chckptEndRec.ActiveTrxTable())
			}
			if !maps.Equal(chckptEndRec.DirtyPageTable(), dptSnapShot) {
				t.Fatalf("DirtyPageTable mismatch: want=%v got=%v", dptSnapShot, chckptEndRec.DirtyPageTable())
			}
		})

		t.Run("StartRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const txnum = int32(1)
			lsn, err := WriteStartToLog(lm, txnum)
			if err != nil {
				t.Fatalf("WriteStartToLog failed: %v", err)
			}
			bytes := readBack(t, lm, lsn)
			rec := CreateLogRecord(bytes, lsn)
			if rec == nil {
				t.Fatalf("expected not nil")
			}

			t.Run("Opはstartである", func(t *testing.T) {
				if rec.Op() != start {
					t.Errorf("Opが一致しない: want=%d got=%d", start, rec.Op())
				}
			})
			t.Run("PrevLSNは-1である", func(t *testing.T) {
				if rec.PrevLSN() != -1 {
					t.Errorf("PrevLSNが一致しない: want=%d got=%d", -1, rec.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if rec.TxNumber() != txnum {
					t.Errorf("TxNumberが一致しない: want=%d got=%d", txnum, rec.TxNumber())
				}
			})
		})

		t.Run("EndRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
			)
			lsn, err := WriteEndToLog(lm, prevLSN, txnum)
			if err != nil {
				t.Fatalf("WriteEndToLog failed: %v", err)
			}
			bytes := readBack(t, lm, lsn)
			rec := CreateLogRecord(bytes, lsn)
			if rec == nil {
				t.Fatalf("expected not nil")
			}

			t.Run("Opはendである", func(t *testing.T) {
				if rec.Op() != end {
					t.Errorf("Opが一致しない: want=%d got=%d", end, rec.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if rec.PrevLSN() != prevLSN {
					t.Errorf("PrevLSNが一致しない: want=%d got=%d", prevLSN, rec.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if rec.TxNumber() != txnum {
					t.Errorf("TxNumberが一致しない: want=%d got=%d", txnum, rec.TxNumber())
				}
			})
		})

		t.Run("CommitRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
			)
			lsn, err := WriteCommitToLog(lm, prevLSN, txnum)
			if err != nil {
				t.Fatalf("WriteCommitToLog failed: %v", err)
			}
			bytes := readBack(t, lm, lsn)
			rec := CreateLogRecord(bytes, lsn)
			if rec == nil {
				t.Fatalf("expected not nil")
			}

			t.Run("Opはcommitである", func(t *testing.T) {
				if rec.Op() != commit {
					t.Errorf("Opが一致しない: want=%d got=%d", commit, rec.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if rec.PrevLSN() != prevLSN {
					t.Errorf("PrevLSNが一致しない: want=%d got=%d", prevLSN, rec.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if rec.TxNumber() != txnum {
					t.Errorf("TxNumberが一致しない: want=%d got=%d", txnum, rec.TxNumber())
				}
			})
		})

		t.Run("AbortRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
			)
			lsn, err := WriteAbortToLog(lm, prevLSN, txnum)
			if err != nil {
				t.Fatalf("WriteAbortToLog failed: %v", err)
			}
			bytes := readBack(t, lm, lsn)
			rec := CreateLogRecord(bytes, lsn)
			if rec == nil {
				t.Fatalf("expected not nil")
			}

			t.Run("Opはabortである", func(t *testing.T) {
				if rec.Op() != abort {
					t.Errorf("Opが一致しない: want=%d got=%d", abort, rec.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if rec.PrevLSN() != prevLSN {
					t.Errorf("PrevLSNが一致しない: want=%d got=%d", prevLSN, rec.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if rec.TxNumber() != txnum {
					t.Errorf("TxNumberが一致しない: want=%d got=%d", txnum, rec.TxNumber())
				}
			})
		})

		t.Run("SetBoolRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
				blkNum  = int32(1)
				offset  = int32(int32Size)
				oldVal  = false
				newVal  = true
			)
			blk := file.NewBlockId(datafile, blkNum)
			lsn, err := WriteSetBoolToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal)
			if err != nil {
				t.Fatalf("WriteSetBoolToLog failed: %v", err)
			}
			bytes := readBack(t, lm, lsn)
			rec := CreateLogRecord(bytes, lsn)
			if rec == nil {
				t.Fatalf("expected not nil")
			}

			t.Run("OpはsetBoolである", func(t *testing.T) {
				if rec.Op() != setBool {
					t.Errorf("Opが一致しない: want=%d got=%d", setBool, rec.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if rec.PrevLSN() != prevLSN {
					t.Errorf("PrevLSNが一致しない: want=%d got=%d", prevLSN, rec.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if rec.TxNumber() != txnum {
					t.Errorf("TxNumberが一致しない: want=%d got=%d", txnum, rec.TxNumber())
				}
			})

			t.Run("SetBoolRecordにキャスト可能である", func(t *testing.T) {
				setBoolRec, ok := rec.(*SetBoolRecord)
				if !ok {
					t.Fatalf("SetBoolRecordに変換できない: got=%T", rec)
				}

				t.Run("Blkは (datafile, 1) である", func(t *testing.T) {
					t.Run("Blk.FileNameはdatafileである", func(t *testing.T) {
						if setBoolRec.Blk().FileName() != datafile {
							t.Errorf("Blk.FileNameが一致しない: want=%s got=%s", datafile, setBoolRec.Blk().FileName())
						}
					})

					t.Run("Blk.Numberは1である", func(t *testing.T) {
						if setBoolRec.Blk().Number() != 1 {
							t.Errorf("Blk.Numberが一致しない: want=%d got=%d", 1, setBoolRec.Blk().Number())
						}
					})
				})
			})
		})

		t.Run("SetStrRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN = int32(1)
				txnum   = int32(1)
				offset  = int32(8)
			)

			t.Run("oldValが空文字、newValが1文字でも復元できる", func(t *testing.T) {
				lsn, err := WriteSetStrToLog(lm, prevLSN, txnum, blk, offset, "", "a")
				if err != nil {
					t.Fatalf("WriteSetStrToLogが失敗した: %v", err)
				}
				rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
				r, ok := rec.(*SetStrRecord)
				if !ok {
					t.Fatalf("SetStrRecordに変換できない: %T", rec)
				}
				if r.oldVal != "" || r.newVal != "a" {
					t.Fatalf("oldVal/newValが一致しない: old=%q new=%q", r.oldVal, r.newVal)
				}
			})

			t.Run("oldValが長く、newValが短くても復元できる", func(t *testing.T) {
				oldVal := "longer_value_here"
				newVal := "s"
				lsn, err := WriteSetStrToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal)
				if err != nil {
					t.Fatalf("WriteSetStrToLogが失敗した: %v", err)
				}
				rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
				r, ok := rec.(*SetStrRecord)
				if !ok {
					t.Fatalf("SetStrRecordに変換できない: %T", rec)
				}
				if r.oldVal != oldVal || r.newVal != newVal {
					t.Fatalf("oldVal/newValが一致しない: old=%q new=%q", r.oldVal, r.newVal)
				}
			})
		})

		t.Run("SetStrRecord（長いファイル名）を復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			longName := strings.Repeat("x", 60)
			blk := file.NewBlockId(longName, 7)
			const (
				prevLSN = int32(10)
				txnum   = int32(3)
				offset  = int32(32)
			)
			oldVal, newVal := "old", "new_value"
			lsn, err := WriteSetStrToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal)
			if err != nil {
				t.Fatalf("WriteSetStrToLog failed: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			r, ok := rec.(*SetStrRecord)
			if !ok {
				t.Fatalf("SetStrRecordに変換できない: got=%T", rec)
			}
			if got := r.blk.FileName(); got != longName {
				t.Fatalf("blk filename mismatch: want=%q got=%q", longName, got)
			}
			if got := r.blk.Number(); got != 7 {
				t.Fatalf("blk number mismatch: want=7 got=%d", got)
			}
			if r.prevLSN != prevLSN || r.txnum != txnum || r.offset != offset || r.oldVal != oldVal || r.newVal != newVal {
				t.Fatalf("record fields mismatch: prevLSN=%d txnum=%d offset=%d old=%q new=%q", r.prevLSN, r.txnum, r.offset, r.oldVal, r.newVal)
			}
		})

		t.Run("CompensationSetInt16Recordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(10)
				txnum       = int32(99)
				offset      = int32(16)
				oldVal      = int16(1)
				newVal      = int16(2)
				undoNextLSN = int32(7)
			)

			lsn, err := WriteCompensationSetInt16ToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetInt16ToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			r, ok := rec.(*CompensationSetInt16Record)
			if !ok {
				t.Fatalf("CompensationSetInt16Recordに変換できない: %T", rec)
			}

			t.Run("OpはcompensationSetInt16である", func(t *testing.T) {
				if r.Op() != compensationSetInt16 {
					t.Fatalf("Opが一致しない: want=%d got=%d", compensationSetInt16, r.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.PrevLSN() != prevLSN {
					t.Fatalf("PrevLSNが一致しない: want=%d got=%d", prevLSN, r.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if r.TxNumber() != txnum {
					t.Fatalf("TxNumberが一致しない: want=%d got=%d", txnum, r.TxNumber())
				}
			})
			t.Run("Blkは引数で渡したBlockIdと一致する", func(t *testing.T) {
				if got := r.Blk().FileName(); got != datafile {
					t.Fatalf("Blk.FileName expected %q, got %q", datafile, got)
				}
				if got := r.Blk().Number(); got != 1 {
					t.Fatalf("Blk.Number expected %d, got %d", 1, got)
				}
			})
			t.Run("offsetは引数で渡した値と一致する", func(t *testing.T) {
				if r.offset != offset {
					t.Fatalf("offsetが一致しない: want=%d got=%d", offset, r.offset)
				}
			})
			t.Run("oldValは引数で渡した値と一致する", func(t *testing.T) {
				if r.oldVal != oldVal {
					t.Fatalf("oldValが一致しない: want=%d got=%d", oldVal, r.oldVal)
				}
			})
			t.Run("newValは引数で渡した値と一致する", func(t *testing.T) {
				if r.newVal != newVal {
					t.Fatalf("newValが一致しない: want=%d got=%d", newVal, r.newVal)
				}
			})
			t.Run("UndoNextLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.UndoNextLSN() != undoNextLSN {
					t.Fatalf("UndoNextLSNが一致しない: want=%d got=%d", undoNextLSN, r.UndoNextLSN())
				}
			})
		})

		t.Run("CompensationSetInt32Recordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(10)
				txnum       = int32(99)
				offset      = int32(16)
				oldVal      = int32(3)
				newVal      = int32(4)
				undoNextLSN = int32(7)
			)

			lsn, err := WriteCompensationSetInt32ToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetInt32ToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			r, ok := rec.(*CompensationSetInt32Record)
			if !ok {
				t.Fatalf("CompensationSetInt32Recordに変換できない: %T", rec)
			}

			t.Run("OpはcompensationSetInt32である", func(t *testing.T) {
				if r.Op() != compensationSetInt32 {
					t.Fatalf("Opが一致しない: want=%d got=%d", compensationSetInt32, r.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.PrevLSN() != prevLSN {
					t.Fatalf("PrevLSNが一致しない: want=%d got=%d", prevLSN, r.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if r.TxNumber() != txnum {
					t.Fatalf("TxNumberが一致しない: want=%d got=%d", txnum, r.TxNumber())
				}
			})
			t.Run("Blkは引数で渡したBlockIdと一致する", func(t *testing.T) {
				if got := r.Blk().FileName(); got != datafile {
					t.Fatalf("Blk.FileName expected %q, got %q", datafile, got)
				}
				if got := r.Blk().Number(); got != 1 {
					t.Fatalf("Blk.Number expected %d, got %d", 1, got)
				}
			})
			t.Run("offsetは引数で渡した値と一致する", func(t *testing.T) {
				if r.offset != offset {
					t.Fatalf("offsetが一致しない: want=%d got=%d", offset, r.offset)
				}
			})
			t.Run("oldValは引数で渡した値と一致する", func(t *testing.T) {
				if r.oldVal != oldVal {
					t.Fatalf("oldValが一致しない: want=%d got=%d", oldVal, r.oldVal)
				}
			})
			t.Run("newValは引数で渡した値と一致する", func(t *testing.T) {
				if r.newVal != newVal {
					t.Fatalf("newValが一致しない: want=%d got=%d", newVal, r.newVal)
				}
			})
			t.Run("UndoNextLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.UndoNextLSN() != undoNextLSN {
					t.Fatalf("UndoNextLSNが一致しない: want=%d got=%d", undoNextLSN, r.UndoNextLSN())
				}
			})
		})

		t.Run("CompensationSetStrRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(10)
				txnum       = int32(99)
				offset      = int32(16)
				undoNextLSN = int32(7)
			)
			oldVal, newVal := "old", "new"

			lsn, err := WriteCompensationSetStrToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetStrToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			r, ok := rec.(*CompensationSetStrRecord)
			if !ok {
				t.Fatalf("CompensationSetStrRecordに変換できない: %T", rec)
			}

			t.Run("OpはcompensationSetStrである", func(t *testing.T) {
				if r.Op() != compensationSetStr {
					t.Fatalf("Opが一致しない: want=%d got=%d", compensationSetStr, r.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.PrevLSN() != prevLSN {
					t.Fatalf("PrevLSNが一致しない: want=%d got=%d", prevLSN, r.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if r.TxNumber() != txnum {
					t.Fatalf("TxNumberが一致しない: want=%d got=%d", txnum, r.TxNumber())
				}
			})
			t.Run("Blkは引数で渡したBlockIdと一致する", func(t *testing.T) {
				if got := r.Blk().FileName(); got != datafile {
					t.Fatalf("Blk.FileName expected %q, got %q", datafile, got)
				}
				if got := r.Blk().Number(); got != 1 {
					t.Fatalf("Blk.Number expected %d, got %d", 1, got)
				}
			})
			t.Run("offsetは引数で渡した値と一致する", func(t *testing.T) {
				if r.offset != offset {
					t.Fatalf("offsetが一致しない: want=%d got=%d", offset, r.offset)
				}
			})
			t.Run("oldValは引数で渡した値と一致する", func(t *testing.T) {
				if r.oldVal != oldVal {
					t.Fatalf("oldValが一致しない: want=%q got=%q", oldVal, r.oldVal)
				}
			})
			t.Run("newValは引数で渡した値と一致する", func(t *testing.T) {
				if r.newVal != newVal {
					t.Fatalf("newValが一致しない: want=%q got=%q", newVal, r.newVal)
				}
			})
			t.Run("UndoNextLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.UndoNextLSN() != undoNextLSN {
					t.Fatalf("UndoNextLSNが一致しない: want=%d got=%d", undoNextLSN, r.UndoNextLSN())
				}
			})
		})

		t.Run("CompensationSetBoolRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(10)
				txnum       = int32(99)
				offset      = int32(16)
				oldVal      = false
				newVal      = true
				undoNextLSN = int32(7)
			)

			lsn, err := WriteCompensationSetBoolToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetBoolToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			r, ok := rec.(*CompensationSetBoolRecord)
			if !ok {
				t.Fatalf("CompensationSetBoolRecordに変換できない: %T", rec)
			}

			t.Run("OpはcompensationSetBoolである", func(t *testing.T) {
				if r.Op() != compensationSetBool {
					t.Fatalf("Opが一致しない: want=%d got=%d", compensationSetBool, r.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.PrevLSN() != prevLSN {
					t.Fatalf("PrevLSNが一致しない: want=%d got=%d", prevLSN, r.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if r.TxNumber() != txnum {
					t.Fatalf("TxNumberが一致しない: want=%d got=%d", txnum, r.TxNumber())
				}
			})
			t.Run("Blkは引数で渡したBlockIdと一致する", func(t *testing.T) {
				if got := r.Blk().FileName(); got != datafile {
					t.Fatalf("Blk.FileName expected %q, got %q", datafile, got)
				}
				if got := r.Blk().Number(); got != 1 {
					t.Fatalf("Blk.Number expected %d, got %d", 1, got)
				}
			})
			t.Run("offsetは引数で渡した値と一致する", func(t *testing.T) {
				if r.offset != offset {
					t.Fatalf("offsetが一致しない: want=%d got=%d", offset, r.offset)
				}
			})
			t.Run("oldValは引数で渡した値と一致する", func(t *testing.T) {
				if r.oldVal != oldVal {
					t.Fatalf("oldValが一致しない: want=%t got=%t", oldVal, r.oldVal)
				}
			})
			t.Run("newValは引数で渡した値と一致する", func(t *testing.T) {
				if r.newVal != newVal {
					t.Fatalf("newValが一致しない: want=%t got=%t", newVal, r.newVal)
				}
			})
			t.Run("UndoNextLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.UndoNextLSN() != undoNextLSN {
					t.Fatalf("UndoNextLSNが一致しない: want=%d got=%d", undoNextLSN, r.UndoNextLSN())
				}
			})
		})

		t.Run("CompensationSetDateRecordを復元できる", func(t *testing.T) {
			_, lm := newLogMgr(t)
			blk := file.NewBlockId(datafile, 1)
			const (
				prevLSN     = int32(10)
				txnum       = int32(99)
				offset      = int32(16)
				undoNextLSN = int32(7)
			)
			oldVal := time.Unix(1_690_000_000, 0).UTC()
			newVal := time.Unix(1_690_000_100, 0).UTC()

			lsn, err := WriteCompensationSetDateToLog(lm, prevLSN, txnum, blk, offset, oldVal, newVal, undoNextLSN)
			if err != nil {
				t.Fatalf("WriteCompensationSetDateToLogが失敗した: %v", err)
			}
			rec := CreateLogRecord(readBack(t, lm, lsn), lsn)
			r, ok := rec.(*CompensationSetDateRecord)
			if !ok {
				t.Fatalf("CompensationSetDateRecordに変換できない: %T", rec)
			}

			t.Run("OpはcompensationSetDateである", func(t *testing.T) {
				if r.Op() != compensationSetDate {
					t.Fatalf("Opが一致しない: want=%d got=%d", compensationSetDate, r.Op())
				}
			})
			t.Run("PrevLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.PrevLSN() != prevLSN {
					t.Fatalf("PrevLSNが一致しない: want=%d got=%d", prevLSN, r.PrevLSN())
				}
			})
			t.Run("TxNumberは引数で渡した値と一致する", func(t *testing.T) {
				if r.TxNumber() != txnum {
					t.Fatalf("TxNumberが一致しない: want=%d got=%d", txnum, r.TxNumber())
				}
			})
			t.Run("Blkは引数で渡したBlockIdと一致する", func(t *testing.T) {
				if got := r.Blk().FileName(); got != datafile {
					t.Fatalf("Blk.FileName expected %q, got %q", datafile, got)
				}
				if got := r.Blk().Number(); got != 1 {
					t.Fatalf("Blk.Number expected %d, got %d", 1, got)
				}
			})
			t.Run("offsetは引数で渡した値と一致する", func(t *testing.T) {
				if r.offset != offset {
					t.Fatalf("offsetが一致しない: want=%d got=%d", offset, r.offset)
				}
			})
			t.Run("oldValは引数で渡した値と一致する", func(t *testing.T) {
				if !r.oldVal.Equal(oldVal) {
					t.Fatalf("oldValが一致しない: want=%v got=%v", oldVal, r.oldVal)
				}
			})
			t.Run("newValは引数で渡した値と一致する", func(t *testing.T) {
				if !r.newVal.Equal(newVal) {
					t.Fatalf("newValが一致しない: want=%v got=%v", newVal, r.newVal)
				}
			})
			t.Run("UndoNextLSNは引数で渡した値と一致する", func(t *testing.T) {
				if r.UndoNextLSN() != undoNextLSN {
					t.Fatalf("UndoNextLSNが一致しない: want=%d got=%d", undoNextLSN, r.UndoNextLSN())
				}
			})
		})
	})
}
