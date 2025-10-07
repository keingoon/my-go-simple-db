package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

const (
	blocksize = int32(256)
	logfile   = "logfile"
	filename  = "testfile"
	numbuffs  = 10
	numwaits  = 10
)

func initRecoveryEnv(t *testing.T, txnum int32) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *access.Transaction, *RecoveryMgr) {
	t.Helper()
	fm, err := file.NewFileMgr(t.TempDir(), blocksize)
	if err != nil {
		t.Fatalf("failed to create FileMgr: %v", err)
	}
	lm, err := log.NewLogMgr(fm, logfile)
	if err != nil {
		t.Fatalf("failed to create LogMgr: %v", err)
	}
	bm := buffer.NewBufferMgr(fm, lm, numbuffs, numwaits)
	tx := access.NewTransaction(fm, lm, bm, txnum, nil)
	rm := NewRecoveryMgr(lm, bm, tx, txnum)
	return fm, lm, bm, tx, rm
}

func logHasOpForTx(lm *log.LogMgr, op int32, txnum int32) bool {
	iter := lm.Iterater()
	for iter.HasNext() {
		rec := CreateLogRecord(iter.Next())
		if rec.Op() == op && rec.TxNumber() == txnum {
			return true
		}
	}
	return false
}

func logHasOp(lm *log.LogMgr, op int32) bool {
	iter := lm.Iterater()
	for iter.HasNext() {
		rec := CreateLogRecord(iter.Next())
		if rec.Op() == op {
			return true
		}
	}
	return false
}

func TestRecoveryMgr(t *testing.T) {
	t.Run("NewRecoveryMgr writes START", func(t *testing.T) {
		_, lm, _, _, rm := initRecoveryEnv(t, 1)
		if rm == nil {
			t.Fatal("recovery mgr is nil")
		}
		if !logHasOpForTx(lm, start, 1) {
			t.Errorf("expected START record for tx %d", 1)
		}
	})

	t.Run("Commit writes COMMIT record", func(t *testing.T) {
		ctx := context.Background()
		_, lm, _, _, rm := initRecoveryEnv(t, 2)
		if err := rm.Commit(ctx); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		if !logHasOpForTx(lm, commit, 2) {
			t.Errorf("expected COMMIT record for tx %d", 2)
		}
	})

	t.Run("Rollback undoes all Set* and writes ROLLBACK", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm, _, rm := initRecoveryEnv(t, 3)

		// Use separate blocks for each Set* to avoid XLock conflicts during undo
		blk16, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		blk32, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		blkStr, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		blkBool, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		blkDate, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		buff16, err := bm.Pin(ctx, blk16)
		if err != nil {
			t.Fatalf("pin16 failed: %v", err)
		}
		buff32, err := bm.Pin(ctx, blk32)
		if err != nil {
			t.Fatalf("pin32 failed: %v", err)
		}
		buffStr, err := bm.Pin(ctx, blkStr)
		if err != nil {
			t.Fatalf("pinStr failed: %v", err)
		}
		buffBool, err := bm.Pin(ctx, blkBool)
		if err != nil {
			t.Fatalf("pinBool failed: %v", err)
		}
		buffDate, err := bm.Pin(ctx, blkDate)
		if err != nil {
			t.Fatalf("pinDate failed: %v", err)
		}

		// Original values and offsets
		const off16 int32 = 0
		const off32 int32 = 4
		const offStr int32 = 32
		const offBool int32 = 64
		const offDate int32 = 96
		orig16 := int16(123)
		orig32 := int32(456789)
		origStr := "orig"
		origBool := true
		origDate := time.Unix(1_690_000_000, 0).UTC()

		p16 := buff16.Contents()
		if err := p16.SetInt16(off16, orig16); err != nil {
			t.Fatal(err)
		}
		p32 := buff32.Contents()
		if err := p32.SetInt32(off32, orig32); err != nil {
			t.Fatal(err)
		}
		pStr := buffStr.Contents()
		if err := pStr.SetStr(offStr, origStr); err != nil {
			t.Fatal(err)
		}
		pBool := buffBool.Contents()
		if err := pBool.SetBool(offBool, origBool); err != nil {
			t.Fatal(err)
		}
		pDate := buffDate.Contents()
		if err := pDate.SetDate(offDate, origDate); err != nil {
			t.Fatal(err)
		}

		// Log with Set* (LSNs should be > 0)
		if lsn, err := rm.SetInt16(buff16, off16, 999); err != nil || lsn <= 0 {
			t.Fatalf("SetInt16 log failed: lsn=%d err=%v", lsn, err)
		}
		if lsn, err := rm.SetInt32(buff32, off32, 888); err != nil || lsn <= 0 {
			t.Fatalf("SetInt32 log failed: lsn=%d err=%v", lsn, err)
		}
		if lsn, err := rm.SetStr(buffStr, offStr, "after"); err != nil || lsn <= 0 {
			t.Fatalf("SetStr log failed: lsn=%d err=%v", lsn, err)
		}
		if lsn, err := rm.SetBool(buffBool, offBool, !origBool); err != nil || lsn <= 0 {
			t.Fatalf("SetBool log failed: lsn=%d err=%v", lsn, err)
		}
		if lsn, err := rm.SetDate(buffDate, offDate, time.Unix(0, 0).UTC()); err != nil || lsn <= 0 {
			t.Fatalf("SetDate log failed: lsn=%d err=%v", lsn, err)
		}

		// Overwrite values to verify Undo restores originals
		p16.SetInt16(off16, 999)
		p32.SetInt32(off32, 888)
		pStr.SetStr(offStr, "after")
		pBool.SetBool(offBool, !origBool)
		pDate.SetDate(offDate, time.Unix(0, 0).UTC())

		if err := rm.Rollback(ctx); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		// Expect originals restored
		if got := p16.GetInt16(off16); got != orig16 {
			t.Errorf("int16 expected %d got %d", orig16, got)
		}
		if got := p32.GetInt32(off32); got != orig32 {
			t.Errorf("int32 expected %d got %d", orig32, got)
		}
		if got := pStr.GetStr(offStr); got != origStr {
			t.Errorf("str expected %q got %q", origStr, got)
		}
		if got := pBool.GetBool(offBool); got != origBool {
			t.Errorf("bool expected %v got %v", origBool, got)
		}
		if got := pDate.GetDate(offDate); !got.Equal(origDate) {
			t.Errorf("date expected %v got %v", origDate, got)
		}

		if !logHasOpForTx(lm, rollback, 3) {
			t.Errorf("expected ROLLBACK record for tx %d", 3)
		}
	})

	t.Run("Recover undoes uncommitted and writes CHECKPOINT", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm, _, rm := initRecoveryEnv(t, 4)

		// Use separate block to avoid conflicts with other potential XLocks
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		buff, err := bm.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("pin failed: %v", err)
		}
		p := buff.Contents()
		const off32 int32 = 8
		orig := int32(13579)
		if err := p.SetInt32(off32, orig); err != nil {
			t.Fatal(err)
		}
		if lsn, err := rm.SetInt32(buff, off32, 24680); err != nil || lsn <= 0 {
			t.Fatalf("SetInt32 log failed: lsn=%d err=%v", lsn, err)
		}
		p.SetInt32(off32, 24680)

		if err := rm.Recover(ctx); err != nil {
			t.Fatalf("Recover failed: %v", err)
		}

		if got := p.GetInt32(off32); got != orig {
			t.Errorf("int32 expected %d got %d", orig, got)
		}
		if !logHasOp(lm, checkpoint) {
			t.Errorf("expected CHECKPOINT record")
		}
	})
}
