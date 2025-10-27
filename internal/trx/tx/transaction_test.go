package tx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
)

const (
	blocksize = int32(256)
	logfile   = "logfile"
	filename  = "testfile"
	numbuffs  = 10
)

func initMgr(t *testing.T, dir string) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr) {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	fm, err := file.NewFileMgr(dir, blocksize)
	if err != nil {
		t.Fatalf("failed to create FileMgr: %v", err)
	}
	lm, err := log.NewLogMgr(fm, logfile)
	if err != nil {
		t.Fatalf("failed to create LogMgr: %v", err)
	}
	bm := buffer.NewBufferMgr(fm, lm, numbuffs, 10)
	return fm, lm, bm
}

func TestTransaction(t *testing.T) {
	t.Run("NewTransactionMgr", func(t *testing.T) {
		fm, lm, bm := initMgr(t, "")
		wTx := NewTransactionMgr(fm, lm, bm)

		if wTx == nil {
			t.Fatal("NewTransactionMgr returned nil")
		}
		if wTx.bm != bm {
			t.Errorf("expected bm to be set")
		}
		if wTx.fm == nil {
			t.Errorf("expected fm to be set")
		}
		if wTx.recoveryMgr == nil {
			t.Errorf("expected recoveryMgr to be set")
		}
		if wTx.mybuffers == nil {
			t.Errorf("expected mybuffers to be initialized")
		}
		if wTx.txAccess == nil {
			t.Errorf("expected txAccess to be initialized")
		}
		if wTx.txnum != 1 {
			t.Errorf("expected txnum 1, got %d", wTx.txnum)
		}
	})

	t.Run("Pin and Unpin", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		wTx := NewTransactionMgr(fm, lm, bm)

		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		before := bm.Available()
		wTx.Pin(ctx, blk)
		if bm.Available() != before-1 {
			t.Errorf("expected available %d, got %d", before-1, bm.Available())
		}
		buff := wTx.mybuffers.GetBuffer(blk)
		if buff == nil || !buff.IsPinned() {
			t.Errorf("expected buffer pinned for blk")
		}

		wTx.Unpin(ctx, blk)
		if bm.Available() != before {
			t.Errorf("expected available %d, got %d", before, bm.Available())
		}
		if wTx.mybuffers.GetBuffer(blk) != nil {
			t.Errorf("expected buffer to be removed from mybuffers after unpin")
		}
	})

	t.Run("SetInt16 and GetInt16", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		wTx := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)

		const off int32 = 0
		val := int16(1234)
		if err := wTx.SetInt16(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt16 failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx := NewTransactionMgr(fm, lm, bm)
		rTx.Pin(ctx, blk)
		buff := rTx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetInt16(off); got != val {
			t.Errorf("expected page int16 %d, got %d", val, got)
		}
		got, err := rTx.GetInt16(ctx, blk, off)
		if err != nil {
			t.Fatalf("GetInt16 failed: %v", err)
		}
		if got != val {
			t.Errorf("expected GetInt16 %d, got %d", val, got)
		}
		rTx.Commit(ctx)
		rTx.Unpin(ctx, blk)
	})

	t.Run("SetInt32 and GetInt32", func(t *testing.T) {
		ctx := context.Background()

		fm, lm, bm := initMgr(t, "")
		wTx := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)
		const off int32 = 4
		val := int32(987654321)
		if err := wTx.SetInt32(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt32 failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx := NewTransactionMgr(fm, lm, bm)
		rTx.Pin(ctx, blk)
		buff := rTx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetInt32(off); got != val {
			t.Errorf("expected page int32 %d, got %d", val, got)
		}
		if got, err := rTx.GetInt32(ctx, blk, off); err != nil {
			t.Fatalf("GetInt32 failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetInt32 %d, got %d", val, got)
		}
		rTx.Commit(ctx)
		rTx.Unpin(ctx, blk)
	})

	t.Run("SetStr and GetStr", func(t *testing.T) {
		ctx := context.Background()

		fm, lm, bm := initMgr(t, "")
		wTx := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)
		const off int32 = 32
		val := "hello"
		if err := wTx.SetStr(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetStr failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx := NewTransactionMgr(fm, lm, bm)
		rTx.Pin(ctx, blk)
		buff := rTx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetStr(off); got != val {
			t.Errorf("expected page str %q, got %q", val, got)
		}
		if got, err := rTx.GetStr(ctx, blk, off); err != nil {
			t.Fatalf("GetStr failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetStr %q, got %q", val, got)
		}
		rTx.Commit(ctx)
		rTx.Unpin(ctx, blk)
	})

	t.Run("SetBool and GetBool", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		wTx := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)

		const off int32 = 64
		val := true
		if err := wTx.SetBool(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetBool failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx := NewTransactionMgr(fm, lm, bm)
		rTx.Pin(ctx, blk)
		buff := rTx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetBool(off); got != val {
			t.Errorf("expected page bool %v, got %v", val, got)
		}
		if got, err := rTx.GetBool(ctx, blk, off); err != nil {
			t.Fatalf("GetBool failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetBool %v, got %v", val, got)
		}
		rTx.Commit(ctx)
		rTx.Unpin(ctx, blk)
	})

	t.Run("SetDate and GetDate", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		wTx := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)

		const off int32 = 96
		val := time.Unix(1_690_000_000, 0).UTC()
		if err := wTx.SetDate(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetDate failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx := NewTransactionMgr(fm, lm, bm)
		rTx.Pin(ctx, blk)
		buff := rTx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetDate(off); !got.Equal(val) {
			t.Errorf("expected page date %v, got %v", val, got)
		}
		if got, err := rTx.GetDate(ctx, blk, off); err != nil {
			t.Fatalf("GetDate failed: %v", err)
		} else if !got.Equal(val) {
			t.Errorf("expected GetDate %v, got %v", val, got)
		}
		rTx.Commit(ctx)
		rTx.Unpin(ctx, blk)
	})
	t.Run("SetInt16 and GetInt16 in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		txmgr := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 0
		val := int16(1234)
		if err := txmgr.SetInt16(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt16 failed: %v", err)
		}
		buff := txmgr.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetInt16(off); got != val {
			t.Errorf("expected page int16 %d, got %d", val, got)
		}
		if got, err := txmgr.GetInt16(ctx, blk, off); err != nil {
			t.Fatalf("GetInt16 failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetInt16 %d, got %d", val, got)
		}
		txmgr.Commit(ctx)
	})

	// Rollback should undo uncommitted changes
	t.Run("Rollback undoes uncommitted writes", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		wTx := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)

		const off int32 = 4
		before := int32(0)
		// write and then rollback
		if err := wTx.SetInt32(ctx, blk, off, 111, true); err != nil {
			t.Fatalf("SetInt32 failed: %v", err)
		}
		wTx.Rollback(ctx)

		// start a new transaction to read the page
		rTx := NewTransactionMgr(fm, lm, bm)
		rTx.Pin(ctx, blk)
		got, err := rTx.GetInt32(ctx, blk, off)
		if err != nil {
			t.Fatalf("GetInt32 failed: %v", err)
		}
		if got != before {
			t.Errorf("expected value after rollback %d, got %d", before, got)
		}
		rTx.Commit(ctx)
	})

	// Recover should undo losers and keep winners
	t.Run("Recover undoes losers and keeps committed", func(t *testing.T) {
		ctx := context.Background()

		// Use a fixed directory to simulate crash/restart using same disk
		dir := t.TempDir()
		fm, lm, bm := initMgr(t, dir)

		// tx1 writes and commits
		wTx1 := NewTransactionMgr(fm, lm, bm)
		blk1, err := fm.Append("recover_keep")
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx1.Pin(ctx, blk1)
		const off1 int32 = 0
		val1 := int32(777)
		if err := wTx1.SetInt32(ctx, blk1, off1, val1, true); err != nil {
			t.Fatalf("tx1 SetInt32 failed: %v", err)
		}
		wTx1.Commit(ctx)

		// tx2 writes but does NOT commit (loser)
		wTx2 := NewTransactionMgr(fm, lm, bm)
		blk2, err := fm.Append("recover_undo")
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx2.Pin(ctx, blk2)
		const off2 int32 = 4
		val2 := int32(999)
		if err := wTx2.SetInt32(ctx, blk2, off2, val2, true); err != nil {
			t.Fatalf("tx2 SetInt32 failed: %v", err)
		}
		// no commit for tx2; simulate crash
		// Reset in-memory global lock table as a reboot would
		concurrency.LockTbl = concurrency.NewLockTable()
		// Recreate managers pointing to same directory (fresh process state)
		fm, lm, bm = initMgr(t, dir)

		// system recovery
		recoverTxMgr := NewTransactionMgr(fm, lm, bm)
		recoverTxMgr.Recover(ctx)

		// verify committed value remains
		rTx1 := NewTransactionMgr(fm, lm, bm)
		rTx1.Pin(ctx, blk1)
		if got, err := rTx1.GetInt32(ctx, blk1, off1); err != nil {
			t.Fatalf("GetInt32 failed: %v", err)
		} else if got != val1 {
			t.Errorf("expected committed value %d, got %d", val1, got)
		}
		rTx1.Commit(ctx)

		// verify loser write undone (expect 0)
		rTx2 := NewTransactionMgr(fm, lm, bm)
		rTx2.Pin(ctx, blk2)
		if got, err := rTx2.GetInt32(ctx, blk2, off2); err != nil {
			t.Fatalf("GetInt32 failed: %v", err)
		} else if got != 0 {
			t.Errorf("expected undone value %d, got %d", 0, got)
		}
		rTx2.Commit(ctx)
	})

	t.Run("SetInt32 and GetInt32 in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		txmgr := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 4
		val := int32(987654321)
		if err := txmgr.SetInt32(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt32 failed: %v", err)
		}
		buff := txmgr.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetInt32(off); got != val {
			t.Errorf("expected page int32 %d, got %d", val, got)
		}
		if got, err := txmgr.GetInt32(ctx, blk, off); err != nil {
			t.Fatalf("GetInt32 failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetInt32 %d, got %d", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("SetStr and GetStr in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		txmgr := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 32
		val := "hello"
		if err := txmgr.SetStr(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetStr failed: %v", err)
		}
		buff := txmgr.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetStr(off); got != val {
			t.Errorf("expected page str %q, got %q", val, got)
		}
		if got, err := txmgr.GetStr(ctx, blk, off); err != nil {
			t.Fatalf("GetStr failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetStr %q, got %q", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("SetBool and GetBool in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		txmgr := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 64
		val := true
		if err := txmgr.SetBool(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetBool failed: %v", err)
		}
		buff := txmgr.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetBool(off); got != val {
			t.Errorf("expected page bool %v, got %v", val, got)
		}
		if got, err := txmgr.GetBool(ctx, blk, off); err != nil {
			t.Fatalf("GetBool failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetBool %v, got %v", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("SetDate and GetDate in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		txmgr := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 96
		val := time.Unix(1_690_000_000, 0).UTC()
		if err := txmgr.SetDate(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetDate failed: %v", err)
		}
		buff := txmgr.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetDate(off); !got.Equal(val) {
			t.Errorf("expected page date %v, got %v", val, got)
		}
		if got, err := txmgr.GetDate(ctx, blk, off); err != nil {
			t.Fatalf("GetDate failed: %v", err)
		} else if !got.Equal(val) {
			t.Errorf("expected GetDate %v, got %v", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("Commit releases resources", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t, "")
		txmgr := NewTransactionMgr(fm, lm, bm)

		blk, err := fm.Append("testfile_tx_commit")
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		before := bm.Available()
		txmgr.Pin(ctx, blk)
		if bm.Available() != before-1 {
			t.Fatalf("expected available %d after pin, got %d", before-1, bm.Available())
		}

		// Perform a write to exercise logging path
		if err := txmgr.SetInt32(ctx, blk, 4, 42, true); err != nil {
			t.Fatalf("SetInt32 failed: %v", err)
		}

		// Commit should release locks and unpin all buffers
		txmgr.Commit(ctx)
		if bm.Available() != before {
			t.Errorf("expected available %d after commit, got %d", before, bm.Available())
		}
		if txmgr.mybuffers.GetBuffer(blk) != nil {
			t.Errorf("expected mybuffers to be empty after commit")
		}
	})

	// Concurrency conflict tests
	t.Run("Read vs Write conflict times out", func(t *testing.T) {
		// reader holds SLock, writer attempts XLock and should time out
		fm, lm, bm := initMgr(t, "")
		ctx := context.Background()
		blk, err := fm.Append("conflict_rw")
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		rTx := NewTransactionMgr(fm, lm, bm)
		rTx.Pin(ctx, blk)
		if _, err := rTx.GetInt32(ctx, blk, 0); err != nil { // acquire SLock
			t.Fatalf("reader GetInt32 failed: %v", err)
		}

		wTx := NewTransactionMgr(fm, lm, bm)
		wTx.Pin(ctx, blk)

		// writer should block on XLock and then time out quickly
		wctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		if err := wTx.SetInt32(wctx, blk, 0, 1, true); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected writer timeout (DeadlineExceeded), got %v", err)
		}
		rTx.Commit(ctx)   // cleanup
		wTx.Rollback(ctx) // cleanup
	})

	t.Run("Write vs Write conflict times out", func(t *testing.T) {
		// first writer holds XLock, second writer should time out
		fm, lm, bm := initMgr(t, "")
		blk, err := fm.Append("conflict_ww")
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		wctx1 := context.Background()
		wTx1 := NewTransactionMgr(fm, lm, bm)
		wTx1.Pin(wctx1, blk)
		if err := wTx1.SetInt32(wctx1, blk, 0, 100, true); err != nil { // acquire XLock
			t.Fatalf("writer1 SetInt32 failed: %v", err)
		}

		wctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel2()
		wTx2 := NewTransactionMgr(fm, lm, bm)
		wTx2.Pin(wctx2, blk)

		if err := wTx2.SetInt32(wctx2, blk, 0, 200, true); err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected writer1 timeout (DeadlineExceeded), got %v", err)
		}

		// cleanup
		wTx1.Commit(wctx1)
		wTx2.Rollback(wctx2)
	})

	t.Run("Upgrade conflict times out (S->X vs S->X)", func(t *testing.T) {
		// both transactions hold SLock and try to upgrade to XLock; at least one should time out
		fm, lm, bm := initMgr(t, "")
		ctx := context.Background()
		blk, err := fm.Append("conflict_upgrade")
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		tx1 := NewTransactionMgr(fm, lm, bm)
		tx1.Pin(ctx, blk)
		if _, err := tx1.GetInt32(ctx, blk, 0); err != nil { // acquire SLock
			t.Fatalf("tx1 GetInt32 failed: %v", err)
		}

		tx2 := NewTransactionMgr(fm, lm, bm)
		tx2.Pin(ctx, blk)
		if _, err := tx2.GetInt32(ctx, blk, 0); err != nil { // acquire SLock
			t.Fatalf("tx2 GetInt32 failed: %v", err)
		}

		wctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel1()
		err1 := tx1.SetInt32(wctx1, blk, 0, 1, true)

		wctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel2()
		err2 := tx2.SetInt32(wctx2, blk, 0, 2, true)

		// In this implementation both may time out; assert both time out to be strict
		if err1 == nil || !errors.Is(err1, context.DeadlineExceeded) {
			t.Fatalf("expected tx1 timeout (DeadlineExceeded), got %v", err1)
		}
		if err2 == nil || !errors.Is(err2, context.DeadlineExceeded) {
			t.Fatalf("expected tx2 timeout (DeadlineExceeded), got %v", err2)
		}

		// cleanup
		tx1.Rollback(ctx)
		tx2.Rollback(ctx)
	})

	t.Run("Deadlock causes timeout abort in one transaction", func(t *testing.T) {
		// tx1: S blk1 -> X blk2, tx2: S blk2 -> X blk1; at least one should time out
		fm, lm, bm := initMgr(t, "")
		ctx := context.Background()
		blk1, err := fm.Append("deadlock_blk1")
		if err != nil {
			t.Fatalf("append blk1 failed: %v", err)
		}
		blk2, err := fm.Append("deadlock_blk2")
		if err != nil {
			t.Fatalf("append blk2 failed: %v", err)
		}

		tx1 := NewTransactionMgr(fm, lm, bm)
		tx2 := NewTransactionMgr(fm, lm, bm)
		tx1.Pin(ctx, blk1)
		tx1.Pin(ctx, blk2)
		tx2.Pin(ctx, blk1)
		tx2.Pin(ctx, blk2)

		if _, err := tx1.GetInt32(ctx, blk1, 0); err != nil { // S blk1
			t.Fatalf("tx1 SLock blk1 failed: %v", err)
		}
		if _, err := tx2.GetInt32(ctx, blk2, 0); err != nil { // S blk2
			t.Fatalf("tx2 SLock blk2 failed: %v", err)
		}

		wctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel1()
		wctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel2()

		err1 := tx1.SetInt32(wctx1, blk2, 0, 10, true)
		err2 := tx2.SetInt32(wctx2, blk1, 0, 20, true)

		if (err1 == nil || !errors.Is(err1, context.DeadlineExceeded)) && (err2 == nil || !errors.Is(err2, context.DeadlineExceeded)) {
			t.Fatalf("expected at least one timeout (DeadlineExceeded), got err1=%v err2=%v", err1, err2)
		}

		// cleanup
		tx1.Rollback(ctx)
		tx2.Rollback(ctx)
	})

	// Deadlock resolved by rollback (upgrade vs upgrade)
	t.Run("Deadlock upgrade resolved by rollback", func(t *testing.T) {
		fm, lm, bm := initMgr(t, "")
		ctx := context.Background()
		blk, err := fm.Append("deadlock_upgrade_blk")
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		tx1 := NewTransactionMgr(fm, lm, bm)
		tx2 := NewTransactionMgr(fm, lm, bm)
		tx1.Pin(ctx, blk)
		tx2.Pin(ctx, blk)

		// both acquire S
		if _, err := tx1.GetInt32(ctx, blk, 0); err != nil {
			t.Fatalf("tx1 SLock failed: %v", err)
		}
		if _, err := tx2.GetInt32(ctx, blk, 0); err != nil {
			t.Fatalf("tx2 SLock failed: %v", err)
		}

		var err1 error
		var wg sync.WaitGroup
		wg.Add(2)

		// tx1 tries to upgrade and should eventually succeed
		go func() {
			defer wg.Done()
			wctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel1()
			err1 = tx1.SetInt32(wctx1, blk, 0, 111, true)
		}()

		// tx2 tries to upgrade, times out quickly, then rolls back in the same goroutine
		go func() {
			defer wg.Done()
			wctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel2()
			_ = tx2.SetInt32(wctx2, blk, 0, 222, true)
			tx2.Rollback(ctx)
		}()

		wg.Wait()

		if err1 != nil {
			t.Fatalf("expected tx1 to succeed after tx2 rollback, got %v", err1)
		}
		tx1.Commit(ctx)
	})

	// Deadlock resolved by rollback (read/write across two blocks)
	t.Run("Deadlock read/write resolved by rollback", func(t *testing.T) {
		fm, lm, bm := initMgr(t, "")
		ctx := context.Background()
		blk1, err := fm.Append("deadlock_rw_blk1")
		if err != nil {
			t.Fatalf("append blk1 failed: %v", err)
		}
		blk2, err := fm.Append("deadlock_rw_blk2")
		if err != nil {
			t.Fatalf("append blk2 failed: %v", err)
		}

		tx1 := NewTransactionMgr(fm, lm, bm)
		tx2 := NewTransactionMgr(fm, lm, bm)
		tx1.Pin(ctx, blk1)
		tx1.Pin(ctx, blk2)
		tx2.Pin(ctx, blk1)
		tx2.Pin(ctx, blk2)

		// S on different blocks
		if _, err := tx1.GetInt32(ctx, blk1, 0); err != nil {
			t.Fatalf("tx1 S blk1 failed: %v", err)
		}
		if _, err := tx2.GetInt32(ctx, blk2, 0); err != nil {
			t.Fatalf("tx2 S blk2 failed: %v", err)
		}

		var err1 error
		var wg sync.WaitGroup
		wg.Add(2)

		go func() { // tx1 tries X on blk2 and should succeed after tx2 rollback
			defer wg.Done()
			wctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel1()
			err1 = tx1.SetInt32(wctx1, blk2, 0, 10, true)
		}()

		go func() { // tx2 tries X on blk1, times out and rolls back
			defer wg.Done()
			wctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel2()
			_ = tx2.SetInt32(wctx2, blk1, 0, 20, true)
			tx2.Rollback(ctx)
		}()

		wg.Wait()

		if err1 != nil {
			t.Fatalf("expected tx1 to proceed after tx2 rollback, got %v", err1)
		}
		tx1.Commit(ctx)
	})

	// Deadlock resolved by rollback (write/write across two blocks)
	t.Run("Deadlock write/write resolved by rollback", func(t *testing.T) {
		fm, lm, bm := initMgr(t, "")
		ctx := context.Background()
		blk1, err := fm.Append("deadlock_ww_blk1")
		if err != nil {
			t.Fatalf("append blk1 failed: %v", err)
		}
		blk2, err := fm.Append("deadlock_ww_blk2")
		if err != nil {
			t.Fatalf("append blk2 failed: %v", err)
		}

		tx1 := NewTransactionMgr(fm, lm, bm)
		tx2 := NewTransactionMgr(fm, lm, bm)
		tx1.Pin(ctx, blk1)
		tx1.Pin(ctx, blk2)
		tx2.Pin(ctx, blk1)
		tx2.Pin(ctx, blk2)

		// X on different blocks
		if err := tx1.SetInt32(ctx, blk1, 0, 100, true); err != nil {
			t.Fatalf("tx1 X blk1 failed: %v", err)
		}
		if err := tx2.SetInt32(ctx, blk2, 0, 200, true); err != nil {
			t.Fatalf("tx2 X blk2 failed: %v", err)
		}

		var err1 error
		var wg sync.WaitGroup
		wg.Add(2)

		go func() { // tx1 tries X on blk2 and should succeed after tx2 rollback
			defer wg.Done()
			wctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel1()
			err1 = tx1.SetInt32(wctx1, blk2, 4, 101, true)
		}()

		go func() { // tx2 tries X on blk1, times out and rolls back
			defer wg.Done()
			wctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel2()
			_ = tx2.SetInt32(wctx2, blk1, 4, 201, true)
			tx2.Rollback(ctx)
		}()

		wg.Wait()

		if err1 != nil {
			t.Fatalf("expected tx1 to proceed after tx2 rollback, got %v", err1)
		}
		tx1.Commit(ctx)
	})
}
