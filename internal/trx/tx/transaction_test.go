package tx

import (
	"context"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

const (
	blocksize = int32(256)
	logfile   = "logfile"
	filename  = "testfile"
	numbuffs  = 10
)

func initMgr(t *testing.T) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr) {
	t.Helper()
	fm, err := file.NewFileMgr(t.TempDir(), blocksize)
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
		fm, lm, bm := initMgr(t)
		txmgr := NewTransactionMgr(fm, lm, bm)

		if txmgr == nil {
			t.Fatal("NewTransactionMgr returned nil")
		}
		if txmgr.bm != bm {
			t.Errorf("expected bm to be set")
		}
		if txmgr.fm == nil {
			t.Errorf("expected fm to be set")
		}
		if txmgr.recoveryMgr == nil {
			t.Errorf("expected recoveryMgr to be set")
		}
		if txmgr.mybuffers == nil {
			t.Errorf("expected mybuffers to be initialized")
		}
		if txmgr.txAccess == nil {
			t.Errorf("expected txAccess to be initialized")
		}
		if txmgr.txnum != 1 {
			t.Errorf("expected txnum 1, got %d", txmgr.txnum)
		}
	})

	t.Run("Pin and Unpin", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
		txmgr := NewTransactionMgr(fm, lm, bm)

		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		before := bm.Available()
		txmgr.Pin(ctx, blk)
		if bm.Available() != before-1 {
			t.Errorf("expected available %d, got %d", before-1, bm.Available())
		}
		buff := txmgr.mybuffers.GetBuffer(blk)
		if buff == nil || !buff.IsPinned() {
			t.Errorf("expected buffer pinned for blk")
		}

		txmgr.Unpin(ctx, blk)
		if bm.Available() != before {
			t.Errorf("expected available %d, got %d", before, bm.Available())
		}
		if txmgr.mybuffers.GetBuffer(blk) != nil {
			t.Errorf("expected buffer to be removed from mybuffers after unpin")
		}
	})

	t.Run("SetInt16 and GetInt16", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
		txmgr1 := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr1.Pin(ctx, blk)

		const off int32 = 0
		val := int16(1234)
		if err := txmgr1.SetInt16(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt16 failed: %v", err)
		}
		txmgr1.Commit(ctx)
		txmgr1.Unpin(ctx, blk)

		txmgr2 := NewTransactionMgr(fm, lm, bm)
		txmgr2.Pin(ctx, blk)
		buff := txmgr2.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetInt16(off); got != val {
			t.Errorf("expected page int16 %d, got %d", val, got)
		}
		if got := txmgr2.GetInt16(ctx, blk, off); got != val {
			t.Errorf("expected GetInt16 %d, got %d", val, got)
		}
		txmgr2.Commit(ctx)
		txmgr2.Unpin(ctx, blk)
	})

	t.Run("SetInt32 and GetInt32", func(t *testing.T) {
		ctx := context.Background()

		fm, lm, bm := initMgr(t)
		txmgr1 := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr1.Pin(ctx, blk)
		const off int32 = 4
		val := int32(987654321)
		if err := txmgr1.SetInt32(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt32 failed: %v", err)
		}
		txmgr1.Commit(ctx)
		txmgr1.Unpin(ctx, blk)

		txmgr2 := NewTransactionMgr(fm, lm, bm)
		txmgr2.Pin(ctx, blk)
		buff := txmgr2.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetInt32(off); got != val {
			t.Errorf("expected page int32 %d, got %d", val, got)
		}
		if got := txmgr2.GetInt32(ctx, blk, off); got != val {
			t.Errorf("expected GetInt32 %d, got %d", val, got)
		}
		txmgr2.Commit(ctx)
		txmgr2.Unpin(ctx, blk)
	})

	t.Run("SetStr and GetStr", func(t *testing.T) {
		ctx := context.Background()

		fm, lm, bm := initMgr(t)
		txmgr1 := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr1.Pin(ctx, blk)
		const off int32 = 32
		val := "hello"
		if err := txmgr1.SetStr(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetStr failed: %v", err)
		}
		txmgr1.Commit(ctx)
		txmgr1.Unpin(ctx, blk)

		txmgr2 := NewTransactionMgr(fm, lm, bm)
		txmgr2.Pin(ctx, blk)
		buff := txmgr2.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetStr(off); got != val {
			t.Errorf("expected page str %q, got %q", val, got)
		}
		if got := txmgr2.GetStr(ctx, blk, off); got != val {
			t.Errorf("expected GetStr %q, got %q", val, got)
		}
		txmgr2.Commit(ctx)
		txmgr2.Unpin(ctx, blk)
	})

	t.Run("SetBool and GetBool", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
		txmgr1 := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr1.Pin(ctx, blk)

		const off int32 = 64
		val := true
		if err := txmgr1.SetBool(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetBool failed: %v", err)
		}
		txmgr1.Commit(ctx)
		txmgr1.Unpin(ctx, blk)

		txmgr2 := NewTransactionMgr(fm, lm, bm)
		txmgr2.Pin(ctx, blk)
		buff := txmgr2.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetBool(off); got != val {
			t.Errorf("expected page bool %v, got %v", val, got)
		}
		if got := txmgr2.GetBool(ctx, blk, off); got != val {
			t.Errorf("expected GetBool %v, got %v", val, got)
		}
		txmgr2.Commit(ctx)
		txmgr2.Unpin(ctx, blk)
	})

	t.Run("SetDate and GetDate", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
		txmgr1 := NewTransactionMgr(fm, lm, bm)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr1.Pin(ctx, blk)

		const off int32 = 96
		val := time.Unix(1_690_000_000, 0).UTC()
		if err := txmgr1.SetDate(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetDate failed: %v", err)
		}
		txmgr1.Commit(ctx)
		txmgr1.Unpin(ctx, blk)

		txmgr2 := NewTransactionMgr(fm, lm, bm)
		txmgr2.Pin(ctx, blk)
		buff := txmgr2.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetDate(off); !got.Equal(val) {
			t.Errorf("expected page date %v, got %v", val, got)
		}
		if got := txmgr2.GetDate(ctx, blk, off); !got.Equal(val) {
			t.Errorf("expected GetDate %v, got %v", val, got)
		}
		txmgr2.Commit(ctx)
		txmgr2.Unpin(ctx, blk)
	})
	t.Run("SetInt16 and GetInt16 in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
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
		if got := txmgr.GetInt16(ctx, blk, off); got != val {
			t.Errorf("expected GetInt16 %d, got %d", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("SetInt32 and GetInt32 in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
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
		if got := txmgr.GetInt32(ctx, blk, off); got != val {
			t.Errorf("expected GetInt32 %d, got %d", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("SetStr and GetStr in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
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
		if got := txmgr.GetStr(ctx, blk, off); got != val {
			t.Errorf("expected GetStr %q, got %q", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("SetBool and GetBool in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
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
		if got := txmgr.GetBool(ctx, blk, off); got != val {
			t.Errorf("expected GetBool %v, got %v", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("SetDate and GetDate in same transaction", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
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
		if got := txmgr.GetDate(ctx, blk, off); !got.Equal(val) {
			t.Errorf("expected GetDate %v, got %v", val, got)
		}
		txmgr.Commit(ctx)
	})

	t.Run("Commit releases resources", func(t *testing.T) {
		ctx := context.Background()
		fm, lm, bm := initMgr(t)
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
}
