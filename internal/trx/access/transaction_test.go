package access

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
	numbuffs  = 5
	numwaits  = 5
)

func initTx(t *testing.T, txnum int32) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *Transaction) {
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

	// mybuffers arg is ignored by NewTransaction; it creates its own BufferList
	tx := NewTransaction(fm, lm, bm, txnum, nil)
	return fm, lm, bm, tx
}

func TestTransaction(t *testing.T) {
	t.Parallel()

	t.Run("NewTransaction", func(t *testing.T) {
		t.Parallel()
		_, _, bm, tx := initTx(t, 1)

		if tx == nil {
			t.Fatal("NewTransaction returned nil")
		}
		if tx.bm != bm {
			t.Errorf("expected bm to be set")
		}
		if tx.fm == nil {
			t.Errorf("expected fm to be set")
		}
		if tx.concurMgr == nil {
			t.Errorf("expected concurMgr to be set")
		}
		if tx.mybuffers == nil {
			t.Errorf("expected mybuffers to be initialized")
		}
		if tx.txnum != 1 {
			t.Errorf("expected txnum 1, got %d", tx.txnum)
		}
	})

	t.Run("Pin and Unpin", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fm, _, bm, tx := initTx(t, 1)

		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		before := bm.Available()
		tx.Pin(ctx, blk)
		if bm.Available() != before-1 {
			t.Errorf("expected available %d, got %d", before-1, bm.Available())
		}
		buff := tx.mybuffers.GetBuffer(blk)
		if buff == nil || !buff.IsPinned() {
			t.Errorf("expected buffer pinned for blk")
		}

		tx.Unpin(ctx, blk)
		if bm.Available() != before {
			t.Errorf("expected available %d, got %d", before, bm.Available())
		}
		if tx.mybuffers.GetBuffer(blk) != nil {
			t.Errorf("expected buffer to be removed from mybuffers after unpin")
		}
	})

	t.Run("SetInt16 and GetInt16", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fm, _, _, tx := initTx(t, 2)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		tx.Pin(ctx, blk)

		var loggedLSN int32 = 7
		logFn := func(buff *buffer.Buffer, offset int32, oldVal int16) (int32, error) {
			return loggedLSN, nil
		}
		const off int32 = 0
		var val int16 = 1234
		if err := tx.SetInt16(ctx, blk, off, val, true, logFn); err != nil {
			t.Fatalf("SetInt16 failed: %v", err)
		}
		buff := tx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetInt16(off); got != val {
			t.Errorf("expected page int16 %d, got %d", val, got)
		}
		if got := tx.GetInt16(ctx, blk, off); got != val {
			t.Errorf("expected GetInt16 %d, got %d", val, got)
		}
		if buff.ModifyingTx() != tx.txnum {
			t.Errorf("expected modifying tx %d, got %d", tx.txnum, buff.ModifyingTx())
		}
	})

	t.Run("SetInt32 and GetInt32", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fm, _, _, tx := initTx(t, 3)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		tx.Pin(ctx, blk)

		var loggedLSN int32 = 11
		logFn := func(buff *buffer.Buffer, offset int32, oldVal int32) (int32, error) {
			return loggedLSN, nil
		}
		const off int32 = 4
		var val int32 = 987654321
		if err := tx.SetInt32(ctx, blk, off, val, true, logFn); err != nil {
			t.Fatalf("SetInt32 failed: %v", err)
		}
		buff := tx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetInt32(off); got != val {
			t.Errorf("expected page int32 %d, got %d", val, got)
		}
		if got := tx.GetInt32(ctx, blk, off); got != val {
			t.Errorf("expected GetInt32 %d, got %d", val, got)
		}
	})

	t.Run("SetStr and GetStr", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fm, _, _, tx := initTx(t, 4)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		tx.Pin(ctx, blk)

		var loggedLSN int32 = 5
		logFn := func(buff *buffer.Buffer, offset int32, oldVal string) (int32, error) {
			return loggedLSN, nil
		}
		const off int32 = 32
		val := "hello"
		if err := tx.SetStr(ctx, blk, off, val, true, logFn); err != nil {
			t.Fatalf("SetStr failed: %v", err)
		}
		buff := tx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetStr(off); got != val {
			t.Errorf("expected page str %q, got %q", val, got)
		}
		if got := tx.GetStr(ctx, blk, off); got != val {
			t.Errorf("expected GetStr %q, got %q", val, got)
		}
	})

	t.Run("SetBool and GetBool", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fm, _, _, tx := initTx(t, 5)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		tx.Pin(ctx, blk)

		var loggedLSN int32 = 9
		logFn := func(buff *buffer.Buffer, offset int32, oldVal bool) (int32, error) {
			return loggedLSN, nil
		}
		const off int32 = 64
		val := true
		if err := tx.SetBool(ctx, blk, off, val, true, logFn); err != nil {
			t.Fatalf("SetBool failed: %v", err)
		}
		buff := tx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetBool(off); got != val {
			t.Errorf("expected page bool %v, got %v", val, got)
		}
		if got := tx.GetBool(ctx, blk, off); got != val {
			t.Errorf("expected GetBool %v, got %v", val, got)
		}
	})

	t.Run("SetDate and GetDate", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		fm, _, _, tx := initTx(t, 6)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		tx.Pin(ctx, blk)

		var loggedLSN int32 = 13
		logFn := func(buff *buffer.Buffer, offset int32, oldVal time.Time) (int32, error) {
			return loggedLSN, nil
		}
		const off int32 = 96
		val := time.Unix(1_690_000_000, 0).UTC()
		if err := tx.SetDate(ctx, blk, off, val, true, logFn); err != nil {
			t.Fatalf("SetDate failed: %v", err)
		}
		buff := tx.mybuffers.GetBuffer(blk)
		if got := buff.Contents().GetDate(off); !got.Equal(val) {
			t.Errorf("expected page date %v, got %v", val, got)
		}
		if got := tx.GetDate(ctx, blk, off); !got.Equal(val) {
			t.Errorf("expected GetDate %v, got %v", val, got)
		}
	})
}
