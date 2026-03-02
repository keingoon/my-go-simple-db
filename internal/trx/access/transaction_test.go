package access

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/bufferlist"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
)

const (
	blocksize = int32(256)
	logfile   = "logfile"
	filename  = "testfile"
	numbuffs  = 5
	numwaits  = 5
)

func initTx(t *testing.T, locktbl *concurrency.LockTable, txnum int32) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *Transaction) {
	t.Helper()

	fm, err := file.NewFileMgr(t.TempDir(), blocksize)
	if err != nil {
		t.Fatalf("failed to create FileMgr: %v", err)
	}
	lm, err := log.NewLogMgr(fm, logfile)
	if err != nil {
		t.Fatalf("failed to create LogMgr: %v", err)
	}
	dpt := buffer.NewDirtyPageTable()
	bm := buffer.NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
	mybuffers := bufferlist.NewBufferList(bm)

	tx := NewTransaction(locktbl, fm, lm, bm, txnum, mybuffers)
	return fm, lm, bm, tx
}

func TestTransaction(t *testing.T) {
	t.Run("コンストラクタ", func(t *testing.T) {
		t.Run("NewTransaction: 通常トランザクションを生成するとStrict2PLで初期化される", func(t *testing.T) {
			_, _, bm, tx := initTx(t, concurrency.NewLockTable(), 1)

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
			if tx.lockPolicy != LockStrict2PL {
				t.Errorf("expected lockPolicy %d, got %d", LockStrict2PL, tx.lockPolicy)
			}
		})

		t.Run("NewRecoveryTransaction: リカバリ用トランザクションを生成するとLockNoLockで初期化される", func(t *testing.T) {
			fm, err := file.NewFileMgr(t.TempDir(), blocksize)
			if err != nil {
				t.Fatalf("failed to create FileMgr: %v", err)
			}
			lm, err := log.NewLogMgr(fm, logfile)
			if err != nil {
				t.Fatalf("failed to create LogMgr: %v", err)
			}
			dpt := buffer.NewDirtyPageTable()
			bm := buffer.NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
			tx := NewRecoveryTransaction(concurrency.NewLockTable(), fm, lm, bm)

			if tx == nil {
				t.Fatal("NewRecoveryTransaction returned nil")
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
			if tx.txnum != RecoveryTxNum {
				t.Errorf("expected txnum %d, got %d", RecoveryTxNum, tx.txnum)
			}
			if tx.lockPolicy != LockNoLock {
				t.Errorf("expected lockPolicy %d, got %d", LockNoLock, tx.lockPolicy)
			}
		})
	})

	t.Run("Pin/Unpin: Pin後にUnpinするとバッファ状態が元に戻る", func(t *testing.T) {
		ctx := context.Background()
		fm, _, bm, tx := initTx(t, concurrency.NewLockTable(), 1)

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

	t.Run("SetInt16/GetInt16: 書き込んだint16を読み出せる", func(t *testing.T) {
		ctx := context.Background()
		fm, _, _, tx := initTx(t, concurrency.NewLockTable(), 2)
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
		if got, err := tx.GetInt16(ctx, blk, off); err != nil {
			t.Fatalf("GetInt16 failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetInt16 %d, got %d", val, got)
		}
		if buff.ModifyingTx() != tx.txnum {
			t.Errorf("expected modifying tx %d, got %d", tx.txnum, buff.ModifyingTx())
		}

		// Cleanup
		tx.Unlock(ctx, blk)
	})

	t.Run("SetInt32/GetInt32: 書き込んだint32を読み出せる", func(t *testing.T) {
		ctx := context.Background()
		fm, _, _, tx := initTx(t, concurrency.NewLockTable(), 3)
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
		if got, err := tx.GetInt32(ctx, blk, off); err != nil {
			t.Fatalf("GetInt32 failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetInt32 %d, got %d", val, got)
		}

		// Cleanup
		tx.Unlock(ctx, blk)
	})

	t.Run("SetStr/GetStr: 書き込んだ文字列を読み出せる", func(t *testing.T) {
		ctx := context.Background()
		fm, _, _, tx := initTx(t, concurrency.NewLockTable(), 4)
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
		if got, err := tx.GetStr(ctx, blk, off); err != nil {
			t.Fatalf("GetStr failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetStr %q, got %q", val, got)
		}

		// Cleanup
		tx.Unlock(ctx, blk)
	})

	t.Run("SetBool/GetBool: 書き込んだboolを読み出せる", func(t *testing.T) {
		ctx := context.Background()
		fm, _, _, tx := initTx(t, concurrency.NewLockTable(), 5)
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
		if got, err := tx.GetBool(ctx, blk, off); err != nil {
			t.Fatalf("GetBool failed: %v", err)
		} else if got != val {
			t.Errorf("expected GetBool %v, got %v", val, got)
		}

		// Cleanup
		tx.Unlock(ctx, blk)
	})

	t.Run("SetDate/GetDate: 書き込んだ日付を読み出せる", func(t *testing.T) {
		ctx := context.Background()
		fm, _, _, tx := initTx(t, concurrency.NewLockTable(), 6)
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
		if got, err := tx.GetDate(ctx, blk, off); err != nil {
			t.Fatalf("GetDate failed: %v", err)
		} else if !got.Equal(val) {
			t.Errorf("expected GetDate %v, got %v", val, got)
		}

		// Cleanup
		tx.Unlock(ctx, blk)
	})

	t.Run("SLock/Unlock", func(t *testing.T) {
		t.Run("SLock保持中は他トランザクションのXLockがタイムアウトまで待機する", func(t *testing.T) {
			ctx := context.Background()
			locktbl := concurrency.NewLockTable()
			fm, _, _, tx1 := initTx(t, locktbl, 7)
			_, _, _, tx2 := initTx(t, locktbl, 8)

			blk, err := fm.Append("testfile_lock_slock_block")
			if err != nil {
				t.Fatalf("append block failed: %v", err)
			}
			if err := tx1.SLock(ctx, blk); err != nil {
				t.Fatalf("tx1 SLock failed: %v", err)
			}

			lockCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			err = tx2.XLock(lockCtx, blk)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected timeout while tx1 holds SLock, got %v", err)
			}

			tx1.Unlock(ctx, blk)
		})

		t.Run("SLockを解放すると他トランザクションのXLockが取得できる", func(t *testing.T) {
			ctx := context.Background()
			locktbl := concurrency.NewLockTable()
			fm, _, _, tx1 := initTx(t, locktbl, 9)
			_, _, _, tx2 := initTx(t, locktbl, 10)

			blk, err := fm.Append("testfile_lock_slock_release")
			if err != nil {
				t.Fatalf("append block failed: %v", err)
			}
			if err := tx1.SLock(ctx, blk); err != nil {
				t.Fatalf("tx1 SLock failed: %v", err)
			}

			lockCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			started := make(chan struct{}, 1)
			go func() {
				started <- struct{}{}
				done <- tx2.XLock(lockCtx, blk)
			}()
			<-started

			tx1.Unlock(ctx, blk)

			err = <-done
			if err != nil {
				t.Fatalf("expected XLock to succeed after unlock, got %v", err)
			}

			tx2.Unlock(ctx, blk)
		})
	})

	t.Run("XLock/Unlock", func(t *testing.T) {
		t.Run("XLock保持中は他トランザクションのSLockがタイムアウトまで待機する", func(t *testing.T) {
			ctx := context.Background()
			locktbl := concurrency.NewLockTable()
			fm, _, _, tx1 := initTx(t, locktbl, 11)
			_, _, _, tx2 := initTx(t, locktbl, 12)

			blk, err := fm.Append("testfile_lock_xlock_block")
			if err != nil {
				t.Fatalf("append block failed: %v", err)
			}
			if err := tx1.XLock(ctx, blk); err != nil {
				t.Fatalf("tx1 XLock failed: %v", err)
			}

			lockCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			err = tx2.SLock(lockCtx, blk)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected timeout while tx1 holds XLock, got %v", err)
			}

			tx1.Unlock(ctx, blk)
		})

		t.Run("XLockを解放すると他トランザクションのSLockが取得できる", func(t *testing.T) {
			ctx := context.Background()
			locktbl := concurrency.NewLockTable()
			fm, _, _, tx1 := initTx(t, locktbl, 13)
			_, _, _, tx2 := initTx(t, locktbl, 14)

			blk, err := fm.Append("testfile_lock_xlock_release")
			if err != nil {
				t.Fatalf("append block failed: %v", err)
			}
			if err := tx1.XLock(ctx, blk); err != nil {
				t.Fatalf("tx1 XLock failed: %v", err)
			}

			lockCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			started := make(chan struct{}, 1)
			go func() {
				started <- struct{}{}
				done <- tx2.SLock(lockCtx, blk)
			}()
			<-started

			tx1.Unlock(ctx, blk)

			err = <-done
			if err != nil {
				t.Fatalf("expected SLock to succeed after unlock, got %v", err)
			}

			tx2.Unlock(ctx, blk)
		})
	})

	t.Run("SLock/XLock: 同一トランザクションでSLockからXLockへ昇格できる", func(t *testing.T) {
		ctx := context.Background()
		fm, _, _, tx := initTx(t, concurrency.NewLockTable(), 15)

		blk, err := fm.Append("testfile_lock_upgrade")
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}

		if err := tx.SLock(ctx, blk); err != nil {
			t.Fatalf("SLock failed: %v", err)
		}
		upgradeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if err := tx.XLock(upgradeCtx, blk); err != nil {
			t.Fatalf("XLock upgrade failed: %v", err)
		}

		tx.Unlock(ctx, blk)
	})
}
