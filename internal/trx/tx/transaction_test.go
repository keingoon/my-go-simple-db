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
	"github.com/keingoon/simpledb/internal/trx/access"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
	"github.com/keingoon/simpledb/internal/trx/recovery"
)

const (
	blocksize = int32(256)
	logfile   = "logfile"
	filename  = "testfile"
	numbuffs  = 10
)

var (
	lockConflictTimeout = 200 * time.Millisecond
	lockResolveTimeout  = 2 * time.Second
)

func initMgr(t *testing.T, dir string) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *buffer.DirtyPageTable) {
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
	dptTbl := buffer.NewDirtyPageTable()
	bm := buffer.NewBufferMgr(fm, lm, numbuffs, 10, dptTbl)
	return fm, lm, bm, dptTbl
}

type txTestEnv struct {
	lockTbl *concurrency.LockTable
	atTbl   *recovery.ActiveTrxTable
	fm      *file.FileMgr
	lm      *log.LogMgr
	bm      *buffer.BufferMgr
	dptTbl  *buffer.DirtyPageTable
}

func newTxTestEnv(t *testing.T, dir string) *txTestEnv {
	t.Helper()
	lockTbl := concurrency.NewLockTable()
	atTbl := recovery.NewActiveTrxTable()
	fm, lm, bm, dptTbl := initMgr(t, dir)
	return &txTestEnv{
		lockTbl: lockTbl,
		atTbl:   atTbl,
		fm:      fm,
		lm:      lm,
		bm:      bm,
		dptTbl:  dptTbl,
	}
}

func (e *txTestEnv) newTx(t *testing.T) *TransactionMgr {
	t.Helper()
	tx, err := NewTransactionMgr(e.lockTbl, e.fm, e.lm, e.bm, e.atTbl, e.dptTbl)
	if err != nil {
		t.Fatalf("failed to create NewTransactionMgr: %v", err)
	}
	return tx
}

func (e *txTestEnv) newRecoveryTx(t *testing.T) *TransactionMgr {
	t.Helper()
	tx, err := NewRecoveryTransactionMgr(e.lockTbl, e.fm, e.lm, e.bm, e.atTbl, e.dptTbl)
	if err != nil {
		t.Fatalf("failed to create NewRecoveryTransactionMgr: %v", err)
	}
	return tx
}

func mustAppendBlock(t *testing.T, fm *file.FileMgr, name string) *file.BlockId {
	t.Helper()
	blk, err := fm.Append(name)
	if err != nil {
		t.Fatalf("append block failed: %v", err)
	}
	return blk
}

func setInt32WithTimeout(tx *TransactionMgr, blk *file.BlockId, value int32, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return tx.SetInt32(ctx, blk, 0, value, true)
}

func TestTransaction_Constructors(t *testing.T) {
	t.Run("NewTransactionMgr: 通常トランザクションマネージャを生成すると依存が初期化される", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")

		// Act
		wTx := env.newTx(t)

		// Assert
		if wTx == nil {
			t.Fatal("NewTransactionMgr returned nil")
		}
		if wTx.bm != env.bm {
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
		if wTx.txnum <= 0 {
			t.Errorf("expected txnum to be positive, got %d", wTx.txnum)
		}
	})

	t.Run("NewRecoveryTransactionMgr: リカバリ用トランザクションマネージャを生成すると依存が初期化される", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")

		// Act
		rTx := env.newRecoveryTx(t)

		// Assert
		if rTx == nil {
			t.Fatal("NewRecoveryTransactionMgr returned nil")
		}
		if rTx.bm != env.bm {
			t.Errorf("expected bm to be set")
		}
		if rTx.fm == nil {
			t.Errorf("expected fm to be set")
		}
		if rTx.recoveryMgr == nil {
			t.Errorf("expected recoveryMgr to be set")
		}
		if rTx.mybuffers == nil {
			t.Errorf("expected mybuffers to be initialized")
		}
		if rTx.txAccess == nil {
			t.Errorf("expected txAccess to be initialized")
		}
		if rTx.txnum != access.RecoveryTxNum {
			t.Errorf("expected txnum %d, got %d", access.RecoveryTxNum, rTx.txnum)
		}
	})
}

func TestTransaction_Persistence(t *testing.T) {
	t.Run("Pin/Unpin", func(t *testing.T) {
		t.Run("Pin: Pinすると利用可能バッファ数が1減る", func(t *testing.T) {
			ctx := context.Background()
			lockTbl := concurrency.NewLockTable()
			atTbl := recovery.NewActiveTrxTable()
			fm, lm, bm, dptTbl := initMgr(t, "")
			wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
			if err != nil {
				t.Fatalf("failed to create NewTransactionMgr: %v", err)
			}
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
		})

		t.Run("Unpin: Unpinすると利用可能バッファ数が元に戻る", func(t *testing.T) {
			ctx := context.Background()
			lockTbl := concurrency.NewLockTable()
			atTbl := recovery.NewActiveTrxTable()
			fm, lm, bm, dptTbl := initMgr(t, "")
			wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
			if err != nil {
				t.Fatalf("failed to create NewTransactionMgr: %v", err)
			}
			blk, err := fm.Append(filename)
			if err != nil {
				t.Fatalf("append block failed: %v", err)
			}
			before := bm.Available()
			wTx.Pin(ctx, blk)
			wTx.Unpin(ctx, blk)
			if bm.Available() != before {
				t.Errorf("expected available %d, got %d", before, bm.Available())
			}
		})

		t.Run("Unpin: Unpinするとmybuffersから対象ブロックが外れる", func(t *testing.T) {
			ctx := context.Background()
			lockTbl := concurrency.NewLockTable()
			atTbl := recovery.NewActiveTrxTable()
			fm, lm, bm, dptTbl := initMgr(t, "")
			wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
			if err != nil {
				t.Fatalf("failed to create NewTransactionMgr: %v", err)
			}
			blk, err := fm.Append(filename)
			if err != nil {
				t.Fatalf("append block failed: %v", err)
			}
			wTx.Pin(ctx, blk)
			wTx.Unpin(ctx, blk)
			if wTx.mybuffers.GetBuffer(blk) != nil {
				t.Errorf("expected buffer to be removed from mybuffers after unpin")
			}
		})

		t.Run("Pin: 存在しないブロックを指定するとerrorを返す", func(t *testing.T) {
			ctx := context.Background()
			lockTbl := concurrency.NewLockTable()
			atTbl := recovery.NewActiveTrxTable()
			fm, lm, bm, dptTbl := initMgr(t, "")
			wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
			if err != nil {
				t.Fatalf("failed to create NewTransactionMgr: %v", err)
			}

			blk := file.NewBlockId(filename, 0)
			if err := wTx.Pin(ctx, blk); err == nil {
				t.Fatal("Pinは失敗するべき")
			}
		})
	})

	t.Run("SetInt16/GetInt16: 書き込んだint16を別トランザクションで読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)

		const off int32 = 0
		val := int16(1234)

		// Act (Write)
		if err := wTx.SetInt16(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt16 failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		rTx.Pin(ctx, blk)

		// Act (Read) / Assert
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

	t.Run("SetInt32/GetInt32: 書き込んだint32を別トランザクションで読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)
		const off int32 = 4
		val := int32(987654321)

		// Act (Write)
		if err := wTx.SetInt32(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt32 failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		rTx.Pin(ctx, blk)

		// Act (Read) / Assert
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

	t.Run("SetStr/GetStr: 書き込んだ文字列を別トランザクションで読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()

		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)
		const off int32 = 32
		val := "hello"

		// Act (Write)
		if err := wTx.SetStr(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetStr failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		rTx.Pin(ctx, blk)

		// Act (Read) / Assert
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

	t.Run("SetBool/GetBool: 書き込んだboolを別トランザクションで読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)

		const off int32 = 64
		val := true

		// Act (Write)
		if err := wTx.SetBool(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetBool failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		rTx.Pin(ctx, blk)

		// Act (Read) / Assert
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

	t.Run("SetDate/GetDate: 書き込んだ日付を別トランザクションで読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		wTx.Pin(ctx, blk)

		const off int32 = 96
		val := time.Unix(1_690_000_000, 0).UTC()

		// Act (Write)
		if err := wTx.SetDate(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetDate failed: %v", err)
		}
		wTx.Commit(ctx)
		wTx.Unpin(ctx, blk)

		rTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		rTx.Pin(ctx, blk)

		// Act (Read) / Assert
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
	t.Run("SetInt16/GetInt16: 同一トランザクション内で書き込み値を読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		txmgr, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 0
		val := int16(1234)

		// Act
		if err := txmgr.SetInt16(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt16 failed: %v", err)
		}

		// Assert
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
	t.Run("Rollback: 未コミット更新を取り消す", func(t *testing.T) {
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		wTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
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
		rTx, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
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

	t.Run("Checkpoint", func(t *testing.T) {
		t.Run("Checkpoint: RecoveryTransactionMgrから呼ぶと成功する", func(t *testing.T) {
			ctx := context.Background()
			env := newTxTestEnv(t, "")
			recoveryTx := env.newRecoveryTx(t)

			endLSN, err := recoveryTx.Checkpoint(ctx)
			if err != nil {
				t.Fatalf("Checkpoint failed: %v", err)
			}
			if endLSN <= 0 {
				t.Fatalf("Checkpointは正のLSNを返すべきだが%dだった", endLSN)
			}
		})

		t.Run("Checkpoint: 実行後にReadLastCheckpointLSNが更新される", func(t *testing.T) {
			ctx := context.Background()
			env := newTxTestEnv(t, "")
			recoveryTx := env.newRecoveryTx(t)
			wTx := env.newTx(t)
			if err := wTx.Commit(ctx); err != nil {
				t.Fatalf("checkpoint前のCommitが失敗した: %v", err)
			}

			before, err := env.lm.ReadLastCheckpointLSN()
			if err != nil {
				t.Fatalf("checkpoint前のReadLastCheckpointLSNが失敗した: %v", err)
			}

			endLSN, err := recoveryTx.Checkpoint(ctx)
			if err != nil {
				t.Fatalf("Checkpoint failed: %v", err)
			}

			after, err := env.lm.ReadLastCheckpointLSN()
			if err != nil {
				t.Fatalf("checkpoint後のReadLastCheckpointLSNが失敗した: %v", err)
			}

			if after <= before {
				t.Errorf("lastCheckpointLSNは更新されるべきだが before=%d after=%d だった", before, after)
			}
			if after >= endLSN {
				t.Errorf("lastCheckpointLSNはcheckpointのendLSNより前を指すべきだが lastCheckpointLSN=%d endLSN=%d だった", after, endLSN)
			}
		})
	})

	// Recover should undo losers and keep winners
	t.Run("Recover: loserを取り消してcommitted更新を維持する", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		dir := t.TempDir()
		beforeCrash := newTxTestEnv(t, dir)

		wTx1 := beforeCrash.newTx(t)
		blk1 := mustAppendBlock(t, beforeCrash.fm, "recover_keep")
		wTx1.Pin(ctx, blk1)
		const off1 int32 = 0
		val1 := int32(777)
		if err := wTx1.SetInt32(ctx, blk1, off1, val1, true); err != nil {
			t.Fatalf("tx1 SetInt32 failed: %v", err)
		}
		wTx1.Commit(ctx)

		wTx2 := beforeCrash.newTx(t)
		blk2 := mustAppendBlock(t, beforeCrash.fm, "recover_undo")
		wTx2.Pin(ctx, blk2)
		const off2 int32 = 4
		val2 := int32(999)
		if err := wTx2.SetInt32(ctx, blk2, off2, val2, true); err != nil {
			t.Fatalf("tx2 SetInt32 failed: %v", err)
		}
		afterCrash := newTxTestEnv(t, dir)

		// Act
		recoverTxMgr := afterCrash.newRecoveryTx(t)
		if err := recoverTxMgr.Recover(ctx); err != nil {
			t.Fatalf("Recover failed: %v", err)
		}

		// Assert
		rTx1 := afterCrash.newTx(t)
		rTx1.Pin(ctx, blk1)
		if got, err := rTx1.GetInt32(ctx, blk1, off1); err != nil {
			t.Fatalf("GetInt32 failed: %v", err)
		} else if got != val1 {
			t.Errorf("expected committed value %d, got %d", val1, got)
		}
		rTx1.Commit(ctx)

		rTx2 := afterCrash.newTx(t)
		rTx2.Pin(ctx, blk2)
		if got, err := rTx2.GetInt32(ctx, blk2, off2); err != nil {
			t.Fatalf("GetInt32 failed: %v", err)
		} else if got != 0 {
			t.Errorf("expected undone value %d, got %d", 0, got)
		}
		rTx2.Commit(ctx)
	})

	t.Run("SetInt32/GetInt32: 同一トランザクション内で書き込み値を読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		txmgr, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 4
		val := int32(987654321)

		// Act
		if err := txmgr.SetInt32(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetInt32 failed: %v", err)
		}

		// Assert
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

	t.Run("SetStr/GetStr: 同一トランザクション内で書き込み値を読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		txmgr, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 32
		val := "hello"

		// Act
		if err := txmgr.SetStr(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetStr failed: %v", err)
		}

		// Assert
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

	t.Run("SetBool/GetBool: 同一トランザクション内で書き込み値を読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		txmgr, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 64
		val := true

		// Act
		if err := txmgr.SetBool(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetBool failed: %v", err)
		}

		// Assert
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

	t.Run("SetDate/GetDate: 同一トランザクション内で書き込み値を読み出せる", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		lockTbl := concurrency.NewLockTable()
		atTbl := recovery.NewActiveTrxTable()
		fm, lm, bm, dptTbl := initMgr(t, "")
		txmgr, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
		if err != nil {
			t.Fatalf("failed to create NewTransactionMgr: %v", err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append block failed: %v", err)
		}
		txmgr.Pin(ctx, blk)

		const off int32 = 96
		val := time.Unix(1_690_000_000, 0).UTC()

		// Act
		if err := txmgr.SetDate(ctx, blk, off, val, true); err != nil {
			t.Fatalf("SetDate failed: %v", err)
		}

		// Assert
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

	t.Run("Commit", func(t *testing.T) {
		t.Run("Commit: 実行後に利用可能バッファ数が元に戻る", func(t *testing.T) {
			ctx := context.Background()
			lockTbl := concurrency.NewLockTable()
			atTbl := recovery.NewActiveTrxTable()
			fm, lm, bm, dptTbl := initMgr(t, "")
			txmgr, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
			if err != nil {
				t.Fatalf("failed to create NewTransactionMgr: %v", err)
			}
			blk, err := fm.Append("testfile_tx_commit_available")
			if err != nil {
				t.Fatalf("append block failed: %v", err)
			}
			before := bm.Available()
			txmgr.Pin(ctx, blk)
			if err := txmgr.SetInt32(ctx, blk, 4, 42, true); err != nil {
				t.Fatalf("SetInt32 failed: %v", err)
			}
			txmgr.Commit(ctx)
			if bm.Available() != before {
				t.Errorf("expected available %d after commit, got %d", before, bm.Available())
			}
		})

		t.Run("Commit: 実行後にmybuffersが空になる", func(t *testing.T) {
			ctx := context.Background()
			lockTbl := concurrency.NewLockTable()
			atTbl := recovery.NewActiveTrxTable()
			fm, lm, bm, dptTbl := initMgr(t, "")
			txmgr, err := NewTransactionMgr(lockTbl, fm, lm, bm, atTbl, dptTbl)
			if err != nil {
				t.Fatalf("failed to create NewTransactionMgr: %v", err)
			}
			blk, err := fm.Append("testfile_tx_commit_mybuffers")
			if err != nil {
				t.Fatalf("append block failed: %v", err)
			}
			txmgr.Pin(ctx, blk)
			if err := txmgr.SetInt32(ctx, blk, 4, 42, true); err != nil {
				t.Fatalf("SetInt32 failed: %v", err)
			}
			txmgr.Commit(ctx)
			if txmgr.mybuffers.GetBuffer(blk) != nil {
				t.Errorf("expected mybuffers to be empty after commit")
			}
		})
	})
}

func TestTransaction_Conflicts(t *testing.T) {
	// Concurrency conflict tests
	t.Run("SetInt32: 読み取り保持中のブロックへの書き込みはタイムアウトする", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")
		ctx := context.Background()
		blk := mustAppendBlock(t, env.fm, "conflict_rw")
		rTx := env.newTx(t)
		rTx.Pin(ctx, blk)
		if _, err := rTx.GetInt32(ctx, blk, 0); err != nil { // acquire SLock
			t.Fatalf("reader GetInt32 failed: %v", err)
		}

		wTx := env.newTx(t)
		wTx.Pin(ctx, blk)

		// Act
		err := setInt32WithTimeout(wTx, blk, 1, lockConflictTimeout)

		// Assert
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected writer timeout (DeadlineExceeded), got %v", err)
		}
		rTx.Commit(ctx)   // cleanup
		wTx.Rollback(ctx) // cleanup
	})

	t.Run("SetInt32: 書き込み保持中のブロックへの書き込みはタイムアウトする", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")
		ctx := context.Background()
		blk := mustAppendBlock(t, env.fm, "conflict_ww")

		wTx1 := env.newTx(t)
		wTx1.Pin(ctx, blk)
		if err := wTx1.SetInt32(ctx, blk, 0, 100, true); err != nil { // acquire XLock
			t.Fatalf("writer1 SetInt32 failed: %v", err)
		}

		wTx2 := env.newTx(t)
		wTx2.Pin(ctx, blk)

		// Act
		err := setInt32WithTimeout(wTx2, blk, 200, lockConflictTimeout)

		// Assert
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected writer1 timeout (DeadlineExceeded), got %v", err)
		}

		// cleanup
		wTx1.Commit(ctx)
		wTx2.Rollback(ctx)
	})

	t.Run("SetInt32: 同時S->X昇格競合はタイムアウトする", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")
		ctx := context.Background()
		blk := mustAppendBlock(t, env.fm, "conflict_upgrade")
		tx1 := env.newTx(t)
		tx1.Pin(ctx, blk)
		if _, err := tx1.GetInt32(ctx, blk, 0); err != nil { // acquire SLock
			t.Fatalf("tx1 GetInt32 failed: %v", err)
		}

		tx2 := env.newTx(t)
		tx2.Pin(ctx, blk)
		if _, err := tx2.GetInt32(ctx, blk, 0); err != nil { // acquire SLock
			t.Fatalf("tx2 GetInt32 failed: %v", err)
		}

		var (
			err1 error
			err2 error
		)
		var wg sync.WaitGroup
		wg.Add(2)

		// Act
		go func() {
			defer wg.Done()
			err1 = setInt32WithTimeout(tx1, blk, 1, lockConflictTimeout)
		}()
		go func() {
			defer wg.Done()
			err2 = setInt32WithTimeout(tx2, blk, 2, lockConflictTimeout)
		}()
		wg.Wait()

		// Assert
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
}

func TestTransaction_Deadlocks(t *testing.T) {
	t.Run("SetInt32: 相互待機デッドロックでは少なくとも一方がタイムアウトする", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")
		ctx := context.Background()
		blk1 := mustAppendBlock(t, env.fm, "deadlock_blk1")
		blk2 := mustAppendBlock(t, env.fm, "deadlock_blk2")

		tx1 := env.newTx(t)
		tx2 := env.newTx(t)
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

		var (
			err1 error
			err2 error
		)
		var wg sync.WaitGroup
		wg.Add(2)

		// Act
		go func() {
			defer wg.Done()
			err1 = setInt32WithTimeout(tx1, blk2, 10, lockConflictTimeout)
		}()
		go func() {
			defer wg.Done()
			err2 = setInt32WithTimeout(tx2, blk1, 20, lockConflictTimeout)
		}()
		wg.Wait()

		// Assert
		if (err1 == nil || !errors.Is(err1, context.DeadlineExceeded)) && (err2 == nil || !errors.Is(err2, context.DeadlineExceeded)) {
			t.Fatalf("expected at least one timeout (DeadlineExceeded), got err1=%v err2=%v", err1, err2)
		}

		// cleanup
		tx1.Rollback(ctx)
		tx2.Rollback(ctx)
	})

	// Deadlock resolved by rollback (upgrade vs upgrade)
	t.Run("SetInt32: S->Xデッドロックでも片方のRollback後にもう片方は進める", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")
		ctx := context.Background()
		blk := mustAppendBlock(t, env.fm, "deadlock_upgrade_blk")

		tx1 := env.newTx(t)
		tx2 := env.newTx(t)
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

		// Act
		go func() {
			defer wg.Done()
			wctx1, cancel1 := context.WithTimeout(context.Background(), lockResolveTimeout)
			defer cancel1()
			err1 = tx1.SetInt32(wctx1, blk, 0, 111, true)
		}()

		go func() {
			defer wg.Done()
			_ = setInt32WithTimeout(tx2, blk, 222, lockConflictTimeout)
			tx2.Rollback(ctx)
		}()

		wg.Wait()

		// Assert
		if err1 != nil {
			t.Fatalf("expected tx1 to succeed after tx2 rollback, got %v", err1)
		}
		tx1.Commit(ctx)
	})

	// Deadlock resolved by rollback (read/write across two blocks)
	t.Run("SetInt32: read/writeデッドロックでも片方のRollback後にもう片方は進める", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")
		ctx := context.Background()
		blk1 := mustAppendBlock(t, env.fm, "deadlock_rw_blk1")
		blk2 := mustAppendBlock(t, env.fm, "deadlock_rw_blk2")

		tx1 := env.newTx(t)
		tx2 := env.newTx(t)
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

		// Act
		go func() {
			defer wg.Done()
			wctx1, cancel1 := context.WithTimeout(context.Background(), lockResolveTimeout)
			defer cancel1()
			err1 = tx1.SetInt32(wctx1, blk2, 0, 10, true)
		}()

		go func() {
			defer wg.Done()
			_ = setInt32WithTimeout(tx2, blk1, 20, lockConflictTimeout)
			tx2.Rollback(ctx)
		}()

		wg.Wait()

		// Assert
		if err1 != nil {
			t.Fatalf("expected tx1 to proceed after tx2 rollback, got %v", err1)
		}
		tx1.Commit(ctx)
	})

	// Deadlock resolved by rollback (write/write across two blocks)
	t.Run("SetInt32: write/writeデッドロックでも片方のRollback後にもう片方は進める", func(t *testing.T) {
		// Arrange
		env := newTxTestEnv(t, "")
		ctx := context.Background()
		blk1 := mustAppendBlock(t, env.fm, "deadlock_ww_blk1")
		blk2 := mustAppendBlock(t, env.fm, "deadlock_ww_blk2")

		tx1 := env.newTx(t)
		tx2 := env.newTx(t)
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

		// Act
		go func() {
			defer wg.Done()
			wctx1, cancel1 := context.WithTimeout(context.Background(), lockResolveTimeout)
			defer cancel1()
			err1 = tx1.SetInt32(wctx1, blk2, 4, 101, true)
		}()

		go func() {
			defer wg.Done()
			wctx2, cancel2 := context.WithTimeout(context.Background(), lockConflictTimeout)
			defer cancel2()
			_ = tx2.SetInt32(wctx2, blk1, 4, 201, true)
			tx2.Rollback(ctx)
		}()

		wg.Wait()

		// Assert
		if err1 != nil {
			t.Fatalf("expected tx1 to proceed after tx2 rollback, got %v", err1)
		}
		tx1.Commit(ctx)
	})
}
