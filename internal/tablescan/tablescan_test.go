package tablescan

import (
	"context"
	"testing"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/record"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
	"github.com/keingoon/simpledb/internal/trx/recovery"
	"github.com/keingoon/simpledb/internal/trx/tx"
)

const (
	blocksize = int32(400)
	logfile   = "logfile"
	tblname   = "T"
	numbuffs  = 8
)

type tsTestEnv struct {
	lockTbl *concurrency.LockTable
	atTbl   *recovery.ActiveTrxTable
	fm      *file.FileMgr
	lm      *log.LogMgr
	bm      *buffer.BufferMgr
	dptTbl  *buffer.DirtyPageTable
}

func newTsTestEnv(t *testing.T) *tsTestEnv {
	t.Helper()
	fm, err := file.NewFileMgr(t.TempDir(), blocksize)
	if err != nil {
		t.Fatalf("FileMgrの生成に失敗した: %v", err)
	}
	lm, err := log.NewLogMgr(fm, logfile)
	if err != nil {
		t.Fatalf("LogMgrの生成に失敗した: %v", err)
	}
	dptTbl := buffer.NewDirtyPageTable()
	bm := buffer.NewBufferMgr(fm, lm, numbuffs, 10, dptTbl)
	return &tsTestEnv{
		lockTbl: concurrency.NewLockTable(),
		atTbl:   recovery.NewActiveTrxTable(),
		fm:      fm,
		lm:      lm,
		bm:      bm,
		dptTbl:  dptTbl,
	}
}

func (e *tsTestEnv) newTx(t *testing.T) *tx.TransactionMgr {
	t.Helper()
	txmgr, err := tx.NewTransactionMgr(e.lockTbl, e.fm, e.lm, e.bm, e.atTbl, e.dptTbl)
	if err != nil {
		t.Fatalf("TransactionMgrの生成に失敗した: %v", err)
	}
	return txmgr
}

func newTestLayout() *record.Layout {
	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	return record.NewLayOut(sch)
}

func TestTableScan(t *testing.T) {
	t.Parallel()

	t.Run("TableScan: 空のテーブルならブロックが1つ作成される", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)

		if _, err := NewTableScan(ctx, txmgr, tblname, newTestLayout()); err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		got, err := env.fm.Length(tblname + ".tbl")
		if err != nil {
			t.Fatalf("Lengthに失敗した: %v", err)
		}
		if got != 1 {
			t.Fatalf("ブロック数は1であるべきだが%dだった", got)
		}
	})

	t.Run("TableScan: 既存のテーブルならブロックが増えない", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		if _, err := env.fm.Append(tblname + ".tbl"); err != nil {
			t.Fatalf("Appendに失敗した: %v", err)
		}
		txmgr := env.newTx(t)

		if _, err := NewTableScan(ctx, txmgr, tblname, newTestLayout()); err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		got, err := env.fm.Length(tblname + ".tbl")
		if err != nil {
			t.Fatalf("Lengthに失敗した: %v", err)
		}
		if got != 1 {
			t.Fatalf("ブロック数は1のままであるべきだが%dだった", got)
		}
	})
}
