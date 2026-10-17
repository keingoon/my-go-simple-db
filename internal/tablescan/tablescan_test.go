package tablescan

import (
	"context"
	"testing"
	"time"

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

	t.Run("SetInt: Insertしたレコードに書いた値をGetIntで読める", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)

		ts, err := NewTableScan(ctx, txmgr, tblname, newTestLayout())
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldname = "A"
		const expected int32 = 100
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("Insertに失敗した: %v", err)
		}

		if err := ts.SetInt(ctx, fldname, expected); err != nil {
			t.Fatalf("SetIntに失敗した: %v", err)
		}

		got, err := ts.GetInt(ctx, fldname)
		if err != nil {
			t.Fatalf("GetIntに失敗した: %v", err)
		}

		if got != expected {
			t.Fatalf("書き込まれた値は%dであるべきだが%dだった", expected, got)
		}
	})

	t.Run("SetSmallInt: Insertしたレコードに書いた値をGetSmallIntで読める", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)
		sch := record.NewSchema()
		sch.AddSmallIntField("A")
		layout := record.NewLayOut(sch)

		ts, err := NewTableScan(ctx, txmgr, tblname, layout)
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldname = "A"
		const expected int16 = 6
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("Insertに失敗した: %v", err)
		}
		if err := ts.SetSmallInt(ctx, fldname, expected); err != nil {
			t.Fatalf("SetSmallIntに失敗した: %v", err)
		}

		got, err := ts.GetSmallInt(ctx, fldname)
		if err != nil {
			t.Fatalf("GetSmallIntに失敗した: %v", err)
		}
		if got != expected {
			t.Fatalf("書き込まれた値は%dであるべきだが%dだった", expected, got)
		}
	})

	t.Run("SetBool: Insertしたレコードに書いた値をGetBoolで読める", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)
		sch := record.NewSchema()
		sch.AddBoolField("A")
		layout := record.NewLayOut(sch)

		ts, err := NewTableScan(ctx, txmgr, tblname, layout)
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldname = "A"
		const expected = true
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("Insertに失敗した: %v", err)
		}
		if err := ts.SetBool(ctx, fldname, expected); err != nil {
			t.Fatalf("SetBoolに失敗した: %v", err)
		}

		got, err := ts.GetBool(ctx, fldname)
		if err != nil {
			t.Fatalf("GetBoolに失敗した: %v", err)
		}
		if got != expected {
			t.Fatalf("書き込まれた値は%tであるべきだが%tだった", expected, got)
		}
	})

	t.Run("SetDate: Insertしたレコードに書いた値をGetDateで読める", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)
		sch := record.NewSchema()
		sch.AddDateField("A")
		layout := record.NewLayOut(sch)

		ts, err := NewTableScan(ctx, txmgr, tblname, layout)
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldname = "A"
		expected := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("Insertに失敗した: %v", err)
		}
		if err := ts.SetDate(ctx, fldname, expected); err != nil {
			t.Fatalf("SetDateに失敗した: %v", err)
		}

		got, err := ts.GetDate(ctx, fldname)
		if err != nil {
			t.Fatalf("GetDateに失敗した: %v", err)
		}
		if got != expected {
			t.Fatalf("書き込まれた値は%vであるべきだが%vだった", expected, got)
		}
	})

	t.Run("SetString: Insertしたレコードに書いた値をGetStringで読める", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)

		ts, err := NewTableScan(ctx, txmgr, tblname, newTestLayout())
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldname = "B"
		const expected = "hello"
		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("Insertに失敗した: %v", err)
		}
		if err := ts.SetString(ctx, fldname, expected); err != nil {
			t.Fatalf("SetStringに失敗した: %v", err)
		}

		got, err := ts.GetString(ctx, fldname)
		if err != nil {
			t.Fatalf("GetStringに失敗した: %v", err)
		}
		if got != expected {
			t.Fatalf("書き込まれた値は%qであるべきだが%qだった", expected, got)
		}
	})

	t.Run("Insert: 最後のブロックに到達した後にInsertすると新しいブロックが作成される", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)
		layout := newTestLayout()

		ts, err := NewTableScan(ctx, txmgr, tblname, layout)
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		slotsPerBlock := (blocksize - file.PageHeaderSize) / layout.SlotSize()

		const fldname = "A"
		const expected int32 = 100
		for i := int32(0); i < slotsPerBlock; i++ {
			if err := ts.Insert(ctx); err != nil {
				t.Fatalf("%d件目のInsertに失敗した: %v", i+1, err)
			}
		}
		
		got, err := txmgr.Size(ctx, tblname+".tbl")
		if err != nil {
			t.Fatalf("Sizeに失敗した: %v", err)
		}
		if got != 1 {
			t.Fatalf("満杯直後のブロック数は1であるべきだが%dだった", got)
		}

		if err := ts.Insert(ctx); err != nil {
			t.Fatalf("満杯後のInsertに失敗した: %v", err)
		}

		got, err = txmgr.Size(ctx, tblname+".tbl")
		if err != nil {
			t.Fatalf("Sizeに失敗した: %v", err)
		}
		if got != 2 {
			t.Fatalf("ブロック数は2であるべきだが%dだった", got)
		}
	})

	t.Run("Next: 空のテーブルならfalseを返す", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)

		ts, err := NewTableScan(ctx, txmgr, tblname, newTestLayout())
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		got, err := ts.Next(ctx);
		if err != nil {
			t.Fatalf("Nextに失敗した: %v", err)
		}
		if got != false {
			t.Fatalf("ブロック数はfalseであるべきだが%vだった", got)
		}
	})
}
