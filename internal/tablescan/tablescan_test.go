package tablescan

import (
	"context"
	"fmt"
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

type recordColumn struct {
	key string
	val any
}

func setRecord(t *testing.T, ctx context.Context, ts *TableScan, columnSets ...recordColumn) {
	t.Helper()
	if err := ts.Insert(ctx); err != nil {
		t.Fatalf("Insertに失敗した: %v", err)
	}

	for _, col := range columnSets {
		switch v := col.val.(type) {
		case int32:
			if err := ts.SetInt(ctx, col.key, v); err != nil {
				t.Fatalf("SetIntに失敗した: %v", err)
			}
		case string:
			if err := ts.SetString(ctx, col.key, v); err != nil {
				t.Fatalf("SetStringに失敗した: %v", err)
			}
		default:
			t.Fatalf("未対応の型: %T", v)
		}
	}
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

		got, err := txmgr.Size(ctx, tblname+".tbl")
		if err != nil {
			t.Fatalf("Sizeに失敗した: %v", err)
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

		got, err := txmgr.Size(ctx, tblname+".tbl")
		if err != nil {
			t.Fatalf("Sizeに失敗した: %v", err)
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

	t.Run("Delete: Delete で空いたスロットに次の Insert が入る", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)
		layout := newTestLayout()

		ts, err := NewTableScan(ctx, txmgr, tblname, layout)
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldnameInt = "A"
		const fldnameString = "B"
		setRecord(t, ctx, ts, recordColumn{fldnameInt, int32(1)}, recordColumn{fldnameString, "v1"})
		setRecord(t, ctx, ts, recordColumn{fldnameInt, int32(2)}, recordColumn{fldnameString, "v2"})

		if err := ts.BeforeFirst(ctx); err != nil {
			t.Fatalf("BeforeFirstに失敗した: %v", err)
		}
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("Nextに失敗した: %v", err)
		}
		if !ok {
			t.Fatalf("1件目のNextはtrueであるべき")
		}
		if err := ts.Delete(ctx); err != nil {
			t.Fatalf("Deleteに失敗した: %v", err)
		}

		if err := ts.BeforeFirst(ctx); err != nil {
			t.Fatalf("BeforeFirstに失敗した: %v", err)
		}
		setRecord(t, ctx, ts, recordColumn{fldnameInt, int32(3)}, recordColumn{fldnameString, "v3"})

		if err := ts.BeforeFirst(ctx); err != nil {
			t.Fatalf("BeforeFirstに失敗した: %v", err)
		}

		expectedInts := []int32{3, 2}
		expectedStrs := []string{"v3", "v2"}
		count := 0
		for {
			ok, err := ts.Next(ctx)
			if err != nil {
				t.Fatalf("Nextに失敗した: %v", err)
			}
			if !ok {
				break
			}
			if count >= len(expectedInts) {
				t.Fatalf("レコード数が%dを超えた", len(expectedInts))
			}

			gotInt, err := ts.GetInt(ctx, fldnameInt)
			if err != nil {
				t.Fatalf("GetIntに失敗した: %v", err)
			}
			if gotInt != expectedInts[count] {
				t.Fatalf("Aは%dであるべきだが%dだった", expectedInts[count], gotInt)
			}

			gotStr, err := ts.GetString(ctx, fldnameString)
			if err != nil {
				t.Fatalf("GetStringに失敗した: %v", err)
			}
			if gotStr != expectedStrs[count] {
				t.Fatalf("Bは%qであるべきだが%qだった", expectedStrs[count], gotStr)
			}
			count++
		}
		if count != len(expectedInts) {
			t.Fatalf("レコード数は%dであるべきだが%dだった", len(expectedInts), count)
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

		got, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("Nextに失敗した: %v", err)
		}
		if got != false {
			t.Fatalf("Nextはfalseであるべきだが%vだった", got)
		}
	})

	t.Run("Next: InsertしたレコードをBeforeFirst後に辿れる", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)

		ts, err := NewTableScan(ctx, txmgr, tblname, newTestLayout())
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldnameInt = "A"
		const expectedInt int32 = 100
		const fldnameString = "B"
		const expectedString string = "hello"
		setRecord(t, ctx, ts, recordColumn{fldnameInt, expectedInt}, recordColumn{fldnameString, expectedString})

		if err := ts.BeforeFirst(ctx); err != nil {
			t.Fatalf("BeforeFirstに失敗した: %v", err)
		}

		gotNext, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("Nextに失敗した: %v", err)
		}
		if gotNext != true {
			t.Fatalf("NextはTrueであるべきだが%vだった", gotNext)
		}

		gotInt, err := ts.GetInt(ctx, fldnameInt)
		if err != nil {
			t.Fatalf("GetIntに失敗した: %v", err)
		}
		if gotInt != expectedInt {
			t.Fatalf("書き込まれた値は%dであるべきだが%dだった", expectedInt, gotInt)
		}

		gotStr, err := ts.GetString(ctx, fldnameString)
		if err != nil {
			t.Fatalf("GetStringに失敗した: %v", err)
		}
		if gotStr != expectedString {
			t.Fatalf("書き込まれた値は%qであるべきだが%qだった", expectedString, gotStr)
		}
	})

	t.Run("Next: 複数件 Insert すると Next で全件を挿入順に読める", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)

		ts, err := NewTableScan(ctx, txmgr, tblname, newTestLayout())
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const expectedCount int32 = 5
		const fldnameInt = "A"
		const fldnameString = "B"
		for i := int32(0); i < expectedCount; i++ {
			setRecord(t, ctx, ts, recordColumn{fldnameInt, i}, recordColumn{fldnameString, fmt.Sprintf("v%d", i)})
		}

		if err := ts.BeforeFirst(ctx); err != nil {
			t.Fatalf("BeforeFirstに失敗した: %v", err)
		}

		count := int32(0)
		for {
			ok, err := ts.Next(ctx)
			if err != nil {
				t.Fatalf("Nextに失敗した: %v", err)
			}
			if !ok {
				break
			}

			gotInt, err := ts.GetInt(ctx, fldnameInt)
			if err != nil {
				t.Fatalf("GetIntに失敗した: %v", err)
			}
			if gotInt != count {
				t.Fatalf("Aは%dであるべきだが%dだった", count, gotInt)
			}

			gotStr, err := ts.GetString(ctx, fldnameString)
			if err != nil {
				t.Fatalf("GetStringに失敗した: %v", err)
			}
			expectedString := fmt.Sprintf("v%d", count)
			if gotStr != expectedString {
				t.Fatalf("Bは%qであるべきだが%qだった", expectedString, gotStr)
			}

			count++
		}

		if count != expectedCount {
			t.Fatalf("書き込まれたレコード数は%dであるべきだが%dだった", expectedCount, count)
		}
	})

	t.Run("Next: ブロックをまたいで Insert した全件を Next で漏れなく読める", func(t *testing.T) {
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
		var expectedCount int32 = slotsPerBlock + 1
		const fldnameInt = "A"
		const fldnameString = "B"
		for i := int32(0); i < expectedCount; i++ {
			setRecord(t, ctx, ts, recordColumn{fldnameInt, i}, recordColumn{fldnameString, fmt.Sprintf("v%d", i)})
		}

		if err := ts.BeforeFirst(ctx); err != nil {
			t.Fatalf("BeforeFirstに失敗した: %v", err)
		}

		count := int32(0)
		for {
			ok, err := ts.Next(ctx)
			if err != nil {
				t.Fatalf("Nextに失敗した: %v", err)
			}
			if !ok {
				break
			}

			gotInt, err := ts.GetInt(ctx, fldnameInt)
			if err != nil {
				t.Fatalf("GetIntに失敗した: %v", err)
			}
			if gotInt != count {
				t.Fatalf("Aは%dであるべきだが%dだった", count, gotInt)
			}

			gotStr, err := ts.GetString(ctx, fldnameString)
			if err != nil {
				t.Fatalf("GetStringに失敗した: %v", err)
			}
			expectedString := fmt.Sprintf("v%d", count)
			if gotStr != expectedString {
				t.Fatalf("Bは%qであるべきだが%qだった", expectedString, gotStr)
			}

			count++
		}

		if count != expectedCount {
			t.Fatalf("書き込まれたレコード数は%dであるべきだが%dだった", expectedCount, count)
		}
	})

	t.Run("Next: Delete したレコードは Next で読めなくなる", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)
		layout := newTestLayout()

		ts, err := NewTableScan(ctx, txmgr, tblname, layout)
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldnameInt = "A"
		const expectedInt int32 = 100
		const fldnameString = "B"
		const expectedString string = "hello"
		setRecord(t, ctx, ts, recordColumn{fldnameInt, expectedInt}, recordColumn{fldnameString, expectedString})

		if err := ts.Delete(ctx); err != nil {
			t.Fatalf("Deleteに失敗した: %v", err)
		}

		if err := ts.BeforeFirst(ctx); err != nil {
			t.Fatalf("BeforeFirstに失敗した: %v", err)
		}
		isNext, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("Nextに失敗した: %v", err)
		}

		if isNext {
			t.Fatalf("Nextで読めなくなるはずが読めてしまった")
		}
	})

	t.Run("GetRid: GetRid は現在のブロック番号とスロットを返す", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)
		layout := newTestLayout()

		ts, err := NewTableScan(ctx, txmgr, tblname, layout)
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		const fldnameInt = "A"
		const expectedInt int32 = 100
		const fldnameString = "B"
		const expectedString string = "hello"

		setRecord(t, ctx, ts, recordColumn{fldnameInt, expectedInt}, recordColumn{fldnameString, expectedString})
		setRecord(t, ctx, ts, recordColumn{fldnameInt, expectedInt}, recordColumn{fldnameString, expectedString})
		rid := ts.GetRid()
		expectedRid := record.NewRID(0, 1)
		if !rid.Equals(expectedRid) {
			t.Fatalf("RIDが%vであるべきだが%vを返す", expectedRid, rid)
		}

		if err := ts.BeforeFirst(ctx); err != nil {
			t.Fatalf("BeforeFirstに失敗した: %v", err)
		}
		ok, err := ts.Next(ctx)
		if err != nil {
			t.Fatalf("Nextに失敗した: %v", err)
		}
		if !ok {
			t.Fatalf("Nextはtrueであるべき")
		}

		rid = ts.GetRid()
		expectedRid = record.NewRID(0, 0)
		if !rid.Equals(expectedRid) {
			t.Fatalf("RIDが%vであるべきだが%vを返す", expectedRid, rid)
		}
	})

	t.Run("MoveToRid: MoveToRid で指定したレコードに移動し、その値を読める", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newTsTestEnv(t)
		txmgr := env.newTx(t)
		layout := newTestLayout()

		ts, err := NewTableScan(ctx, txmgr, tblname, layout)
		if err != nil {
			t.Fatalf("NewTableScanに失敗した: %v", err)
		}

		var expectedCount int32 = 5
		const fldnameInt = "A"
		const fldnameString = "B"
		for i := int32(0); i < expectedCount; i++ {
			setRecord(t, ctx, ts, recordColumn{fldnameInt, i}, recordColumn{fldnameString, fmt.Sprintf("v%d", i)})
		}

		const targetSlot int32 = 2
		expectedRid := record.NewRID(0, targetSlot)
		ts.MoveToRid(ctx, expectedRid)

		if got := ts.GetRid(); !got.Equals(expectedRid) {
			t.Fatalf("RIDは%sであるべきだが%sだった", expectedRid.ToString(), got.ToString())
		}

		gotInt, err := ts.GetInt(ctx, fldnameInt)
		if err != nil {
			t.Fatalf("GetIntに失敗した: %v", err)
		}
		if gotInt != targetSlot {
			t.Fatalf("Aは%dであるべきだが%dだった", targetSlot, gotInt)
		}

		gotStr, err := ts.GetString(ctx, fldnameString)
		if err != nil {
			t.Fatalf("GetStringに失敗した: %v", err)
		}
		expectedString := fmt.Sprintf("v%d", targetSlot)
		if gotStr != expectedString {
			t.Fatalf("Bは%qであるべきだが%qだった", expectedString, gotStr)
		}
	})
}
