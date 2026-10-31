package recordpage

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
	filename  = "testfile"
	numbuffs  = 8
)

type rpTestEnv struct {
	lockTbl *concurrency.LockTable
	atTbl   *recovery.ActiveTrxTable
	fm      *file.FileMgr
	lm      *log.LogMgr
	bm      *buffer.BufferMgr
	dptTbl  *buffer.DirtyPageTable
}

func newRpTestEnv(t *testing.T) *rpTestEnv {
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
	return &rpTestEnv{
		lockTbl: concurrency.NewLockTable(),
		atTbl:   recovery.NewActiveTrxTable(),
		fm:      fm,
		lm:      lm,
		bm:      bm,
		dptTbl:  dptTbl,
	}
}

func (e *rpTestEnv) newTx(t *testing.T) *tx.TransactionMgr {
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

func newFormattedRecordPage(t *testing.T, env *rpTestEnv) (*tx.TransactionMgr, *RecordPage) {
	t.Helper()
	ctx := context.Background()
	txmgr := env.newTx(t)
	blk, err := env.fm.Append(filename)
	if err != nil {
		t.Fatalf("Appendに失敗した: %v", err)
	}
	rp := NewRecordPage(ctx, txmgr, blk, newTestLayout())
	rp.Format(ctx)
	return txmgr, rp
}

func TestRecordPage(t *testing.T) {
	t.Parallel()

	t.Run("RecordPage: NewRecordPageは指定したBlockを保持する", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := newRpTestEnv(t)
		txmgr := env.newTx(t)
		blk, err := env.fm.Append(filename)
		if err != nil {
			t.Fatalf("Appendに失敗した: %v", err)
		}

		rp := NewRecordPage(ctx, txmgr, blk, newTestLayout())

		if got := rp.Block(); got != blk {
			t.Fatalf("Blockは指定したblkであるべき")
		}
	})

	t.Run("RecordPage: Format後はUSEDスロットがない", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

		slot, err := rp.NextAfter(ctx, -1)
		if err != nil {
			t.Fatalf("NextAfterが失敗した: %v", err)
		}
		if slot != -1 {
			t.Fatalf("NextAfterは-1を返すべきだが%dだった", slot)
		}
	})

	t.Run("RecordPage: InsertAfter", func(t *testing.T) {
		t.Parallel()

		t.Run("Format後のInsertAfter(-1)はslot0を返す", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			slot, err := rp.InsertAfter(ctx, -1)
			if err != nil {
				t.Fatalf("InsertAfterが失敗した: %v", err)
			}
			if slot != 0 {
				t.Fatalf("InsertAfterは0を返すべきだが%dだった", slot)
			}
		})

		t.Run("InsertしたslotはNextAfterで見つかる", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			if _, err := rp.InsertAfter(ctx, -1); err != nil {
				t.Fatalf("InsertAfterが失敗した: %v", err)
			}

			slot, err := rp.NextAfter(ctx, -1)
			if err != nil {
				t.Fatalf("NextAfterが失敗した: %v", err)
			}
			if slot != 0 {
				t.Fatalf("NextAfterは0を返すべきだが%dだった", slot)
			}
		})

		t.Run("連続Insertすると次の空きslotを返す", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			slot0, err := rp.InsertAfter(ctx, -1)
			if err != nil {
				t.Fatalf("1回目のInsertAfterが失敗した: %v", err)
			}
			slot1, err := rp.InsertAfter(ctx, slot0)
			if err != nil {
				t.Fatalf("2回目のInsertAfterが失敗した: %v", err)
			}
			if slot0 != 0 || slot1 != 1 {
				t.Fatalf("slotは0,1であるべきだが%d,%dだった", slot0, slot1)
			}
		})

		t.Run("ページ満杯後のInsertAfterは-1を返しerrorではない", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			slot := int32(-1)
			count := 0
			for {
				next, err := rp.InsertAfter(ctx, slot)
				if err != nil {
					t.Fatalf("InsertAfterが失敗した: %v", err)
				}
				if next < 0 {
					break
				}
				slot = next
				count++
			}

			got, err := rp.InsertAfter(ctx, slot)
			if err != nil {
				t.Fatalf("満杯後のInsertAfterはerrorでないべきだが%vだった", err)
			}
			if got != -1 {
				t.Fatalf("満杯後のInsertAfterは-1を返すべきだが%dだった", got)
			}
			if count == 0 {
				t.Fatalf("少なくとも1件はInsertできるべき")
			}
		})

		t.Run("入るslot数はusable/slotSizeと一致する", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			layout := newTestLayout()
			want := (blocksize - file.PageHeaderSize) / layout.SlotSize()

			slot := int32(-1)
			var count int32
			for {
				next, err := rp.InsertAfter(ctx, slot)
				if err != nil {
					t.Fatalf("InsertAfterが失敗した: %v", err)
				}
				if next < 0 {
					break
				}
				slot = next
				count++
			}
			if count != want {
				t.Fatalf("Insertできる件数は%dであるべきだが%dだった", want, count)
			}
		})
	})

	t.Run("RecordPage: ページ境界", func(t *testing.T) {
		t.Parallel()

		t.Run("満杯ページの末尾の次のNextAfterは-1を返す", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			slot := int32(-1)
			last := int32(-1)
			for {
				next, err := rp.InsertAfter(ctx, slot)
				if err != nil {
					t.Fatalf("InsertAfterが失敗した: %v", err)
				}
				if next < 0 {
					break
				}
				last = next
				slot = next
			}
			if last < 0 {
				t.Fatalf("少なくとも1件はInsertできるべき")
			}

			got, err := rp.NextAfter(ctx, last)
			if err != nil {
				t.Fatalf("NextAfterが失敗した: %v", err)
			}
			if got != -1 {
				t.Fatalf("末尾の次のNextAfterは-1を返すべきだが%dだった", got)
			}
		})
	})

	t.Run("RecordPage: SetInt/GetInt", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

		slot, err := rp.InsertAfter(ctx, -1)
		if err != nil {
			t.Fatalf("InsertAfterが失敗した: %v", err)
		}
		const want int32 = 42
		if err := rp.SetInt(ctx, slot, "A", want); err != nil {
			t.Fatalf("SetIntが失敗した: %v", err)
		}

		got, err := rp.GetInt(ctx, slot, "A")
		if err != nil {
			t.Fatalf("GetIntが失敗した: %v", err)
		}
		if got != want {
			t.Fatalf("GetIntは%dであるべきだが%dだった", want, got)
		}
	})

	t.Run("RecordPage: SetSmallInt/GetSmallInt", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

		slot, err := rp.InsertAfter(ctx, -1)
		if err != nil {
			t.Fatalf("InsertAfterが失敗した: %v", err)
		}
		const want int16 = 6
		if err := rp.SetSmallInt(ctx, slot, "A", want); err != nil {
			t.Fatalf("SetSmallIntが失敗した: %v", err)
		}

		got, err := rp.GetSmallInt(ctx, slot, "A")
		if err != nil {
			t.Fatalf("GetSmallIntが失敗した: %v", err)
		}
		if got != want {
			t.Fatalf("GetSmallIntは%dであるべきだが%dだった", want, got)
		}
	})

	t.Run("RecordPage: SetBool/GetBool", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

		slot, err := rp.InsertAfter(ctx, -1)
		if err != nil {
			t.Fatalf("InsertAfterが失敗した: %v", err)
		}
		const want bool = true
		if err := rp.SetBool(ctx, slot, "A", want); err != nil {
			t.Fatalf("SetBoolが失敗した: %v", err)
		}

		got, err := rp.GetBool(ctx, slot, "A")
		if err != nil {
			t.Fatalf("GetBoolが失敗した: %v", err)
		}
		if got != want {
			t.Fatalf("GetBoolは%tであるべきだが%tだった", want, got)
		}
	})

	t.Run("RecordPage: SetDate/GetDate", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

		slot, err := rp.InsertAfter(ctx, -1)
		if err != nil {
			t.Fatalf("InsertAfterが失敗した: %v", err)
		}
		want := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
		if err := rp.SetDate(ctx, slot, "A", want); err != nil {
			t.Fatalf("SetDateが失敗した: %v", err)
		}

		got, err := rp.GetDate(ctx, slot, "A")
		if err != nil {
			t.Fatalf("GetDateが失敗した: %v", err)
		}
		if got != want {
			t.Fatalf("GetDateは%vであるべきだが%vだった", want, got)
		}
	})

	t.Run("RecordPage: SetStr/GetString", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

		slot, err := rp.InsertAfter(ctx, -1)
		if err != nil {
			t.Fatalf("InsertAfterが失敗した: %v", err)
		}
		const want = "rec42"
		if err := rp.SetStr(ctx, slot, "B", want); err != nil {
			t.Fatalf("SetStrが失敗した: %v", err)
		}

		got, err := rp.GetString(ctx, slot, "B")
		if err != nil {
			t.Fatalf("GetStringが失敗した: %v", err)
		}
		if got != want {
			t.Fatalf("GetStringは%qであるべきだが%qだった", want, got)
		}
	})

	t.Run("RecordPage: Delete", func(t *testing.T) {
		t.Parallel()

		t.Run("削除したslotはNextAfterで見つからない", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			slot, err := rp.InsertAfter(ctx, -1)
			if err != nil {
				t.Fatalf("InsertAfterが失敗した: %v", err)
			}
			if err := rp.Delete(ctx, slot); err != nil {
				t.Fatalf("Deleteが失敗した: %v", err)
			}

			got, err := rp.NextAfter(ctx, -1)
			if err != nil {
				t.Fatalf("NextAfterが失敗した: %v", err)
			}
			if got != -1 {
				t.Fatalf("NextAfterは-1を返すべきだが%dだった", got)
			}
		})

		t.Run("削除したslotはInsertAfterで再利用できる", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			slot, err := rp.InsertAfter(ctx, -1)
			if err != nil {
				t.Fatalf("InsertAfterが失敗した: %v", err)
			}
			if err := rp.Delete(ctx, slot); err != nil {
				t.Fatalf("Deleteが失敗した: %v", err)
			}

			reused, err := rp.InsertAfter(ctx, -1)
			if err != nil {
				t.Fatalf("再利用のInsertAfterが失敗した: %v", err)
			}
			if reused != slot {
				t.Fatalf("再利用slotは%dであるべきだが%dだった", slot, reused)
			}
		})
	})

	t.Run("RecordPage: 複数レコードの走査", func(t *testing.T) {
		t.Parallel()

		t.Run("NextAfterはUSEDスロットを順に返す", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			s0, err := rp.InsertAfter(ctx, -1)
			if err != nil {
				t.Fatalf("InsertAfter(0)が失敗した: %v", err)
			}
			s1, err := rp.InsertAfter(ctx, s0)
			if err != nil {
				t.Fatalf("InsertAfter(1)が失敗した: %v", err)
			}
			if err := rp.SetInt(ctx, s0, "A", 10); err != nil {
				t.Fatalf("SetInt(s0)が失敗した: %v", err)
			}
			if err := rp.SetInt(ctx, s1, "A", 20); err != nil {
				t.Fatalf("SetInt(s1)が失敗した: %v", err)
			}

			first, err := rp.NextAfter(ctx, -1)
			if err != nil {
				t.Fatalf("1回目のNextAfterが失敗した: %v", err)
			}
			second, err := rp.NextAfter(ctx, first)
			if err != nil {
				t.Fatalf("2回目のNextAfterが失敗した: %v", err)
			}
			third, err := rp.NextAfter(ctx, second)
			if err != nil {
				t.Fatalf("3回目のNextAfterが失敗した: %v", err)
			}
			if first != s0 || second != s1 || third != -1 {
				t.Fatalf("走査結果は%d,%d,-1であるべきだが%d,%d,%dだった", s0, s1, first, second, third)
			}
		})

		t.Run("途中のslotをDeleteしても残りを走査できる", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, rp := newFormattedRecordPage(t, newRpTestEnv(t))

			s0, err := rp.InsertAfter(ctx, -1)
			if err != nil {
				t.Fatalf("InsertAfter(0)が失敗した: %v", err)
			}
			s1, err := rp.InsertAfter(ctx, s0)
			if err != nil {
				t.Fatalf("InsertAfter(1)が失敗した: %v", err)
			}
			if err := rp.SetInt(ctx, s1, "A", 20); err != nil {
				t.Fatalf("SetInt(s1)が失敗した: %v", err)
			}
			if err := rp.Delete(ctx, s0); err != nil {
				t.Fatalf("Deleteが失敗した: %v", err)
			}

			slot, err := rp.NextAfter(ctx, -1)
			if err != nil {
				t.Fatalf("NextAfterが失敗した: %v", err)
			}
			if slot != s1 {
				t.Fatalf("NextAfterは%dを返すべきだが%dだった", s1, slot)
			}
			got, err := rp.GetInt(ctx, slot, "A")
			if err != nil {
				t.Fatalf("GetIntが失敗した: %v", err)
			}
			if got != 20 {
				t.Fatalf("残ったレコードのAは20であるべきだが%dだった", got)
			}
		})
	})
}
