package buffer

import (
	"testing"

	"github.com/keingoon/simpledb/internal/file"
)

func TestDirtyPageTable(t *testing.T) {
	t.Parallel()

	t.Run("DirtyPageTable: NewDirtyPageTableの初期状態", func(t *testing.T) {
		t.Parallel()
		dpt := NewDirtyPageTable()

		blk := file.NewBlockId("test", 1)

		t.Run("GetPageはfalseを返す", func(t *testing.T) {
			t.Parallel()
			_, ok := dpt.GetPage(blk)
			if ok {
				t.Fatalf("GetPageはfalseを返すべき")
			}
		})

		t.Run("GetRecLSNは(-1,false)を返す", func(t *testing.T) {
			t.Parallel()
			lsn, ok := dpt.GetRecLSN(blk)
			if ok || lsn != -1 {
				t.Fatalf("GetRecLSNは(-1,false)を返すべきだが(%d,%v)だった", lsn, ok)
			}
		})

		t.Run("GetMinRecLSNは(-1,false)を返す", func(t *testing.T) {
			t.Parallel()
			lsn, ok := dpt.GetMinRecLSN()
			if ok || lsn != -1 {
				t.Fatalf("GetMinRecLSNは(-1,false)を返すべきだが(%d,%v)だった", lsn, ok)
			}
		})

		t.Run("GetSnapshotTableは空のmapを返す", func(t *testing.T) {
			t.Parallel()
			snap := dpt.GetSnapshotTable()
			if snap == nil {
				t.Fatalf("snapshotはnilでないべき")
			}
			if len(snap) != 0 {
				t.Fatalf("snapshotは空であるべきだが%vだった", len(snap))
			}
		})
	})

	t.Run("DirtyPageTable: MarkDirty", func(t *testing.T) {
		t.Parallel()
		dpt := NewDirtyPageTable()
		blk := file.NewBlockId("test", 1)
		recLSN1 := int32(10)
		recLSN2 := int32(20)

		dpt.MarkDirty(blk, recLSN1)

		t.Run("GetRecLSNは指定した値を返す", func(t *testing.T) {
			t.Parallel()
			lsn, ok := dpt.GetRecLSN(blk)
			if !ok || lsn != recLSN1 {
				t.Fatalf("GetRecLSNは(%d,true)を返すべきだが(%d,%v)だった", recLSN1, lsn, ok)
			}
		})

		t.Run("GetPageはtrueを返し、entryは指定したrecLSNを持つ", func(t *testing.T) {
			t.Parallel()
			entry, ok := dpt.GetPage(blk)
			if !ok {
				t.Fatalf("GetPageはtrueを返すべき")
			}
			if got := entry.GetRecLSN(); got != recLSN1 {
				t.Fatalf("entry.recLSNは%dであるべきだが%dだった", recLSN1, got)
			}
		})

		t.Run("同一blkを2回MarkDirtyしても上書きしない", func(t *testing.T) {
			t.Parallel()
			dpt.MarkDirty(blk, recLSN2)
			lsn, ok := dpt.GetRecLSN(blk)
			if !ok || lsn != recLSN1 {
				t.Fatalf("GetRecLSNは(%d,true)を返すべきだが(%d,%v)だった", recLSN1, lsn, ok)
			}
		})
	})

	t.Run("DirtyPageTable: Clean", func(t *testing.T) {
		t.Parallel()
		dpt := NewDirtyPageTable()
		blk1 := file.NewBlockId("test", 1)
		blk2 := file.NewBlockId("test", 2)
		dpt.MarkDirty(blk1, 10)
		dpt.MarkDirty(blk2, 20)

		t.Run("存在するblkをCleanするとGetRecLSNは(-1,false)になる", func(t *testing.T) {
			t.Parallel()
			dpt.Clean(blk1)
			lsn, ok := dpt.GetRecLSN(blk1)
			if ok || lsn != -1 {
				t.Fatalf("GetRecLSNは(-1,false)を返すべきだが(%d,%v)だった", lsn, ok)
			}
		})

		t.Run("存在しないblkをCleanしても落ちない", func(t *testing.T) {
			t.Parallel()
			dpt.Clean(file.NewBlockId("test", 999))
		})
	})

	t.Run("DirtyPageTable: GetMinRecLSN", func(t *testing.T) {
		t.Parallel()

		t.Run("空なら(-1,false)を返す", func(t *testing.T) {
			t.Parallel()
			dpt := NewDirtyPageTable()
			lsn, ok := dpt.GetMinRecLSN()
			if ok || lsn != -1 {
				t.Fatalf("GetMinRecLSNは(-1,false)を返すべきだが(%d,%v)だった", lsn, ok)
			}
		})

		t.Run("1件ならそのrecLSNを返す", func(t *testing.T) {
			t.Parallel()
			dpt := NewDirtyPageTable()
			blk := file.NewBlockId("test", 1)
			dpt.MarkDirty(blk, 10)
			lsn, ok := dpt.GetMinRecLSN()
			if !ok || lsn != 10 {
				t.Fatalf("GetMinRecLSNは(10,true)を返すべきだが(%d,%v)だった", lsn, ok)
			}
		})

		t.Run("複数件なら最小recLSNを返す", func(t *testing.T) {
			t.Parallel()
			dpt := NewDirtyPageTable()
			dpt.MarkDirty(file.NewBlockId("test", 1), 20)
			dpt.MarkDirty(file.NewBlockId("test", 2), 10)
			dpt.MarkDirty(file.NewBlockId("test", 3), 30)
			lsn, ok := dpt.GetMinRecLSN()
			if !ok || lsn != 10 {
				t.Fatalf("GetMinRecLSNは(10,true)を返すべきだが(%d,%v)だった", lsn, ok)
			}
		})

		t.Run("Clean後に最小recLSNが更新される", func(t *testing.T) {
			t.Parallel()
			dpt := NewDirtyPageTable()
			blkMin := file.NewBlockId("test", 1)
			blkOther := file.NewBlockId("test", 2)
			dpt.MarkDirty(blkMin, 10)
			dpt.MarkDirty(blkOther, 20)
			dpt.Clean(blkMin)
			lsn, ok := dpt.GetMinRecLSN()
			if !ok || lsn != 20 {
				t.Fatalf("GetMinRecLSNは(20,true)を返すべきだが(%d,%v)だった", lsn, ok)
			}
		})
	})

	t.Run("DirtyPageTable: GetSnapshotTable", func(t *testing.T) {
		t.Parallel()
		dpt := NewDirtyPageTable()
		blk1 := file.NewBlockId("test", 1)
		blk2 := file.NewBlockId("test", 2)
		dpt.MarkDirty(blk1, 10)

		t.Run("snapshotを変更しても本体に影響しない", func(t *testing.T) {
			t.Parallel()
			snap := dpt.GetSnapshotTable()
			delete(snap, *blk1)
			_, ok := dpt.GetPage(blk1)
			if !ok {
				t.Fatalf("snapshotの変更で本体が消えてはいけない")
			}
		})

		t.Run("snapshotは呼んだ時点の内容で固定される", func(t *testing.T) {
			t.Parallel()
			snap := dpt.GetSnapshotTable()
			dpt.MarkDirty(blk2, 20)
			if _, ok := snap[*blk2]; ok {
				t.Fatalf("snapshotには後から追加したblkが含まれてはいけない")
			}
		})
	})

	t.Run("DirtyPageTable: ReplaceSnapshot", func(t *testing.T) {
		t.Parallel()

		t.Run("snapshotの内容で全置換される", func(t *testing.T) {
			t.Parallel()
			dpt := NewDirtyPageTable()
			oldBlk := file.NewBlockId("test", 1)
			newBlk := file.NewBlockId("test", 2)
			dpt.MarkDirty(oldBlk, 10)

			snapshot := map[file.BlockId]DirtyPageEntry{
				*newBlk: NewDirtyPageEntry(*newBlk, 20),
			}
			dpt.ReplaceSnapshot(snapshot)

			if _, ok := dpt.GetPage(oldBlk); ok {
				t.Fatalf("old block should be removed by ReplaceSnapshot")
			}
			entry, ok := dpt.GetPage(newBlk)
			if !ok {
				t.Fatalf("new block should exist after ReplaceSnapshot")
			}
			if got := entry.GetRecLSN(); got != 20 {
				t.Fatalf("expected recLSN=20, got %d", got)
			}
		})

		t.Run("引数snapshotを後で変更しても内部状態が変わらない", func(t *testing.T) {
			t.Parallel()
			dpt := NewDirtyPageTable()
			blk := file.NewBlockId("test", 3)

			snapshot := map[file.BlockId]DirtyPageEntry{
				*blk: NewDirtyPageEntry(*blk, 30),
			}
			dpt.ReplaceSnapshot(snapshot)

			delete(snapshot, *blk)
			if _, ok := dpt.GetPage(blk); !ok {
				t.Fatalf("internal table should not be affected by snapshot mutation")
			}
		})
	})
}
