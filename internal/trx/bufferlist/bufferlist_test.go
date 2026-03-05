package bufferlist

import (
	"context"
	"testing"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

const (
	blocksize = int32(256)
	logfile   = "logfile"
	numbuffs  = 10
	numwaits  = 10
)

func initMgr(t *testing.T, dir string) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *buffer.DirtyPageTable) {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}

	fileMgr, err := file.NewFileMgr(dir, blocksize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}

	logMgr, err := log.NewLogMgr(fileMgr, logfile)
	if err != nil {
		t.Fatalf("Failed to create LogMgr: %v", err)
	}

	dpt := buffer.NewDirtyPageTable()
	bufferMgr := buffer.NewBufferMgr(fileMgr, logMgr, numbuffs, numwaits, dpt)
	return fileMgr, logMgr, bufferMgr, dpt
}

type bufferListTestEnv struct {
	fm  *file.FileMgr
	lm  *log.LogMgr
	bm  *buffer.BufferMgr
	dpt *buffer.DirtyPageTable
	bl  *BufferList
}

func newBufferListTestEnv(t *testing.T, dir string) *bufferListTestEnv {
	t.Helper()
	fm, lm, bm, dpt := initMgr(t, dir)
	return &bufferListTestEnv{
		fm:  fm,
		lm:  lm,
		bm:  bm,
		dpt: dpt,
		bl:  NewBufferList(bm),
	}
}

func TestBufferList(t *testing.T) {
	t.Parallel()

	t.Run("NewBufferList: 生成直後は未Pinブロックに対してGetBufferがnilを返す", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		blk := file.NewBlockId("testfile", 0)

		if bl == nil {
			t.Fatal("NewBufferList returned nil")
		}
		if bl.GetBuffer(blk) != nil {
			t.Fatal("expected nil for unpinned block")
		}
	})

	t.Run("GetBuffer: 未Pinブロックを指定するとnilを返す", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		blk := file.NewBlockId("testfile", 0)

		buff := bl.GetBuffer(blk)
		if buff != nil {
			t.Error("GetBuffer should return nil for non-existent block")
		}
	})

	t.Run("Pin: 未PinブロックをPinするとGetBufferで同じブロックのバッファを取得できる", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		buff := bl.GetBuffer(blk)
		if buff == nil {
			t.Error("GetBuffer should return buffer for pinned block")
		}

		if buff.Block() == nil || !buff.Block().Equals(blk) {
			t.Error("Buffer should be associated with the correct block")
		}
	})

	t.Run("Pin: 未PinブロックをPinすると利用可能バッファ数が1減る", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		bm := env.bm
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)
		before := bm.Available()

		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		if bm.Available() != before-1 {
			t.Fatalf("expected available %d, got %d", before-1, bm.Available())
		}
	})

	t.Run("Pin: 同一ブロックを重ねてPinしても利用可能バッファ数は追加で減らない", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		bm := env.bm
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}
		afterFirstPin := bm.Available()

		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin same block again: %v", err)
		}

		if bm.Available() != afterFirstPin {
			t.Fatalf("expected available %d, got %d", afterFirstPin, bm.Available())
		}
	})

	t.Run("Unpin: 1回PinしたブロックをUnpinするとGetBufferがnilになる", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Pin a block
		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		bl.Unpin(ctx, blk)

		if bl.GetBuffer(blk) != nil {
			t.Fatal("expected nil after unpin")
		}
	})

	t.Run("Unpin: 同一ブロックを2回Pin後に1回UnpinしてもGetBufferで取得できる", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Pin a block twice
		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block again: %v", err)
		}

		bl.Unpin(ctx, blk)

		if bl.GetBuffer(blk) == nil {
			t.Fatal("expected buffer to remain after first unpin")
		}
	})

	t.Run("Unpin: 同一ブロックを2回Pin後に2回UnpinするとGetBufferがnilになる", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}
		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block again: %v", err)
		}

		bl.Unpin(ctx, blk)
		bl.Unpin(ctx, blk)

		if bl.GetBuffer(blk) != nil {
			t.Fatal("expected nil after second unpin")
		}
	})

	t.Run("UnpinAll: 複数ブロックをPin中に呼ぶと全ブロックでGetBufferがnilになる", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		ctx := context.Background()

		blk1 := file.NewBlockId("testfile1", 0)
		blk2 := file.NewBlockId("testfile1", 1)
		blk3 := file.NewBlockId("testfile2", 0)

		err := bl.Pin(ctx, blk1)
		if err != nil {
			t.Fatalf("Failed to pin testfile1 block1: %v", err)
		}

		err = bl.Pin(ctx, blk2)
		if err != nil {
			t.Fatalf("Failed to pin testfile1 block2: %v", err)
		}

		err = bl.Pin(ctx, blk3)
		if err != nil {
			t.Fatalf("Failed to pin testfile2 block3: %v", err)
		}

		bl.UnpinAll(ctx)

		if bl.GetBuffer(blk1) != nil || bl.GetBuffer(blk2) != nil || bl.GetBuffer(blk3) != nil {
			t.Fatal("expected all buffers to be nil after UnpinAll")
		}
	})

	t.Run("UnpinAll: 同一ブロックを複数回Pin中に呼ぶと利用可能バッファ数が元に戻る", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		bm := env.bm
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)
		before := bm.Available()

		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block again: %v", err)
		}

		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block third time: %v", err)
		}

		bl.UnpinAll(ctx)

		if bm.Available() != before {
			t.Fatalf("expected available %d, got %d", before, bm.Available())
		}
		if bl.GetBuffer(blk) != nil {
			t.Fatal("expected nil after UnpinAll")
		}
	})

	t.Run("Unpin: 未Pinブロックを指定しても利用可能バッファ数は変化しない", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		bm := env.bm
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)
		before := bm.Available()

		bl.Unpin(ctx, blk)

		if bm.Available() != before {
			t.Fatalf("expected available %d, got %d", before, bm.Available())
		}
		if bl.GetBuffer(blk) != nil {
			t.Fatal("expected nil for unpinned block")
		}
	})
}

func TestBufferListConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("Pin/Unpin: 異なるブロックで並行実行しても最終的に利用可能バッファ数が元に戻る", func(t *testing.T) {
		t.Parallel()

		env := newBufferListTestEnv(t, "")
		bl := env.bl
		bm := env.bm
		ctx := context.Background()
		before := bm.Available()
		blk1 := file.NewBlockId("testfile1", 0)
		blk2 := file.NewBlockId("testfile2", 0)

		done := make(chan bool, 2)

		go func() {
			defer func() { done <- true }()
			for i := 0; i < 10; i++ {
				err := bl.Pin(ctx, blk1)
				if err != nil {
					t.Errorf("Failed to pin in goroutine 1: %v", err)
					return
				}
				bl.Unpin(ctx, blk1)
			}
		}()

		go func() {
			defer func() { done <- true }()
			for i := 0; i < 10; i++ {
				err := bl.Pin(ctx, blk2)
				if err != nil {
					t.Errorf("Failed to pin in goroutine 2: %v", err)
					return
				}
				bl.Unpin(ctx, blk2)
			}
		}()

		<-done
		<-done

		if bm.Available() != before {
			t.Fatalf("expected available %d, got %d", before, bm.Available())
		}
		if bl.GetBuffer(blk1) != nil || bl.GetBuffer(blk2) != nil {
			t.Fatal("expected nil buffers after concurrent operations")
		}
	})
}
