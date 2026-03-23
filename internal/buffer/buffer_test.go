package buffer

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

const int32Size = 4

func initFileLogMgr(dir string, blocksize int32, logfile string) (*file.FileMgr, *log.LogMgr, error) {
	fileMgr, err := file.NewFileMgr(dir, blocksize)
	if err != nil {
		return nil, nil, err
	}
	logMgr, err := log.NewLogMgr(fileMgr, logfile)
	if err != nil {
		return nil, nil, err
	}
	return fileMgr, logMgr, nil
}

func createLogRec(i int32, str string) []byte {
	recstrlen := len([]rune(str))
	rec := make([]byte, int32Size+file.VarBytesLen(recstrlen))
	strpos := int32(0)
	intpos := int32(int32Size + file.VarBytesLen(recstrlen))
	p := file.NewLogPage(rec)
	p.SetStr(strpos, str)
	p.SetInt32(intpos, i)
	return rec
}

func readPageFromDisk(t *testing.T, fm *file.FileMgr, blk *file.BlockId) *file.Page {
	t.Helper()

	p := file.NewPage(fm.BlockSize())
	if err := fm.Read(blk, p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBuffer(t *testing.T) {
	t.Parallel()

	t.Run("Buffer: NewBufferの初期状態", func(t *testing.T) {
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		newBuf := func(t *testing.T) *Buffer {
			t.Helper()
			fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
			if err != nil {
				t.Fatal(err)
			}
			dpt := NewDirtyPageTable()
			return NewBuffer(fm, lm, dpt)
		}

		t.Run("Contentsはnilでない", func(t *testing.T) {
			t.Parallel()
			if newBuf(t).Contents() == nil {
				t.Errorf("Contentsはnilでないべき")
			}
		})

		t.Run("Blockはnilである", func(t *testing.T) {
			t.Parallel()
			if got := newBuf(t).Block(); got != nil {
				t.Errorf("Blockはnilであるべきだが%vだった", got)
			}
		})

		t.Run("IsPinnedはfalseである", func(t *testing.T) {
			t.Parallel()
			if newBuf(t).IsPinned() {
				t.Errorf("IsPinnedはfalseであるべきだがtrueだった")
			}
		})

		t.Run("ModifyingTxは-1である", func(t *testing.T) {
			t.Parallel()
			if got := newBuf(t).ModifyingTx(); got != -1 {
				t.Errorf("ModifyingTxは-1であるべきだが%vだった", got)
			}
		})

		t.Run("pageLSNは0である", func(t *testing.T) {
			t.Parallel()
			if got := newBuf(t).Contents().GetPageLSN(); got != 0 {
				t.Errorf("pageLSNは0であるべきだが%vだった", got)
			}
		})
	})

	t.Run("Buffer: SetModifiedはtxnumを更新する", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			txnum     = int32(1)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		b, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		defer mgr.Unpin(ctx, b)

		b.SetModified(txnum, -1)
		if got := b.ModifyingTx(); got != txnum {
			t.Errorf("ModifyingTxは%vであるべきだが%vだった", txnum, got)
		}
	})

	t.Run("Buffer: SetModifiedは正のLSNでpageLSNを更新する", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			txnum     = int32(1)
			lsn       = int32(10)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		b, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		defer mgr.Unpin(ctx, b)

		b.SetModified(txnum, lsn)
		if got := b.Contents().GetPageLSN(); got != lsn {
			t.Errorf("pageLSNは%vであるべきだが%vだった", lsn, got)
		}
	})

	t.Run("Buffer: SetModifiedは負のLSNではpageLSNを更新しない", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			txnum     = int32(1)
			lsn       = int32(-2)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		b, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		defer mgr.Unpin(ctx, b)

		b.SetModified(txnum, lsn)
		if got := b.Contents().GetPageLSN(); got != 0 {
			t.Errorf("pageLSNは更新されず0のままであるべきだが%vだった", got)
		}
	})

	t.Run("Buffer: SetModifiedは負のLSNではrecLSNを更新しない", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			txnum     = int32(1)
			lsn       = int32(-2)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		b, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		defer mgr.Unpin(ctx, b)

		b.SetModified(txnum, lsn)
		if got := b.recLSN; got != -1 {
			t.Errorf("recLSNは更新されず-1のままであるべきだが%vだった", got)
		}
	})

	t.Run("Buffer: SetModifiedは負のLSNではdirtyPageTableのrecLSNを更新しない", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			txnum     = int32(1)
			lsn       = int32(-2)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		b, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		defer mgr.Unpin(ctx, b)

		b.SetModified(txnum, lsn)
		if got, _ := dpt.GetRecLSN(b.Block()); got != -1 {
			t.Errorf("recLSNは更新されず-1のままであるべきだが%vだった", got)
		}
	})

	t.Run("Buffer: bufferが未変更の場合SetModifiedはrecLSNを更新する", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			txnum     = int32(1)
			lsn       = int32(10)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		b, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		defer mgr.Unpin(ctx, b)

		b.SetModified(txnum, lsn)
		if got := b.recLSN; got != lsn {
			t.Errorf("recLSNは%vであるべきだが%vだった", lsn, got)
		}
	})

	t.Run("Buffer: bufferが未変更の場合SetModifiedはdirtyPageTableのrecLSNを更新する", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			txnum     = int32(1)
			lsn       = int32(10)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		b, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		defer mgr.Unpin(ctx, b)

		b.SetModified(txnum, lsn)
		if got, _ := dpt.GetRecLSN(b.Block()); got != lsn {
			t.Errorf("recLSNは%vであるべきだが%vだった", lsn, got)
		}
	})
}

func TestBufferMgr(t *testing.T) {
	t.Parallel()

	t.Run("BufferMgr: NewBufferMgrの初期Availableはnumbuffsである", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = int32(3)
			numwaits  = int32(3)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		if got := mgr.Available(); got != numbuffs {
			t.Errorf("Availableは%vであるべきだが%vだった", numbuffs, got)
		}
	})

	t.Run("BufferMgr: Pin/Unpinの基本", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(3)
			numwaits  = int32(3)
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		b, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}

		t.Run("PinするとAvailableが1減る", func(t *testing.T) {
			if got := mgr.Available(); got != numbuffs-1 {
				t.Errorf("Pin後のAvailableは%vであるべきだが%vだった", numbuffs-1, got)
			}
		})

		t.Run("Pinすると返るBufferはpinnedである", func(t *testing.T) {
			if !b.IsPinned() {
				t.Errorf("Pin後のIsPinnedはtrueであるべきだがfalseだった")
			}
		})

		t.Run("Pinで返るBufferのBlockは指定blkである", func(t *testing.T) {
			gotBlk := b.Block()
			if gotBlk == nil || gotBlk.FileName() != blk.FileName() || gotBlk.Number() != blk.Number() {
				gotFile, gotNum := "<nil>", int32(-1)
				if gotBlk != nil {
					gotFile, gotNum = gotBlk.FileName(), gotBlk.Number()
				}
				t.Errorf("Pinで返るBufferのBlockは(%s,%d)であるべきだが(%s,%d)だった", blk.FileName(), blk.Number(), gotFile, gotNum)
			}
		})

		// Unpin後の状態
		mgr.Unpin(ctx, b)

		t.Run("UnpinするとAvailableが1増える", func(t *testing.T) {
			if got := mgr.Available(); got != numbuffs {
				t.Errorf("Unpin後のAvailableは%vであるべきだが%vだった", numbuffs, got)
			}
		})

		t.Run("UnpinするとBufferはunpinnedになる", func(t *testing.T) {
			if b.IsPinned() {
				t.Errorf("Unpin後のIsPinnedはfalseであるべきだがtrueだった")
			}
		})
	})

	t.Run("BufferMgr: 全バッファがpin中だとPinは待機し、Unpinで進む", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(1)
			numwaits  = int32(1)
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		mgr.Maxtime = 500 // ms

		blk1, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		blk2, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}

		bPinned, err := mgr.Pin(ctx, blk1)
		if err != nil {
			t.Fatal(err)
		}

		done := make(chan error, 1)
		go func() {
			_, err := mgr.Pin(ctx, blk2)
			done <- err
		}()

		t.Run("Unpin前は待機する", func(t *testing.T) {
			select {
			case err := <-done:
				t.Errorf("Unpin前にPinが完了してしまった（err=%v）", err)
			case <-time.After(30 * time.Millisecond):
				// まだ待っているのが期待
			}
		})

		t.Run("Unpinすると待機中のPinが完了する", func(t *testing.T) {
			mgr.Unpin(ctx, bPinned)
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Unpin後のPinは成功すべきだが%vだった", err)
				}
			case <-time.After(300 * time.Millisecond):
				t.Errorf("Unpin後もPinが完了しない")
			}
		})
	})

	t.Run("BufferMgr: 全バッファがpin中だとPinはタイムアウトする", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(1)
			numwaits  = int32(1)
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)
		mgr.Maxtime = 50 // ms

		blk1, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		blk2, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}

		_, err = mgr.Pin(ctx, blk1)
		if err != nil {
			t.Fatal(err)
		}

		_, err = mgr.Pin(ctx, blk2)
		want := errors.New("buffer abort exception")
		if err == nil || err.Error() != want.Error() {
			t.Errorf("エラーは%vであるべきだが%vだった", want, err)
		}
	})

	type flushAllFixture struct {
		ctx context.Context
		tx1 int32
		tx2 int32

		fm  *file.FileMgr
		lm  *log.LogMgr
		mgr *BufferMgr

		dpt *DirtyPageTable

		blk1 *file.BlockId
		blk2 *file.BlockId
		b1   *Buffer
		b2   *Buffer

		logrec1 []byte
		logrec2 []byte
		lsn1    int32
		lsn2    int32
	}

	setupFlushAll := func(t *testing.T) flushAllFixture {
		t.Helper()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			tx1       = int32(1)
			tx2       = int32(2)
		)

		f := flushAllFixture{
			ctx: context.Background(),
			tx1: tx1,
			tx2: tx2,
		}

		var err error
		f.fm, f.lm, err = initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		f.dpt = dpt
		f.mgr = NewBufferMgr(f.fm, f.lm, numbuffs, numwaits, dpt)

		f.blk1, err = f.fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}

		f.blk2, err = f.fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}

		f.b1, err = f.mgr.Pin(f.ctx, f.blk1)
		if err != nil {
			t.Fatal(err)
		}
		f.b2, err = f.mgr.Pin(f.ctx, f.blk2)
		if err != nil {
			t.Fatal(err)
		}

		f.logrec1 = createLogRec(1, "record1")
		f.lsn1, err = f.lm.Append(f.logrec1)
		if err != nil {
			t.Fatal(err)
		}
		b1p := f.b1.Contents()
		b1p.SetInt32(0, 1)
		b1p.SetStr(int32Size, "record1")
		f.b1.SetModified(f.tx1, f.lsn1)

		f.logrec2 = createLogRec(2, "record2")
		f.lsn2, err = f.lm.Append(f.logrec2)
		if err != nil {
			t.Fatal(err)
		}
		b2p := f.b2.Contents()
		b2p.SetInt32(0, 2)
		b2p.SetStr(int32Size, "record2")
		f.b2.SetModified(f.tx2, f.lsn2)

		f.mgr.FlushAll(f.ctx, f.tx1)
		return f
	}

	t.Run("BufferMgr: FlushPageはdirtyな対象ページをflushする", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize    = int32(256)
			logfile      = "logfile"
			filename     = "testfile"
			numbuffs     = int32(2)
			numwaits     = int32(2)
			txnum        = int32(1)
			wantInt32    = int32(1)
			wantStr      = "record1"
			targetOffset = int32(0)
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		logrec := createLogRec(wantInt32, wantStr)
		lsn, err := lm.Append(logrec)
		if err != nil {
			t.Fatal(err)
		}
		p := buff.Contents()
		p.SetInt32(targetOffset, wantInt32)
		p.SetStr(int32Size, wantStr)
		buff.SetModified(txnum, lsn)
		mgr.Unpin(ctx, buff)

		flushed, err := mgr.FlushPage(ctx, blk)
		if err != nil {
			t.Fatalf("FlushPageが失敗した: %v", err)
		}
		if !flushed {
			t.Fatal("dirtyな対象ページはflushされるべき")
		}

		onDiskPage := readPageFromDisk(t, fm, blk)
		if got := onDiskPage.GetInt32(targetOffset); got != wantInt32 {
			t.Errorf("blk1 int32(0)は%vであるべきだが%vだった", wantInt32, got)
		}
		if got := onDiskPage.GetStr(int32Size); got != wantStr {
			t.Errorf("blk1 str(%d)は%qであるべきだが%qだった", int32Size, wantStr, got)
		}
		if got := onDiskPage.GetPageLSN(); got != lsn {
			t.Errorf("blk1 pageLSNは%vであるべきだが%vだった", lsn, got)
		}
	})

	t.Run("BufferMgr: FlushPageはbuffer poolに存在しないページではfalse,nilを返す", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

		blkInBufferPool, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff, err := mgr.Pin(ctx, blkInBufferPool)
		if err != nil {
			t.Fatal(err)
		}
		logrec := createLogRec(1, "record1")
		lsn, err := lm.Append(logrec)
		if err != nil {
			t.Fatal(err)
		}
		buff.Contents().SetInt32(0, 1)
		buff.Contents().SetStr(int32Size, "record1")
		buff.SetModified(1, lsn)

		missingBlk, err := fm.Append("missingfile")
		if err != nil {
			t.Fatal(err)
		}

		flushed, err := mgr.FlushPage(ctx, missingBlk)
		if err != nil {
			t.Fatalf("FlushPageは失敗しないべきだが%vだった", err)
		}
		if flushed {
			t.Fatal("buffer poolに存在しないページはflushされるべきではない")
		}
	})

	t.Run("BufferMgr: FlushPageはpinnedなページをflushしない", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = int32(2)
			numwaits  = int32(2)
			txnum     = int32(1)
			wantInt32 = int32(1)
			wantStr   = "record1"
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff, err := mgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		logrec := createLogRec(wantInt32, wantStr)
		lsn, err := lm.Append(logrec)
		if err != nil {
			t.Fatal(err)
		}
		buff.Contents().SetInt32(0, wantInt32)
		buff.Contents().SetStr(int32Size, wantStr)
		buff.SetModified(txnum, lsn)

		flushed, err := mgr.FlushPage(ctx, blk)
		if err != nil {
			t.Fatalf("FlushPageは失敗しないべきだが%vだった", err)
		}
		if flushed {
			t.Fatal("pinnedなページはflushされるべきではない")
		}
		if got := buff.recLSN; got != lsn {
			t.Errorf("pinnedページのrecLSNは%vのままであるべきだが%vだった", lsn, got)
		}
		if _, ok := dpt.GetPage(blk); !ok {
			t.Fatal("pinnedページのDPT entryは残るべき")
		}
	})

	t.Run("BufferMgr: FlushDirtyPagesはrecLSNの小さい順にflushする", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize             = int32(256)
			logfile               = "logfile"
			filename              = "testfile"
			numbuffs              = int32(2)
			numwaits              = int32(2)
			firstTxnum            = int32(1)
			secondTxnum           = int32(2)
			firstWantInt32        = int32(1)
			secondWantInt32       = int32(2)
			firstWantStr          = "record1"
			secondWantStr         = "record2"
			wantInitialOnDiskInt32 = int32(0)
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

		blk1, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff1, err := mgr.Pin(ctx, blk1)
		if err != nil {
			t.Fatal(err)
		}
		logrec1 := createLogRec(firstWantInt32, firstWantStr)
		lsn1, err := lm.Append(logrec1)
		if err != nil {
			t.Fatal(err)
		}
		buff1.Contents().SetInt32(0, firstWantInt32)
		buff1.Contents().SetStr(int32Size, firstWantStr)
		buff1.SetModified(firstTxnum, lsn1)
		mgr.Unpin(ctx, buff1)

		blk2, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff2, err := mgr.Pin(ctx, blk2)
		if err != nil {
			t.Fatal(err)
		}
		logrec2 := createLogRec(secondWantInt32, secondWantStr)
		lsn2, err := lm.Append(logrec2)
		if err != nil {
			t.Fatal(err)
		}
		buff2.Contents().SetInt32(0, secondWantInt32)
		buff2.Contents().SetStr(int32Size, secondWantStr)
		buff2.SetModified(secondTxnum, lsn2)
		mgr.Unpin(ctx, buff2)

		flushed, err := mgr.FlushDirtyPages(ctx, 1)
		if err != nil {
			t.Fatalf("FlushDirtyPagesが失敗した: %v", err)
		}
		if flushed != 1 {
			t.Fatalf("flush件数は1であるべきだが%vだった", flushed)
		}

		page1OnDisk := readPageFromDisk(t, fm, blk1)
		if got := page1OnDisk.GetInt32(0); got != firstWantInt32 {
			t.Errorf("最小recLSNのページはint32(0)=%vであるべきだが%vだった", firstWantInt32, got)
		}

		page2OnDisk := readPageFromDisk(t, fm, blk2)
		if got := page2OnDisk.GetInt32(0); got != wantInitialOnDiskInt32 {
			t.Errorf("後続ページは未flushなのでint32(0)=%vであるべきだが%vだった", wantInitialOnDiskInt32, got)
		}
	})

	t.Run("BufferMgr: FlushDirtyPagesはlimit件だけflushする", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize              = int32(256)
			logfile                = "logfile"
			filename               = "testfile"
			numbuffs               = int32(3)
			numwaits               = int32(3)
			firstTxnum             = int32(1)
			secondTxnum            = int32(2)
			thirdTxnum             = int32(3)
			firstWantInt32         = int32(1)
			secondWantInt32        = int32(2)
			thirdWantInt32         = int32(3)
			firstWantStr           = "record1"
			secondWantStr          = "record2"
			thirdWantStr           = "record3"
			wantInitialOnDiskInt32 = int32(0)
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		dpt := NewDirtyPageTable()
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

		blk1, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff1, err := mgr.Pin(ctx, blk1)
		if err != nil {
			t.Fatal(err)
		}
		logrec1 := createLogRec(firstWantInt32, firstWantStr)
		lsn1, err := lm.Append(logrec1)
		if err != nil {
			t.Fatal(err)
		}
		buff1.Contents().SetInt32(0, firstWantInt32)
		buff1.Contents().SetStr(int32Size, firstWantStr)
		buff1.SetModified(firstTxnum, lsn1)
		mgr.Unpin(ctx, buff1)

		blk2, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff2, err := mgr.Pin(ctx, blk2)
		if err != nil {
			t.Fatal(err)
		}
		logrec2 := createLogRec(secondWantInt32, secondWantStr)
		lsn2, err := lm.Append(logrec2)
		if err != nil {
			t.Fatal(err)
		}
		buff2.Contents().SetInt32(0, secondWantInt32)
		buff2.Contents().SetStr(int32Size, secondWantStr)
		buff2.SetModified(secondTxnum, lsn2)
		mgr.Unpin(ctx, buff2)

		blk3, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff3, err := mgr.Pin(ctx, blk3)
		if err != nil {
			t.Fatal(err)
		}
		logrec3 := createLogRec(thirdWantInt32, thirdWantStr)
		lsn3, err := lm.Append(logrec3)
		if err != nil {
			t.Fatal(err)
		}
		buff3.Contents().SetInt32(0, thirdWantInt32)
		buff3.Contents().SetStr(int32Size, thirdWantStr)
		buff3.SetModified(thirdTxnum, lsn3)
		mgr.Unpin(ctx, buff3)

		flushed, err := mgr.FlushDirtyPages(ctx, 2)
		if err != nil {
			t.Fatalf("FlushDirtyPagesが失敗した: %v", err)
		}
		if flushed != 2 {
			t.Fatalf("flush件数は2であるべきだが%vだった", flushed)
		}

		page1OnDisk := readPageFromDisk(t, fm, blk1)
		if got := page1OnDisk.GetInt32(0); got != firstWantInt32 {
			t.Errorf("1件目はflushされるのでint32(0)=%vであるべきだが%vだった", firstWantInt32, got)
		}

		page2OnDisk := readPageFromDisk(t, fm, blk2)
		if got := page2OnDisk.GetInt32(0); got != secondWantInt32 {
			t.Errorf("2件目はflushされるのでint32(0)=%vであるべきだが%vだった", secondWantInt32, got)
		}

		page3OnDisk := readPageFromDisk(t, fm, blk3)
		if got := page3OnDisk.GetInt32(0); got != wantInitialOnDiskInt32 {
			t.Errorf("3件目は未flushなのでint32(0)=%vであるべきだが%vだった", wantInitialOnDiskInt32, got)
		}
	})

	t.Run("BufferMgr: FlushDirtyPagesはlimitが0以下なら0,nilを返す", func(t *testing.T) {
		t.Parallel()
		t.Run("limitが0のとき0,nilを返す", func(t *testing.T) {
			const (
				blocksize = int32(256)
				logfile   = "logfile"
				filename  = "testfile"
				numbuffs  = int32(2)
				numwaits  = int32(2)
			)
			ctx := context.Background()

			fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
			if err != nil {
				t.Fatal(err)
			}
			dpt := NewDirtyPageTable()
			mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

			blk1, err := fm.Append(filename)
			if err != nil {
				t.Fatal(err)
			}
			buff1, err := mgr.Pin(ctx, blk1)
			if err != nil {
				t.Fatal(err)
			}
			logrec1 := createLogRec(1, "record1")
			lsn1, err := lm.Append(logrec1)
			if err != nil {
				t.Fatal(err)
			}
			buff1.Contents().SetInt32(0, 1)
			buff1.Contents().SetStr(int32Size, "record1")
			buff1.SetModified(1, lsn1)
			mgr.Unpin(ctx, buff1)

			blk2, err := fm.Append(filename)
			if err != nil {
				t.Fatal(err)
			}
			buff2, err := mgr.Pin(ctx, blk2)
			if err != nil {
				t.Fatal(err)
			}
			logrec2 := createLogRec(2, "record2")
			lsn2, err := lm.Append(logrec2)
			if err != nil {
				t.Fatal(err)
			}
			buff2.Contents().SetInt32(0, 2)
			buff2.Contents().SetStr(int32Size, "record2")
			buff2.SetModified(2, lsn2)
			mgr.Unpin(ctx, buff2)

			flushed, err := mgr.FlushDirtyPages(ctx, 0)
			if err != nil {
				t.Fatalf("FlushDirtyPagesは失敗しないべきだが%vだった", err)
			}
			if flushed != 0 {
				t.Fatalf("flush件数は0であるべきだが%vだった", flushed)
			}
		})

		t.Run("limitが負のとき0,nilを返す", func(t *testing.T) {
			const (
				blocksize = int32(256)
				logfile   = "logfile"
				filename  = "testfile"
				numbuffs  = int32(2)
				numwaits  = int32(2)
			)
			ctx := context.Background()

			fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
			if err != nil {
				t.Fatal(err)
			}
			dpt := NewDirtyPageTable()
			mgr := NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

			blk1, err := fm.Append(filename)
			if err != nil {
				t.Fatal(err)
			}
			buff1, err := mgr.Pin(ctx, blk1)
			if err != nil {
				t.Fatal(err)
			}
			logrec1 := createLogRec(1, "record1")
			lsn1, err := lm.Append(logrec1)
			if err != nil {
				t.Fatal(err)
			}
			buff1.Contents().SetInt32(0, 1)
			buff1.Contents().SetStr(int32Size, "record1")
			buff1.SetModified(1, lsn1)
			mgr.Unpin(ctx, buff1)

			blk2, err := fm.Append(filename)
			if err != nil {
				t.Fatal(err)
			}
			buff2, err := mgr.Pin(ctx, blk2)
			if err != nil {
				t.Fatal(err)
			}
			logrec2 := createLogRec(2, "record2")
			lsn2, err := lm.Append(logrec2)
			if err != nil {
				t.Fatal(err)
			}
			buff2.Contents().SetInt32(0, 2)
			buff2.Contents().SetStr(int32Size, "record2")
			buff2.SetModified(2, lsn2)
			mgr.Unpin(ctx, buff2)

			flushed, err := mgr.FlushDirtyPages(ctx, -1)
			if err != nil {
				t.Fatalf("FlushDirtyPagesは失敗しないべきだが%vだった", err)
			}
			if flushed != 0 {
				t.Fatalf("flush件数は0であるべきだが%vだった", flushed)
			}
		})
	})

	t.Run("BufferMgr: FlushAll(tx)は該当txのログを永続化する", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		gotLog1, err := f.lm.ReadRecordAt(f.lsn1)
		if err != nil {
			t.Errorf("ReadRecordAt(lsn1=%d)が失敗した: %v", f.lsn1, err)
		}
		if !bytes.Equal(gotLog1, f.logrec1) {
			t.Errorf("lsn1=%dのログが一致しない", f.lsn1)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)は該当txのブロックをディスクへ反映する（int32）", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		p1 := file.NewPage(f.fm.BlockSize())
		if err := f.fm.Read(f.blk1, p1); err != nil {
			t.Fatal(err)
		}
		if got := p1.GetInt32(0); got != 1 {
			t.Errorf("blk1 int32(0)は1であるべきだが%vだった", got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)は該当txのブロックをディスクへ反映する（str）", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		p1 := file.NewPage(f.fm.BlockSize())
		if err := f.fm.Read(f.blk1, p1); err != nil {
			t.Fatal(err)
		}
		if got := p1.GetStr(int32Size); got != "record1" {
			t.Errorf("blk1 str(%d)は%qであるべきだが%qだった", int32Size, "record1", got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)は該当txのブロックをディスクへ反映する（pageLSN）", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		p1 := file.NewPage(f.fm.BlockSize())
		if err := f.fm.Read(f.blk1, p1); err != nil {
			t.Fatal(err)
		}
		if got := p1.GetPageLSN(); got != f.lsn1 {
			t.Errorf("blk1 pageLSNは%vであるべきだが%vだった", f.lsn1, got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)は別txのブロックをディスクへ反映しない（int32）", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		p2 := file.NewPage(f.fm.BlockSize())
		if err := f.fm.Read(f.blk2, p2); err != nil {
			t.Fatal(err)
		}
		if got := p2.GetInt32(0); got != 0 {
			t.Errorf("blk2 int32(0)は0であるべきだが%vだった", got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)は別txのブロックをディスクへ反映しない（str）", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		p2 := file.NewPage(f.fm.BlockSize())
		if err := f.fm.Read(f.blk2, p2); err != nil {
			t.Fatal(err)
		}
		if got := p2.GetStr(int32Size); got != "" {
			t.Errorf("blk2 str(%d)は空文字であるべきだが%qだった", int32Size, got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)後に該当txのBufferのModifyingTxは-1になる", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		if got := f.b1.ModifyingTx(); got != -1 {
			t.Errorf("FlushAll後のb1 ModifyingTxは-1であるべきだが%vだった", got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)後に該当txのBufferのrecLSNは-1になる", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		if got := f.b1.recLSN; got != -1 {
			t.Errorf("FlushAll後のb1 recLSNは-1であるべきだが%vだった", got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)後も別txのBufferのModifyingTxは変わらない", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		if got := f.b2.ModifyingTx(); got != 2 {
			t.Errorf("FlushAll後のb2 ModifyingTxは%vであるべきだが%vだった", int32(2), got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)後に登録したDirtyPageの該当txのBlkのEntryが削除されている", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		if _, ok := f.dpt.GetPage(f.blk1); ok {
			t.Errorf("FlushAll後のDirtyPageのblk1のEntryが存在しないべきだが存在した")
		}
	})

	t.Run("BufferMgr: FlushAll(tx)後に登録したDirtyPageの該当txのBlkのEntryが削除されていない", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		if _, ok := f.dpt.GetPage(f.blk2); !ok {
			t.Errorf("FlushAll後のDirtyPageのblk2のEntryが存在するべきだが存在しない")
		}
	})

	t.Run("BufferMgr: FlushAll(tx)が失敗した場合はBuffer状態とDirtyPageを維持する", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)

		f.b1.SetModified(f.tx1, f.lsn1)
		f.b1.blk = file.NewBlockId(f.blk1.FileName(), -1)

		if err := f.mgr.FlushAll(f.ctx, f.tx1); err == nil {
			t.Fatal("FlushAllは失敗するべき")
		}
		if got := f.b1.ModifyingTx(); got != f.tx1 {
			t.Fatalf("失敗時もModifyingTxは維持されるべきだが got=%v", got)
		}
		if _, ok := f.dpt.GetPage(f.blk1); !ok {
			t.Fatal("失敗時もDirtyPageのentryは残るべき")
		}
	})
}
