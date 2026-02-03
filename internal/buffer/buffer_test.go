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
	rec := make([]byte, int32Size+file.MaxLength(recstrlen))
	strpos := int32(0)
	intpos := int32(int32Size + file.MaxLength(recstrlen))
	p := file.NewLogPage(rec)
	p.SetStr(strpos, str)
	p.SetInt32(intpos, i)
	return rec
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
			return NewBuffer(fm, lm)
		}

		t.Run("Contentsはnilでない", func(t *testing.T) {
			t.Parallel()
			if newBuf(t).Contents() == nil {
				t.Fatalf("Contentsはnilでないべき")
			}
		})

		t.Run("Blockはnilである", func(t *testing.T) {
			t.Parallel()
			if got := newBuf(t).Block(); got != nil {
				t.Fatalf("Blockはnilであるべきだが%vだった", got)
			}
		})

		t.Run("IsPinnedはfalseである", func(t *testing.T) {
			t.Parallel()
			if newBuf(t).IsPinned() {
				t.Fatalf("IsPinnedはfalseであるべきだがtrueだった")
			}
		})

		t.Run("ModifyingTxは-1である", func(t *testing.T) {
			t.Parallel()
			if got := newBuf(t).ModifyingTx(); got != -1 {
				t.Fatalf("ModifyingTxは-1であるべきだが%vだった", got)
			}
		})

		t.Run("pageLSNは0である", func(t *testing.T) {
			t.Parallel()
			if got := newBuf(t).Contents().GetPageLSN(); got != 0 {
				t.Fatalf("pageLSNは0であるべきだが%vだった", got)
			}
		})
	})

	t.Run("Buffer: SetModifiedはtxnumを更新する", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			txnum     = int32(1)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		b := NewBuffer(fm, lm)

		b.SetModified(txnum, -1)
		if got := b.ModifyingTx(); got != txnum {
			t.Fatalf("ModifyingTxは%vであるべきだが%vだった", txnum, got)
		}
	})

	t.Run("Buffer: SetModifiedは正のLSNでpageLSNを更新する", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			txnum     = int32(1)
			lsn       = int32(10)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		b := NewBuffer(fm, lm)

		b.SetModified(txnum, lsn)
		if got := b.Contents().GetPageLSN(); got != lsn {
			t.Fatalf("pageLSNは%vであるべきだが%vだった", lsn, got)
		}
	})

	t.Run("Buffer: SetModifiedは負のLSNではpageLSNを更新しない", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			txnum     = int32(1)
			lsn       = int32(-2)
		)
		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		b := NewBuffer(fm, lm)

		b.SetModified(txnum, lsn)
		if got := b.Contents().GetPageLSN(); got != 0 {
			t.Fatalf("pageLSNは更新されず0のままであるべきだが%vだった", got)
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
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		if got := mgr.Available(); got != numbuffs {
			t.Fatalf("Availableは%vであるべきだが%vだった", numbuffs, got)
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
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

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
				t.Fatalf("Pin後のAvailableは%vであるべきだが%vだった", numbuffs-1, got)
			}
		})

		t.Run("Pinすると返るBufferはpinnedである", func(t *testing.T) {
			if !b.IsPinned() {
				t.Fatalf("Pin後のIsPinnedはtrueであるべきだがfalseだった")
			}
		})

		t.Run("Pinで返るBufferのBlockは指定blkである", func(t *testing.T) {
			gotBlk := b.Block()
			if gotBlk == nil || gotBlk.FileName() != blk.FileName() || gotBlk.Number() != blk.Number() {
				gotFile, gotNum := "<nil>", int32(-1)
				if gotBlk != nil {
					gotFile, gotNum = gotBlk.FileName(), gotBlk.Number()
				}
				t.Fatalf("Pinで返るBufferのBlockは(%s,%d)であるべきだが(%s,%d)だった", blk.FileName(), blk.Number(), gotFile, gotNum)
			}
		})

		// Unpin後の状態
		mgr.Unpin(ctx, b)

		t.Run("UnpinするとAvailableが1増える", func(t *testing.T) {
			if got := mgr.Available(); got != numbuffs {
				t.Fatalf("Unpin後のAvailableは%vであるべきだが%vだった", numbuffs, got)
			}
		})

		t.Run("UnpinするとBufferはunpinnedになる", func(t *testing.T) {
			if b.IsPinned() {
				t.Fatalf("Unpin後のIsPinnedはfalseであるべきだがtrueだった")
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
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
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
				t.Fatalf("Unpin前にPinが完了してしまった（err=%v）", err)
			case <-time.After(30 * time.Millisecond):
				// まだ待っているのが期待
			}
		})

		t.Run("Unpinすると待機中のPinが完了する", func(t *testing.T) {
			mgr.Unpin(ctx, bPinned)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Unpin後のPinは成功すべきだが%vだった", err)
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatalf("Unpin後もPinが完了しない")
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
		mgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
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
			t.Fatalf("エラーは%vであるべきだが%vだった", want, err)
		}
	})

	type flushAllFixture struct {
		ctx context.Context
		tx1 int32
		tx2 int32

		fm  *file.FileMgr
		lm  *log.LogMgr
		mgr *BufferMgr

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
		f.mgr = NewBufferMgr(f.fm, f.lm, numbuffs, numwaits)

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

	t.Run("BufferMgr: FlushAll(tx)は該当txのログを永続化する", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		gotLog1, err := f.lm.ReadRecordAt(f.lsn1)
		if err != nil {
			t.Fatalf("ReadRecordAt(lsn1=%d)が失敗した: %v", f.lsn1, err)
		}
		if !bytes.Equal(gotLog1, f.logrec1) {
			t.Fatalf("lsn1=%dのログが一致しない", f.lsn1)
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
			t.Fatalf("blk1 int32(0)は1であるべきだが%vだった", got)
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
			t.Fatalf("blk1 str(%d)は%qであるべきだが%qだった", int32Size, "record1", got)
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
			t.Fatalf("blk1 pageLSNは%vであるべきだが%vだった", f.lsn1, got)
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
			t.Fatalf("blk2 int32(0)は0であるべきだが%vだった", got)
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
			t.Fatalf("blk2 str(%d)は空文字であるべきだが%qだった", int32Size, got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)後に該当txのBufferのModifyingTxは-1になる", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		if got := f.b1.ModifyingTx(); got != -1 {
			t.Fatalf("FlushAll後のb1 ModifyingTxは-1であるべきだが%vだった", got)
		}
	})

	t.Run("BufferMgr: FlushAll(tx)後も別txのBufferのModifyingTxは変わらない", func(t *testing.T) {
		t.Parallel()
		f := setupFlushAll(t)
		if got := f.b2.ModifyingTx(); got != 2 {
			t.Fatalf("FlushAll後のb2 ModifyingTxは%vであるべきだが%vだった", int32(2), got)
		}
	})
}
