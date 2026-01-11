package buffer

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

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

	t.Run("NewBuffer", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		expectedContents := file.NewPage(blocksize)
		if !reflect.DeepEqual(buff.contents, expectedContents) {
			t.Errorf("expected empty page %v, got %v", *expectedContents, *buff.contents)
		}

		if buff.blk != nil {
			t.Errorf("expected nil block %v, got %v", nil, *buff.blk)
		}

		if buff.pins != 0 {
			t.Errorf("expected pins %v, got %v", 0, buff.blk)
		}

		if buff.txnum != -1 {
			t.Errorf("expected txnum %v, got %v", -1, buff.txnum)
		}

		if buff.pageLSN != -1 {
			t.Errorf("expected pageLSN %v, got %v", -1, buff.pageLSN)
		}
	})

	t.Run("Contents after NewBuffer", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		contents := buff.Contents()
		expectedContents := file.NewPage(fm.BlockSize())
		if !reflect.DeepEqual(buff.Contents(), expectedContents) {
			t.Errorf("expected empty page %v, got %v", *expectedContents, *contents)
		}
	})

	t.Run("Block after NewBuffer", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		blk := buff.Block()
		if blk != nil {
			t.Errorf("expected nil block %v, got %v", nil, *blk)
		}
	})

	t.Run("SetModified postive lsn", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			txnum     = 1
			lsn       = 2
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		buff.SetModified(txnum, lsn)
		if buff.txnum != txnum {
			t.Errorf("expected txnum %v, got %v", txnum, buff.txnum)
		}
		if buff.pageLSN != lsn {
			t.Errorf("expected pageLSN %v, got %v", lsn, buff.pageLSN)
		}
	})

	t.Run("SetModified negative lsn", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			txnum     = 1
			lsn       = -2
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		buff.SetModified(txnum, lsn)
		if buff.txnum != txnum {
			t.Errorf("expected txnum %v, got %v", txnum, buff.txnum)
		}
		if buff.pageLSN != -1 {
			t.Errorf("expected pageLSN %v, got %v", -1, buff.pageLSN)
		}
	})

	t.Run("IsPinned after NewBuffer", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		isPinned := buff.IsPinned()
		if isPinned != false {
			t.Errorf("expected IsPinned %v, got %v", false, isPinned)
		}
	})

	t.Run("IsPinned after pin", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)
		buff.pin()

		isPinned := buff.IsPinned()
		if isPinned != true {
			t.Errorf("expected IsPinned %v, got %v", true, isPinned)
		}
	})

	t.Run("ModifyingTx after NewBuffer", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		txnum := buff.ModifyingTx()
		if txnum != -1 {
			t.Errorf("expected IsPinned %v, got %v", -1, txnum)
		}
	})

	t.Run("ModifyingTx after SetModified", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			txnum     = 1
			lsn       = 2
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)
		buff.SetModified(txnum, lsn)

		txnumModified := buff.ModifyingTx()
		if txnumModified != txnum {
			t.Errorf("expected IsPinned %v, got %v", txnumModified, txnum)
		}
	})

	t.Run("assignToBlock after pin", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		newblk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}

		buff.pin()

		buff.assignToBlock(newblk)
		expectedBlk := file.NewBlockId(filename, 0)
		if !reflect.DeepEqual(buff.blk, expectedBlk) {
			t.Errorf("expected empty blk %v, got %v", *expectedBlk, *buff.blk)
		}

		expectedContents := file.NewPage(blocksize)
		if !reflect.DeepEqual(buff.contents, expectedContents) {
			t.Errorf("expected empty contents %v, got %v", *expectedContents, *buff.contents)
		}

		if buff.pins != 0 {
			t.Errorf("expected pins %v, got %v", 0, buff.pins)
		}
	})

	t.Run("flush after NewBuffer", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)
		buff.flush()

		if buff.txnum != -1 {
			t.Errorf("expected txnum %v, got %v", -1, buff.txnum)
		}
	})

	t.Run("flush after write log and data", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			filename      = "testfile"
			txnum         = 1
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		logrec := createLogRec(1, "record")
		lsn, err := lm.Append(logrec)
		if err != nil {
			t.Fatal(err)
		}

		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff.assignToBlock(blk)
		contents := buff.contents
		contents.SetInt32(0, 1)
		contents.SetStr(int32Size, "record")
		if err := fm.Write(blk, contents); err != nil {
			t.Fatal(err)
		}

		buff.SetModified(txnum, lsn)
		buff.flush()

		iter, err := lm.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		logrecGeted := iter.Next()
		if !bytes.Equal(logrecGeted, logrec) {
			t.Errorf("expected log flushed rec %v, got %v", logrec, logrecGeted)
		}

		filePage := file.NewPage(blocksize)
		if err := fm.Read(blk, filePage); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(filePage, contents) {
			t.Errorf("expected flushed page %v, got %v", *contents, *filePage)
		}

		if buff.txnum != -1 {
			t.Errorf("expected txnum %v, got %v", -1, buff.txnum)
		}
	})

	t.Run("pin after NewBuffer", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			txnum     = 1
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		buff.pin()

		if buff.pins != 1 {
			t.Errorf("expected txnum %v, got %v", 1, buff.pins)
		}
	})

	t.Run("unpin after pin", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		buff := NewBuffer(fm, lm)

		buff.pin()
		buff.unpin()

		if buff.pins != 0 {
			t.Errorf("expected txnum %v, got %v", 0, buff.pins)
		}
	})
}

func TestLRUList(t *testing.T) {
	t.Parallel()

	t.Run("NewLRUList", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = 3
			numwaits  = 3
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		bufferpool := bufferMgr.bufferpool

		lrulist := NewLRUList(bufferpool)
		if lrulist.head.buff != bufferpool[0] {
			t.Errorf("expected lrulist head buff %v, got %v", bufferpool[0], lrulist.head.buff)
		}

		if lrulist.head.prev != nil {
			t.Errorf("expected lrulist head prev %v, got %v", nil, lrulist.head.prev)
		}

		if lrulist.head.next.buff != bufferpool[1] {
			t.Errorf("expected lrulist head's next buff %v, got %v", bufferpool[1], lrulist.head.next.buff)
		}

		if lrulist.head.next.next.buff != bufferpool[2] {
			t.Errorf("expected lrulist head's next next buff %v, got %v", bufferpool[2], lrulist.head.next.next.buff)
		}

		if lrulist.tail.buff != bufferpool[2] {
			t.Errorf("expected lrulist tail buff %v, got %v", bufferpool[2], lrulist.tail.buff)
		}

		if lrulist.tail.next != nil {
			t.Errorf("expected lrulist tail next %v, got %v", nil, lrulist.tail.next)
		}

		if lrulist.tail.prev.buff != bufferpool[1] {
			t.Errorf("expected lrulist tail prev buff %v, got %v", bufferpool[1], lrulist.tail.prev.buff)
		}

		if lrulist.tail.prev.prev.buff != bufferpool[0] {
			t.Errorf("expected lrulist tail prev prev buff %v, got %v", bufferpool[0], lrulist.tail.prev.prev.buff)
		}
	})

	t.Run("ChooseVictimBuffer after NewLRUList", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = 3
			numwaits  = 3
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		bufferpool := bufferMgr.bufferpool

		lrulist := NewLRUList(bufferpool)
		buff := lrulist.ChooseVictimBuffer()
		if buff != bufferpool[0] {
			t.Errorf("expected choose victim buff %v, got %v", bufferpool[0], buff)
		}
	})

	t.Run("ChooseVictimBuffer pin 2 times", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = 3
			numwaits  = 3
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		bufferpool := bufferMgr.bufferpool

		lrulist := NewLRUList(bufferpool)
		var latestVictimBuff *Buffer
		for i := 0; i < 2; i++ {
			buff := lrulist.ChooseVictimBuffer()
			buff.pin()
			buff.unpin()
			latestVictimBuff = buff
		}

		if latestVictimBuff != bufferpool[1] {
			t.Errorf("expected choose victim buff %v, got %v", bufferpool[1], latestVictimBuff)
		}
	})

	t.Run("moveToTail head item", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = 3
			numwaits  = 3
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		bufferpool := bufferMgr.bufferpool

		lrulist := NewLRUList(bufferpool)
		lrulist.moveToTail(lrulist.head)

		if lrulist.head.buff != bufferpool[1] {
			t.Errorf("expected new lrulist head buff %v, got %v", bufferpool[1], lrulist.head.buff)
		}

		if lrulist.head.prev != nil {
			t.Errorf("expected new lrulist head prev %v, got %v", nil, lrulist.head.prev)
		}

		if lrulist.head.next.buff != bufferpool[2] {
			t.Errorf("expected new lrulist head's next buff %v, got %v", bufferpool[2], lrulist.head.next.buff)
		}

		if lrulist.head.next.next.buff != bufferpool[0] {
			t.Errorf("expected new lrulist head's next next buff %v, got %v", bufferpool[0], lrulist.head.next.next.buff)
		}

		if lrulist.tail.buff != bufferpool[0] {
			t.Errorf("expected new lrulist tail buff %v, got %v", bufferpool[0], lrulist.tail.buff)
		}

		if lrulist.tail.next != nil {
			t.Errorf("expected new lrulist tail next %v, got %v", nil, lrulist.tail.next)
		}

		if lrulist.tail.prev.buff != bufferpool[2] {
			t.Errorf("expected new lrulist tail prev buff %v, got %v", bufferpool[2], lrulist.tail.prev.buff)
		}

		if lrulist.tail.prev.prev.buff != bufferpool[1] {
			t.Errorf("expected new lrulist tail prev prev buff %v, got %v", bufferpool[1], lrulist.tail.prev.prev.buff)
		}
	})

	t.Run("moveToTail tail item", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = 3
			numwaits  = 3
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		bufferpool := bufferMgr.bufferpool

		lrulist := NewLRUList(bufferpool)
		lrulist.moveToTail(lrulist.tail)

		if lrulist.head.buff != bufferpool[0] {
			t.Errorf("expected new lrulist head buff %v, got %v", bufferpool[0], lrulist.head.buff)
		}

		if lrulist.head.prev != nil {
			t.Errorf("expected new lrulist head prev %v, got %v", nil, lrulist.head.prev)
		}

		if lrulist.head.next.buff != bufferpool[1] {
			t.Errorf("expected new lrulist head's next buff %v, got %v", bufferpool[1], lrulist.head.next.buff)
		}

		if lrulist.head.next.next.buff != bufferpool[2] {
			t.Errorf("expected new lrulist head's next next buff %v, got %v", bufferpool[2], lrulist.head.next.next.buff)
		}

		if lrulist.tail.buff != bufferpool[2] {
			t.Errorf("expected new lrulist tail buff %v, got %v", bufferpool[2], lrulist.tail.buff)
		}

		if lrulist.tail.next != nil {
			t.Errorf("expected new lrulist tail next %v, got %v", nil, lrulist.tail.next)
		}

		if lrulist.tail.prev.buff != bufferpool[1] {
			t.Errorf("expected new lrulist tail prev buff %v, got %v", bufferpool[1], lrulist.tail.prev.buff)
		}

		if lrulist.tail.prev.prev.buff != bufferpool[0] {
			t.Errorf("expected new lrulist tail prev prev buff %v, got %v", bufferpool[0], lrulist.tail.prev.prev.buff)
		}
	})

	t.Run("moveToTail head nor tail item", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = 3
			numwaits  = 3
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		bufferpool := bufferMgr.bufferpool

		lrulist := NewLRUList(bufferpool)
		lrulist.moveToTail(lrulist.head.next)

		if lrulist.head.buff != bufferpool[0] {
			t.Errorf("expected new lrulist head buff %v, got %v", bufferpool[0], lrulist.head.buff)
		}

		if lrulist.head.prev != nil {
			t.Errorf("expected new lrulist head prev %v, got %v", nil, lrulist.head.prev)
		}

		if lrulist.head.next.buff != bufferpool[2] {
			t.Errorf("expected new lrulist head's next buff %v, got %v", bufferpool[2], lrulist.head.next.buff)
		}

		if lrulist.head.next.next.buff != bufferpool[1] {
			t.Errorf("expected new lrulist head's next next buff %v, got %v", bufferpool[1], lrulist.head.next.next.buff)
		}

		if lrulist.tail.buff != bufferpool[1] {
			t.Errorf("expected new lrulist tail buff %v, got %v", bufferpool[1], lrulist.tail.buff)
		}

		if lrulist.tail.next != nil {
			t.Errorf("expected new lrulist tail next %v, got %v", nil, lrulist.tail.next)
		}

		if lrulist.tail.prev.buff != bufferpool[2] {
			t.Errorf("expected new lrulist tail prev buff %v, got %v", bufferpool[2], lrulist.tail.prev.buff)
		}

		if lrulist.tail.prev.prev.buff != bufferpool[0] {
			t.Errorf("expected new lrulist tail prev prev buff %v, got %v", bufferpool[0], lrulist.tail.prev.prev.buff)
		}
	})
}

func TestBufferMgr(t *testing.T) {
	t.Parallel()

	t.Run("NewBufferMgr", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = 3
			numwaits  = 3
			Maxtime   = 10000
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		expectedBuff := NewBuffer(fm, lm)
		expectedBufferpool := []*Buffer{expectedBuff, expectedBuff, expectedBuff}
		if !reflect.DeepEqual(bufferMgr.bufferpool, expectedBufferpool) {
			t.Errorf("expected bufferpool %v, got %v", expectedBufferpool, bufferMgr.bufferpool)
		}

		if bufferMgr.numAvailable != numbuffs {
			t.Errorf("expected numAvailable %v, got %v", numbuffs, bufferMgr.numAvailable)
		}

		if bufferMgr.Maxtime != Maxtime {
			t.Errorf("expected Maxtime %v, got %v", Maxtime, bufferMgr.Maxtime)
		}
	})

	t.Run("Available after NewBufferMgr", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			numbuffs  = 3
			numwaits  = 3
			Maxtime   = 10000
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		available := bufferMgr.Available()
		if available != numbuffs {
			t.Errorf("expected Available %v, got %v", numbuffs, available)
		}
	})

	t.Run("FlushAll after write log and data", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum1    = 1
			txnum2    = 2
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		logrec1 := createLogRec(1, "record")
		lsn1, err := lm.Append(logrec1)
		if err != nil {
			t.Fatal(err)
		}
		blk1, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff1, err := bufferMgr.Pin(ctx, blk1)
		if err != nil {
			t.Fatal(err)
		}
		buff1Page := buff1.Contents()
		buff1Page.SetInt32(0, 1)
		buff1Page.SetStr(int32Size, "record")
		buff1.SetModified(txnum1, lsn1)

		logrec2 := createLogRec(2, "record")
		lsn2, err := lm.Append(logrec2)
		if err != nil {
			t.Fatal(err)
		}
		blk2, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff2, err := bufferMgr.Pin(ctx, blk2)
		if err != nil {
			t.Fatal(err)
		}
		buff2Page := buff1.Contents()
		buff2Page.SetInt32(0, 2)
		buff2Page.SetStr(int32Size, "record")
		buff2.SetModified(txnum2, lsn2)

		bufferMgr.FlushAll(ctx, txnum1)

		p1 := file.NewPage(blocksize)
		if err := fm.Read(blk1, p1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(p1, buff1Page) {
			t.Errorf("expected flushed page %v, got %v", *buff1Page, *p1)
		}

		p2 := file.NewPage(blocksize)
		if err := fm.Read(blk2, p2); err != nil {
			t.Fatal(err)
		}
		emptyP := file.NewPage(blocksize)
		if !reflect.DeepEqual(p2, emptyP) {
			t.Errorf("expected flushed page %v, got %v", *emptyP, *p2)
		}

		if buff1.txnum != -1 {
			t.Errorf("expected txnum %v, got %v", -1, buff1.txnum)
		}

		if buff2.txnum != txnum2 {
			t.Errorf("expected txnum %v, got %v", txnum2, buff2.txnum)
		}
	})

	t.Run("Pin after NewBufferMgr", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		logrec := createLogRec(1, "record")
		lsn, err := lm.Append(logrec)
		if err != nil {
			t.Fatal(err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff, err := bufferMgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		buffPage := buff.Contents()
		buffPage.SetInt32(0, 1)
		buffPage.SetStr(int32Size, "record")
		buff.SetModified(txnum, lsn)

		if bufferMgr.numAvailable != numbuffs-1 {
			t.Errorf("expected numAvailable %v, got %v", numbuffs-1, bufferMgr.numAvailable)
		}

		if !buff.IsPinned() {
			t.Errorf("expected buff IsPinned %v, got %v", true, buff.IsPinned())
		}

		if !reflect.DeepEqual(buff.blk, blk) {
			t.Errorf("expected buff blk %v, got %v", blk, buff.blk)
		}
	})

	t.Run("Pin 4 times", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		var buffErr error
		for i := 0; i < numbuffs+1; i++ {
			logrec := createLogRec(int32(i+1), "record")
			lsn, err := lm.Append(logrec)
			if err != nil {
				t.Fatal(err)
			}
			blk, err := fm.Append(filename)
			if err != nil {
				t.Fatal(err)
			}
			buff, err := bufferMgr.Pin(ctx, blk)
			if err != nil {
				buffErr = err
				break
			}
			buffPage := buff.Contents()
			buffPage.SetInt32(0, int32(i))
			buffPage.SetStr(int32Size, "record")
			buff.SetModified(txnum, lsn)
		}

		for _, buff := range bufferMgr.bufferpool {
			if !buff.IsPinned() {
				t.Errorf("expected buff IsPinned %v, got %v", true, buff.IsPinned())
			}
		}

		if bufferMgr.numAvailable != 0 {
			t.Errorf("expected numAvailable %v, got %v", 0, bufferMgr.numAvailable)
		}

		pinTimeoutErr := errors.New("buffer abort exception")
		if buffErr.Error() != pinTimeoutErr.Error() {
			t.Errorf("expected buffErr %v, got %v", pinTimeoutErr, buffErr)
		}
	})

	t.Run("Unpin after Pin 1 times", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		logrec := createLogRec(1, "record")
		lsn, err := lm.Append(logrec)
		if err != nil {
			t.Fatal(err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff, err := bufferMgr.Pin(ctx, blk)
		if err != nil {
			t.Fatal(err)
		}
		buffPage := buff.Contents()
		buffPage.SetInt32(0, 1)
		buffPage.SetStr(int32Size, "record")
		buff.SetModified(txnum, lsn)

		bufferMgr.Unpin(ctx, buff)

		if buff.IsPinned() {
			t.Errorf("expected buff IsPinned %v, got %v", false, buff.IsPinned())
		}

		if bufferMgr.numAvailable != numbuffs {
			t.Errorf("expected buff numAvailable %v, got %v", numbuffs, bufferMgr.numAvailable)
		}
	})

	t.Run("Unpin after Pin 2 times for same block", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		var latestBuff *Buffer
		blk := file.NewBlockId(filename, 0)
		for i := 0; i < 2; i++ {
			logrec := createLogRec(int32(i+1), "record")
			lsn, err := lm.Append(logrec)
			if err != nil {
				t.Fatal(err)
			}

			buff, err := bufferMgr.Pin(ctx, blk)
			if err != nil {
				t.Fatal(err)
			}
			buffPage := buff.Contents()
			buffPage.SetInt32(0, 1)
			buffPage.SetStr(int32Size, "record")
			buff.SetModified(txnum, lsn)
			latestBuff = buff
		}

		bufferMgr.Unpin(ctx, latestBuff)

		if bufferMgr.numAvailable != numbuffs-1 {
			t.Errorf("expected buff numAvailable %v, got %v", numbuffs-1, bufferMgr.numAvailable)
		}
	})

	t.Run("Unpin after Pin 4 times", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		// 複数blkをファイルに書き込む
		blkList := []*file.BlockId{
			file.NewBlockId(filename, 0), // このblkがunpin対象
			file.NewBlockId(filename, 1),
			file.NewBlockId(filename, 2),
			file.NewBlockId(filename, 3),
		}
		for i := 0; i < len(blkList)-1; i++ {
			if _, err := fm.Append(filename); err != nil {
				t.Fatal(err)
			}
		}

		// blk0~2の数分Pinする
		var firstBuff *Buffer
		var expectedPage = file.NewPage(blocksize)
		for i := 0; i < len(blkList)-1; i++ {
			logrec := createLogRec(int32(i+1), "record")
			lsn, err := lm.Append(logrec)
			if err != nil {
				return
			}

			blk := blkList[i]
			buff, err := bufferMgr.Pin(context.Background(), blk)
			if err != nil {
				return
			}

			// 更新
			buffPage := buff.Contents()
			buffPage.SetInt32(0, 1)
			buffPage.SetStr(int32Size, "record")
			buff.SetModified(txnum, lsn)

			if i == 0 {
				firstBuff = buff
				expectedPage.SetPageLSN(lsn)
				expectedPage.SetInt32(0, 1)
				expectedPage.SetStr(int32Size, "record")
			}
		}

		startPinCh := make(chan struct{}, 1)
		endPinCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)
		// blk3のPinを実施
		go func() {
			// blk3のPinが完了したことを通知
			defer close(endPinCh)

			logrec := createLogRec(int32(len(blkList)), "record")
			lsn, err := lm.Append(logrec)
			if err != nil {
				errCh <- err
				return
			}

			// blk3のPinが開始されたことを通知
			close(startPinCh)

			buff, err := bufferMgr.Pin(context.Background(), blkList[len(blkList)-1])
			if err != nil {
				errCh <- err
				return
			}
			buffPage := buff.Contents()
			buffPage.SetInt32(0, 1)
			buffPage.SetStr(int32Size, "record")
			buff.SetModified(txnum, lsn)
		}()

		// blk3のPinが開始されるのを待つ
		<-startPinCh

		// blk0のunpinを実施
		bufferMgr.Unpin(context.Background(), firstBuff)

		// blk3のPinが完了するのを待つ
		<-endPinCh

		// buffer abort exceptionが発生していないこと
		select {
		case pinErr := <-errCh:
			if pinErr != nil {
				pinTimeoutErr := errors.New("buffer abort exception")
				if pinErr.Error() == pinTimeoutErr.Error() {
					t.Errorf("expected buff error %v, got %v", nil, pinErr)
				} else {
					t.Fatal(pinErr)
				}
			}
		default:
		}

		// numAvailable（空きバッファ数）が0になっていること
		if bufferMgr.numAvailable != 0 {
			t.Errorf("expected buff numAvailable %v, got %v", 0, bufferMgr.numAvailable)
		}

		// 最初にpinしたblkの内容がflushされていること
		p := file.NewPage(blocksize)
		if err := fm.Read(blkList[0], p); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(*p, *expectedPage) {
			t.Errorf("expected flush page %v, got %v", *expectedPage, *p)
		}

		// 残りのroutineのunpinを実施
		for i := 0; i < len(bufferMgr.bufferpool); i++ {
			bufferMgr.Unpin(context.Background(), bufferMgr.bufferpool[i])
		}
	})

	t.Run("Unpin after Pin 4 times for same block", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)
		var wg sync.WaitGroup
		ctx := context.Background()

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		errCh := make(chan error, 4)
		wg.Add(4)
		blk := file.NewBlockId(filename, 0)
		if _, err := fm.Append(filename); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 4; i++ {
			go func() {
				logrec := createLogRec(int32(i+1), "record")
				lsn, err := lm.Append(logrec)
				if err != nil {
					errCh <- err
					return
				}

				buff, err := bufferMgr.Pin(ctx, blk)
				if err != nil {
					errCh <- err
					return
				}
				buffPage := buff.Contents()
				buffPage.SetInt32(0, 1)
				buffPage.SetStr(int32Size, "record")
				buff.SetModified(txnum, lsn)

				if i == 0 {
					bufferMgr.Unpin(ctx, buff)
				}

				wg.Done()
			}()
		}

		wg.Wait()
		close(errCh)

		pinTimeoutErr := errors.New("buffer abort exception")
		for err := range errCh {
			if err.Error() == pinTimeoutErr.Error() {
				t.Errorf("expected buff error %v, got %v", nil, err)
			} else {
				t.Fatal(err)
			}
		}

		if bufferMgr.numAvailable != numbuffs-1 {
			t.Errorf("expected buff numAvailable %v, got %v", numbuffs-1, bufferMgr.numAvailable)
		}

		p := file.NewPage(blocksize)
		if err := fm.Read(blk, p); err != nil {
			t.Fatal(err)
		}

		targetP := file.NewPage(blocksize)
		if !reflect.DeepEqual(*p, *targetP) {
			t.Errorf("expected flush page %v, got %v", *targetP, *p)
		}
	})

	t.Run("tryToPin after NewBufferMgr", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		logrec := createLogRec(int32(1), "record")
		lsn, err := lm.Append(logrec)
		if err != nil {
			t.Fatal(err)
		}
		buff := bufferMgr.tryToPin(blk)
		buffPage := buff.Contents()
		buffPage.SetInt32(0, 1)
		buffPage.SetStr(int32Size, "record")
		buff.SetModified(txnum, lsn)

		if bufferMgr.numAvailable != numbuffs-1 {
			t.Errorf("expected numAvailable %v, got %v", numbuffs-1, bufferMgr.numAvailable)
		}

		if !buff.IsPinned() {
			t.Errorf("expected IsPinned %v, got %v", true, buff.IsPinned())
		}

		p := file.NewPage(blocksize)
		emptyP := file.NewPage(blocksize)
		fm.Read(blk, p)
		if !reflect.DeepEqual(p, emptyP) {
			t.Errorf("expected tryPin blk page %v, got %v", *emptyP, *p)
		}
	})

	t.Run("tryToPin 2 times for same blk", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 2; i++ {
			logrec := createLogRec(int32(1), "record")
			lsn, err := lm.Append(logrec)
			if err != nil {
				t.Fatal(err)
			}
			buff := bufferMgr.tryToPin(blk)
			buffPage := buff.Contents()
			buffPage.SetInt32(0, 1)
			buffPage.SetStr(int32Size, "record")
			buff.SetModified(txnum, lsn)
			if !buff.IsPinned() {
				t.Errorf("expected IsPinned %v, got %v", true, buff.IsPinned())
			}
		}

		if bufferMgr.numAvailable != numbuffs-1 {
			t.Errorf("expected numAvailable %v, got %v", numbuffs-1, bufferMgr.numAvailable)
		}

		p := file.NewPage(blocksize)
		emptyP := file.NewPage(blocksize)
		fm.Read(blk, p)
		if !reflect.DeepEqual(p, emptyP) {
			t.Errorf("expected tryPin blk page %v, got %v", *emptyP, *p)
		}
	})

	t.Run("tryToPin after tryToPin and unpin", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		blkList := make([]*file.BlockId, 0)
		for i := 0; i < 2; i++ {
			logrec := createLogRec(int32(1), "record")
			lsn, err := lm.Append(logrec)
			if err != nil {
				t.Fatal(err)
			}
			blk, err := fm.Append(filename)
			if err != nil {
				t.Fatal(err)
			}
			buff := bufferMgr.tryToPin(blk)
			buffPage := buff.Contents()
			buffPage.SetInt32(0, 1)
			buffPage.SetStr(int32Size, "record")
			buff.SetModified(txnum, lsn)
			if !buff.IsPinned() {
				t.Errorf("expected IsPinned %v, got %v", true, buff.IsPinned())
			}
			blkList = append(blkList, blk)
		}

		for _, blk := range blkList {
			p := file.NewPage(blocksize)
			fm.Read(blk, p)
			emptyP := file.NewPage(blocksize)
			if !reflect.DeepEqual(p, emptyP) {
				t.Errorf("expected tryPin blk page %v, got %v", *emptyP, *p)
			}
		}
	})

	t.Run("findExistingBuffer after NewBufferMgr", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		blk := file.NewBlockId(filename, 0)
		buff := bufferMgr.findExistingBuffer(blk)
		if buff != nil {
			t.Errorf("expected buff found %v, got %v", nil, buff)
		}
	})

	t.Run("findExistingBuffer after tryToPin", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
			filename  = "testfile"
			numbuffs  = 3
			numwaits  = 3
			txnum     = 1
		)

		fm, lm, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		bufferMgr := NewBufferMgr(fm, lm, numbuffs, numwaits)

		logrec := createLogRec(int32(1), "record")
		lsn, err := lm.Append(logrec)
		if err != nil {
			t.Fatal(err)
		}
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatal(err)
		}
		buff := bufferMgr.tryToPin(blk)
		buffPage := buff.Contents()
		buffPage.SetInt32(0, 1)
		buffPage.SetStr(int32Size, "record")
		buff.SetModified(txnum, lsn)

		buffFound := bufferMgr.findExistingBuffer(blk)
		if !reflect.DeepEqual(buffFound, buff) {
			t.Errorf("expected buff found %v, got %v", *buff, *buffFound)
		}
	})
}
