package log

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/keingoon/simpledb/internal/file"
)

func initFileLogMgr(dir string, blocksize int32, logfile string) (*file.FileMgr, *LogMgr, error) {
	fileMgr, err := file.NewFileMgr(dir, blocksize)
	if err != nil {
		return nil, nil, err
	}
	logMgr, err := NewLogMgr(fileMgr, logfile)
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

func TestLogMgr(t *testing.T) {
	t.Parallel()

	t.Run("NewLogMgr at first", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fileMgr, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		b := make([]byte, blocksize)
		expectedpage := file.NewLogPage(b)
		expectedpage.SetInt32(0, int32Size)
		if !reflect.DeepEqual(logMgr.logpage, expectedpage) {
			t.Errorf("expected empty block %v, got %v", *expectedpage, *logMgr.logpage)
		}

		expectedblk := file.NewBlockId(logfile, 1)
		if !reflect.DeepEqual(logMgr.currentblk, expectedblk) {
			t.Errorf("expected empty block %v, got %v", *expectedblk, *logMgr.currentblk)
		}

		// verify header block (block 0)
		hdr := file.NewPage(blocksize)
		if err := fileMgr.Read(file.NewBlockId(logfile, 0), hdr); err != nil {
			t.Fatal(err)
		}
		if got := hdr.GetInt32(headerMagicOffset); got != logHeaderMagic {
			t.Errorf("expected header magic %v, got %v", logHeaderMagic, got)
		}
		if got := hdr.GetInt16(headerVersionOffset); got != logHeaderVersion {
			t.Errorf("expected header version %v, got %v", logHeaderVersion, got)
		}
		if got := hdr.GetInt32(headerPageSizeOffset); got != blocksize {
			t.Errorf("expected header page size %v, got %v", blocksize, got)
		}
		if got := hdr.GetInt32(headerLastCheckpointLSNOffset); got != 0 {
			t.Errorf("expected header last checkpoint LSN %v, got %v", 0, got)
		}
	})

	t.Run("NewLogMgr with invalid header", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		// Prepare a file with a bad header (magic mismatch)
		fileMgr, err := file.NewFileMgr(t.TempDir(), blocksize)
		if err != nil {
			t.Fatal(err)
		}
		blk0, err := fileMgr.Append(logfile)
		if err != nil {
			t.Fatal(err)
		}
		if blk0.Number() != 0 {
			t.Fatalf("expected header block number 0, got %v", blk0.Number())
		}
		bad := file.NewPage(blocksize)
		// leave magic as 0 (invalid), optionally fill other fields
		if err := fileMgr.Write(blk0, bad); err != nil {
			t.Fatal(err)
		}

		if _, err := NewLogMgr(fileMgr, logfile); err == nil {
			t.Fatalf("expected error due to invalid header magic, got nil")
		}
	})

	t.Run("Flush after Append single log rec", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			logrecord     = "logrecord"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append single log rec
		rec := createLogRec(1, logrecord)
		if _, err := logMgr.Append(rec); err != nil {
			t.Fatal(err)
		}

		// Flush single log rec
		logMgr.Flush(logMgr.latestLSN)

		if logMgr.latestLSN != startLSN {
			t.Errorf("expected latestLSN %v, got %v", startLSN, logMgr.latestLSN)
		}

		if logMgr.lastSavedLSN != startLSN {
			t.Errorf("expected lastSavedLSN %v, got %v", startLSN, logMgr.lastSavedLSN)
		}
	})

	t.Run("Iterater after Append single log rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			logrecord     = "logrecord"
			recCount      = 1
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append single log rec
		rec := createLogRec(recCount, logrecord)
		if _, err := logMgr.Append(rec); err != nil {
			t.Fatal(err)
		}

		// Iterater single log rec
		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		if iter.endBlk.Number() != 1 {
			t.Errorf("expected endBlk number %v, got %v", 1, iter.endBlk.Number())
		}
	})

	t.Run("Append single log rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			logrecord     = "logrecord"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append single log rec
		rec := createLogRec(1, logrecord)
		lsn, err := logMgr.Append(rec)
		if err != nil {
			t.Fatal(err)
		}

		if lsn != startLSN {
			t.Errorf("expected latestLSN %v, got %v", startLSN, lsn)
		}

		logpage := logMgr.logpage
		recGeted := logpage.GetBytes(startBoundary)
		if !bytes.Equal(rec, recGeted) {
			t.Errorf("expected logpage rec %v, got %v", rec, recGeted)
		}
	})

	t.Run("Append 20 log rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
			logrecord = "logrecord"
			count     = 20
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append 20 log rec
		var latestRec []byte
		var recLen int32 = 0
		for i := 0; i < count; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			recLen = int32(len(rec))
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
			latestRec = rec
		}

		bytesPerRec := int32Size + recLen
		freeBytes := blocksize - int32Size

		firstBlkRecCnt := freeBytes / bytesPerRec
		secondBlkRecCnt := count - firstBlkRecCnt

		firstBlkLastRecOffset := int32Size + (firstBlkRecCnt-1)*bytesPerRec
		secondBlkLastRecOffset := int32Size + (secondBlkRecCnt-1)*bytesPerRec
		lastSavedLSN := blocksize + firstBlkLastRecOffset
		latestLSN := blocksize*2 + secondBlkLastRecOffset

		expectedblk := file.NewBlockId(logfile, 2)
		if !reflect.DeepEqual(logMgr.currentblk, expectedblk) {
			t.Errorf("expected empty block %v, got %v", *expectedblk, *logMgr.currentblk)
		}

		logpage := logMgr.logpage
		recpos := logpage.GetInt32(0)
		recGeted := logpage.GetBytes(recpos)

		if !bytes.Equal(latestRec, recGeted) {
			t.Errorf("expected logpage rec %v, got %v", latestRec, recGeted)
		}

		if logMgr.latestLSN != latestLSN {
			t.Errorf("expected latestLSN %v, got %v", latestLSN, logMgr.latestLSN)
		}

		if logMgr.lastSavedLSN != lastSavedLSN {
			t.Errorf("expected lastSavedLSN %v, got %v", lastSavedLSN, logMgr.lastSavedLSN)
		}
	})

	t.Run("appendNewBlock", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fileMgr, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		expectedBlk := file.NewBlockId(logfile, 2)
		blk, err := logMgr.appendNewBlock()
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(blk, expectedBlk) {
			t.Errorf("expected empty block %v, got %v", *expectedBlk, *blk)
		}

		logpage := logMgr.logpage
		recpos := logpage.GetInt32(0)
		if recpos != int32Size {
			t.Errorf("expected logpage recpos %v, got %v", int32Size, recpos)
		}

		writenBlkPage := file.NewPage(blocksize)
		fileMgr.Read(blk, writenBlkPage)
		recposInBlock := writenBlkPage.GetInt32(0)
		if recposInBlock != int32Size {
			t.Errorf("expected recpos in block %v, got %v", int32Size, recposInBlock)
		}
	})

	t.Run("flush after Append 1 rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			logrecord     = "logrecord"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		fileMgr, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append single log rec
		rec := createLogRec(1, logrecord)
		if _, err := logMgr.Append(rec); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		blk := file.NewBlockId(logfile, 1)
		logpage := file.NewPage(blocksize)
		fileMgr.Read(blk, logpage)
		recGeted := logpage.GetBytes(startBoundary)

		if !bytes.Equal(rec, recGeted) {
			t.Errorf("expected rec in block %v, got %v", rec, recGeted)
		}

		if logMgr.lastSavedLSN != startLSN {
			t.Errorf("expected lastSavedLSN %v, got %v", startLSN, logMgr.lastSavedLSN)
		}
	})

	t.Run("ReadRecordAt after Append 1 rec and Flush", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
			logrecord = "logrecord"
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		rec := createLogRec(1, logrecord)
		if _, err := logMgr.Append(rec); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		recGeted, err := logMgr.ReadRecordAt(logMgr.lastSavedLSN)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(rec, recGeted) {
			t.Errorf("expected rec %v, got %v", rec, recGeted)
		}
	})

	t.Run("ReadRecordAt after Append 20 rec and Flush", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
			logrecord = "logrecord"
			count     = 20
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		var latestRec []byte
		for i := 0; i < count; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
			latestRec = rec
		}

		logMgr.Flush(logMgr.latestLSN)

		recGeted, err := logMgr.ReadRecordAt(logMgr.lastSavedLSN)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(latestRec, recGeted) {
			t.Errorf("expected rec %v, got %v", latestRec, recGeted)
		}
	})

	// Master LSN tests
	t.Run("ReadMasterLSN at first", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		masterLsn, err := logMgr.ReadMasterLSN()
		if err != nil {
			t.Fatal(err)
		}
		if masterLsn != 0 {
			t.Errorf("expected master LSN %v, got %v", 0, masterLsn)
		}
	})

	t.Run("WriteMasterLSN after reopen", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		dir := t.TempDir()
		fileMgr, logMgr, err := initFileLogMgr(dir, blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		var want int32 = 42
		if err := logMgr.WriteMasterLSN(want); err != nil {
			t.Fatal(err)
		}

		masterLsn, err := logMgr.ReadMasterLSN()
		if err != nil {
			t.Fatal(err)
		}
		if masterLsn != want {
			t.Errorf("expected master LSN %v, got %v", want, masterLsn)
		}

		reopened, err := NewLogMgr(fileMgr, logfile)
		if err != nil {
			t.Fatal(err)
		}
		masterLsn2, err := reopened.ReadMasterLSN()
		if err != nil {
			t.Fatal(err)
		}
		if masterLsn2 != want {
			t.Errorf("expected master LSN after reopen %v, got %v", want, masterLsn2)
		}
	})
}

func TestLogIterator(t *testing.T) {
	t.Parallel()

	t.Run("NewLogIterator", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		logIterator, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		if logIterator.currentpos != startBoundary {
			t.Errorf("expected currentpos %v, got %v", startBoundary, logIterator.currentpos)
		}
		if logIterator.boundary != startBoundary {
			t.Errorf("expected boundary %v, got %v", startBoundary, logIterator.boundary)
		}
	})

	t.Run("HasNext called after Append and Flush 1 rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			logrecord     = "logrecord"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append and Flush single log rec
		rec := createLogRec(1, logrecord)
		if _, err := logMgr.Append(rec); err != nil {
			t.Fatal(err)
		}
		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		hasNext := iter.HasNext()
		if !hasNext {
			t.Errorf("expected HasNext %v, got %v", true, hasNext)
		}
	})

	t.Run("HasNext called from Next call after Append and Flush 1 rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			logrecord     = "logrecord"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append and Flush single log rec
		rec := createLogRec(1, logrecord)
		if _, err := logMgr.Append(rec); err != nil {
			t.Fatal(err)
		}
		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		iter.Next()
		hasNext := iter.HasNext()
		if hasNext {
			t.Errorf("expected HasNext %v, got %v", false, hasNext)
		}
	})

	t.Run("HasNext called from Next 15 called after Append and Flush 20 rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize       = int32(256)
			logfile         = "logfile"
			logrecord       = "logrecord"
			recCount        = 20
			nextCalledCount = 15
			startBoundary   = int32Size
			startLSN        = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append and Flush 20 log rec
		for i := 0; i < recCount; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
		}
		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < nextCalledCount; i++ {
			iter.Next()
		}

		hasNext := iter.HasNext()
		if !hasNext {
			t.Errorf("expected HasNext %v, got %v", true, hasNext)
		}
	})

	t.Run("HasNext called from Next 20 called after Append and Flush 20 rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize       = int32(256)
			logfile         = "logfile"
			logrecord       = "logrecord"
			recCount        = 20
			nextCalledCount = 20
			startBoundary   = int32Size
			startLSN        = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append and Flush 20 log rec
		for i := 0; i < recCount; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
		}
		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < nextCalledCount; i++ {
			iter.Next()
		}

		hasNext := iter.HasNext()
		if hasNext {
			t.Errorf("expected HasNext %v, got %v", false, hasNext)
		}
	})

	t.Run("Next 15 called after Append and Flush 20 rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize       = int32(256)
			logfile         = "logfile"
			logrecord       = "logrecord"
			recCount        = 20
			nextCalledCount = 20
			startBoundary   = int32Size
			startLSN        = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append and Flush 20 log rec
		for i := 0; i < recCount; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
		}
		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		var rec []byte
		for i := 0; i < nextCalledCount; i++ {
			rec = iter.Next()
			recExpected := createLogRec(int32(i+1), logrecord)
			if !bytes.Equal(rec, recExpected) {
				t.Errorf("expected Next rec %v, got %v", recExpected, rec)
			}
		}
	})

	t.Run("moveToBlock after Append 20 rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize       = int32(256)
			logfile         = "logfile"
			logrecord       = "logrecord"
			recCount        = 20
			nextCalledCount = 20
			startBoundary   = int32Size
			startLSN        = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		var bytesPerRec int32 = 0
		// Append 20 log rec
		for i := 0; i < recCount; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			bytesPerRec = int32Size + int32(len(rec))
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
		}

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		// currentBlk number is 1, nextBlk number is 2 (header block is 0)
		nextBlk := file.NewBlockId(logfile, 2)
		iter.moveToBlock(nextBlk, int32Size)

		p := iter.p
		recpos := p.GetInt32(0)
		recFirst := p.GetBytes(recpos)
		recFirstExpected := createLogRec(blocksize/bytesPerRec, logrecord)
		if !bytes.Equal(recFirst, recFirstExpected) {
			t.Errorf("expected page first rec after moveToBlock %v, got %v", recFirstExpected, recFirst)
		}

		if iter.boundary != recpos {
			t.Errorf("expected boundary after moveToBlock %v, got %v", recpos, iter.boundary)
		}
		if iter.currentpos != recpos {
			t.Errorf("expected boundary after moveToBlock %v, got %v", recpos, iter.currentpos)
		}
	})
}

func TestFragmentRecord(t *testing.T) {
	t.Parallel()

	// 大きなレコードを作成するヘルパー（ブロックサイズを超える）
	createLargeLogRec := func(size int) []byte {
		rec := make([]byte, size)
		for i := 0; i < size; i++ {
			rec[i] = byte(i % 256)
		}
		return rec
	}

	t.Run("正常形: first->cont->last が正しく復元できる", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// 3ブロック分の大きなレコードを作成
		largeRec := createLargeLogRec(int(blocksize) * 3)
		if _, err := logMgr.Append(largeRec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		if !iter.HasNext() {
			t.Fatal("expected at least one record")
		}

		reconstructed := iter.Next()
		if !bytes.Equal(largeRec, reconstructed) {
			t.Errorf("reconstructed record mismatch: expected len=%d, got len=%d", len(largeRec), len(reconstructed))
		}
	})

	t.Run("異常形: lastがない（チェイン末尾欠け）", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// まず通常レコードを1件追加
		rec1 := createLogRec(1, "normal1")
		if _, err := logMgr.Append(rec1); err != nil {
			t.Fatal(err)
		}

		// LAST を書かずに FIRST + CONT だけを人工的に挿入する
		totalLen := 100
		dataFirst := bytes.Repeat([]byte{1}, 40)
		dataCont := bytes.Repeat([]byte{2}, 40)

		firstFrag := logMgr.buildFragment(dataFirst, totalLen, true /* first */, false /* isLast */)
		if err := logMgr.ensureAndWrite(firstFrag); err != nil {
			t.Fatalf("ensureAndWrite(firstFrag) failed: %v", err)
		}

		contFrag := logMgr.buildFragment(dataCont, totalLen, false /* first */, false /* isLast */)
		if err := logMgr.ensureAndWrite(contFrag); err != nil {
			t.Fatalf("ensureAndWrite(contFrag) failed: %v", err)
		}

		// その後に別の通常レコードを追加
		rec2 := createLogRec(2, "normal2")
		if _, err := logMgr.Append(rec2); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		// 最初の通常レコード rec1 が最初に返る
		got := iter.Next()
		if !bytes.Equal(rec1, got) {
			t.Errorf("expected latest record %v, got %v", rec1, got)
		}

		// 次の Next 呼び出しでは、壊れたフラグメントチェインをスキップして rec2 が返る
		got = iter.Next()
		if !bytes.Equal(rec2, got) {
			t.Errorf("expected older record %v, got %v", rec2, got)
		}

		// それ以上の論理レコードは存在しない
		if next := iter.Next(); next != nil {
			t.Errorf("expected no more logical records, got %v", next)
		}
	})

	t.Run("正常形: 複数のフラグメントレコードが混在", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// 通常レコード
		rec1 := createLogRec(1, "normal1")
		if _, err := logMgr.Append(rec1); err != nil {
			t.Fatal(err)
		}

		// 大きなレコード1
		largeRec1 := createLargeLogRec(int(blocksize) + 100)
		if _, err := logMgr.Append(largeRec1); err != nil {
			t.Fatal(err)
		}

		// 通常レコード
		rec2 := createLogRec(2, "normal2")
		if _, err := logMgr.Append(rec2); err != nil {
			t.Fatal(err)
		}

		// 大きなレコード2
		largeRec2 := createLargeLogRec(int(blocksize) + 200)
		if _, err := logMgr.Append(largeRec2); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		// 順番で読み戻し（Iteratorは古い順から読む）
		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		gotRec1 := iter.Next()
		if !bytes.Equal(rec1, gotRec1) {
			t.Errorf("rec1 mismatch")
		}

		got1 := iter.Next()
		if !bytes.Equal(largeRec1, got1) {
			t.Errorf("largeRec1 mismatch")
		}

		gotRec2 := iter.Next()
		if !bytes.Equal(rec2, gotRec2) {
			t.Errorf("rec2 mismatch")
		}

		got2 := iter.Next()
		if !bytes.Equal(largeRec2, got2) {
			t.Errorf("largeRec2 mismatch")
		}
	})

	t.Run("正常形: 単一ブロックに収まるレコードは非フラグメントとして扱われる", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// 単一ブロックに収まるレコード
		smallRec := createLogRec(1, "small")
		if _, err := logMgr.Append(smallRec); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		reconstructed := iter.Next()
		if !bytes.Equal(smallRec, reconstructed) {
			t.Errorf("small record mismatch")
		}

		// フラグメントではないことを確認（先頭がfragMagicでない）
		if len(reconstructed) >= int(fragMagicOffset) {
			p := file.NewLogPage(reconstructed)
			if p.GetInt32(0) == fragMagic {
				t.Errorf("small record should not be fragmented")
			}
		}
	})
}

// (moved master LSN tests into TestLogMgr)
