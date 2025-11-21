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
		expectedpage.SetInt32(0, blocksize)
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
			blocksize = int32(256)
			logfile   = "logfile"
			logrecord = "logrecord"
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// Append single log rec
		rec := createLogRec(1, logrecord)
		logMgr.Append(rec)

		// Flush single log rec
		logMgr.Flush(1)

		if logMgr.latestLSN != 1 {
			t.Errorf("expected latestLSN %v, got %v", 1, logMgr.latestLSN)
		}

		if logMgr.lastSavedLSN != 1 {
			t.Errorf("expected lastSavedLSN %v, got %v", 1, logMgr.lastSavedLSN)
		}
	})

	t.Run("Iterater after Append single log rec", func(t *testing.T) {
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

		// Append single log rec
		rec := createLogRec(1, logrecord)
		logMgr.Append(rec)

		// Iterater single log rec
		logMgr.Iterater()

		if logMgr.latestLSN != 1 {
			t.Errorf("expected latestLSN %v, got %v", 1, logMgr.latestLSN)
		}

		if logMgr.lastSavedLSN != 1 {
			t.Errorf("expected lastSavedLSN %v, got %v", 1, logMgr.lastSavedLSN)
		}
	})

	t.Run("Append single log rec", func(t *testing.T) {
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

		// Append single log rec
		rec := createLogRec(1, logrecord)
		lsn, err := logMgr.Append(rec)
		if err != nil {
			t.Fatal(err)
		}

		if lsn != 1 {
			t.Errorf("expected latestLSN %v, got %v", 1, logMgr.latestLSN)
		}

		logpage := logMgr.logpage
		recpos := logpage.GetInt32(0)
		recGeted := logpage.GetBytes(recpos)
		if !bytes.Equal(rec, recGeted) {
			t.Errorf("expected logpage rec %v, got %v", []byte(rec), recGeted)
		}
	})

	t.Run("Append 20 log rec", func(t *testing.T) {
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

		// Append 20 log rec
		var latestRec []byte
		for i := 0; i < 20; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
			latestRec = rec
		}

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

		if logMgr.latestLSN != 20 {
			t.Errorf("expected latestLSN %v, got %v", 20, logMgr.latestLSN)
		}

		// blocksize / (17 + int32Size) = 12
		if logMgr.lastSavedLSN != 12 {
			t.Errorf("expected lastSavedLSN %v, got %v", 12, logMgr.lastSavedLSN)
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
		if recpos != blocksize {
			t.Errorf("expected logpage recpos %v, got %v", blocksize, recpos)
		}

		fileMgr.Read(blk, logpage)
		recposInBlock := logpage.GetInt32(0)
		if recposInBlock != blocksize {
			t.Errorf("expected recpos in block %v, got %v", blocksize, recposInBlock)
		}
	})

	t.Run("flush after Append 1 rec", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
			logrecord = "logrecord"
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

		logMgr.flush()

		blk := logMgr.currentblk
		logpage := logMgr.logpage
		fileMgr.Read(blk, logpage)
		recpos := logpage.GetInt32(0)
		recGeted := logpage.GetBytes(recpos)

		if !bytes.Equal(rec, recGeted) {
			t.Errorf("expected rec in block %v, got %v", rec, recGeted)
		}

		if logMgr.lastSavedLSN != 1 {
			t.Errorf("expected lastSavedLSN %v, got %v", 1, logMgr.lastSavedLSN)
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
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fileMgr, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}
		logIterator := NewLogIterator(fileMgr, logMgr.currentblk)

		if logIterator.currentpos != blocksize {
			t.Errorf("expected currentpos %v, got %v", blocksize, logIterator.currentpos)
		}
		if logIterator.boundary != blocksize {
			t.Errorf("expected boundary %v, got %v", blocksize, logIterator.boundary)
		}
	})

	t.Run("HasNext called after Append 1 rec", func(t *testing.T) {
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

		// Append single log rec
		rec := createLogRec(1, logrecord)
		if _, err := logMgr.Append(rec); err != nil {
			t.Fatal(err)
		}

		iter := logMgr.Iterater()
		hasNext := iter.HasNext()
		if !hasNext {
			t.Errorf("expected HasNext %v, got %v", true, hasNext)
		}
	})

	t.Run("HasNext called from Next call after Append 1 rec", func(t *testing.T) {
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

		// Append single log rec
		rec := createLogRec(1, logrecord)
		if _, err := logMgr.Append(rec); err != nil {
			t.Fatal(err)
		}

		iter := logMgr.Iterater()
		iter.Next()
		hasNext := iter.HasNext()
		if hasNext {
			t.Errorf("expected HasNext %v, got %v", false, hasNext)
		}
	})

	t.Run("HasNext called from Next 15 called after Append 20 rec", func(t *testing.T) {
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

		// Append 20 log rec
		for i := 0; i < 20; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
		}

		iter := logMgr.Iterater()
		for i := 0; i < 15; i++ {
			iter.Next()
		}

		hasNext := iter.HasNext()
		if !hasNext {
			t.Errorf("expected HasNext %v, got %v", true, hasNext)
		}
	})

	t.Run("HasNext called from Next 20 called after Append 20 rec", func(t *testing.T) {
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

		// Append 20 log rec
		for i := 0; i < 20; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
		}

		iter := logMgr.Iterater()
		for i := 0; i < 20; i++ {
			iter.Next()
		}

		hasNext := iter.HasNext()
		if hasNext {
			t.Errorf("expected HasNext %v, got %v", false, hasNext)
		}
	})

	t.Run("Next 15 called after Append 20 rec", func(t *testing.T) {
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

		// Append 20 log rec
		for i := 0; i < 20; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
		}

		iter := logMgr.Iterater()
		var rec []byte
		for i := 0; i < 15; i++ {
			rec = iter.Next()
		}
		recExpected := createLogRec(int32(15), logrecord)
		if !bytes.Equal(rec, recExpected) {
			t.Errorf("expected Next rec %v, got %v", recExpected, rec)
		}
	})

	t.Run("moveToBlock after Append 20 rec", func(t *testing.T) {
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

		// Append 20 log rec
		for i := 0; i < 20; i++ {
			rec := createLogRec(int32(i+1), logrecord)
			if _, err := logMgr.Append(rec); err != nil {
				t.Fatal(err)
			}
		}

		iter := logMgr.Iterater()
		// currentBlk number is 2, nextBlk number is 1 (header block is 0)
		nextBlk := file.NewBlockId(logfile, 1)
		iter.moveToBlock(nextBlk)

		p := iter.p
		recpos := p.GetInt32(0)
		recFirst := p.GetBytes(recpos)
		// blocksize / (17 + int32Size) = 12
		recFirstExpected := createLogRec(12, logrecord)
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
	// 注意: Appendが自動的に先頭に長さを付与するため、ここでは長さを含めない
	createLargeLogRec := func(size int) []byte {
		rec := make([]byte, size)
		// データを埋める（先頭に長さは含めない。Appendが自動的に付与する）
		for i := 0; i < size; i++ {
			rec[i] = byte(i % 256)
		}
		return rec
	}

	t.Run("正常形: first->cont->last が正しく復元できる", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		// ブロックサイズを超える大きなレコードを作成
		largeRec := createLargeLogRec(int(blocksize) + 100)
		lsn, err := logMgr.Append(largeRec)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if lsn <= 0 {
			t.Errorf("expected LSN > 0, got %d", lsn)
		}

		// イテレータで読み戻し
		iter := logMgr.Iterater()
		if !iter.HasNext() {
			t.Fatal("expected at least one record")
		}

		reconstructed := iter.Next()
		if !bytes.Equal(largeRec, reconstructed) {
			t.Errorf("reconstructed record mismatch: expected len=%d, got len=%d", len(largeRec), len(reconstructed))
		}
	})

	t.Run("正常形: 複数ブロックにまたがる大きなレコード", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
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

		iter := logMgr.Iterater()
		reconstructed := iter.Next()
		if !bytes.Equal(largeRec, reconstructed) {
			t.Errorf("reconstructed record mismatch: expected len=%d, got len=%d", len(largeRec), len(reconstructed))
		}
	})

	t.Run("異常形: lastがない（チェイン末尾欠け）", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
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

		iter := logMgr.Iterater()

		// 直近の通常レコード rec2 が最初に返る
		got := iter.Next()
		if !bytes.Equal(rec2, got) {
			t.Errorf("expected latest record %v, got %v", rec2, got)
		}

		// 次の Next 呼び出しでは、壊れたフラグメントチェインをスキップして rec1 が返る
		got = iter.Next()
		if !bytes.Equal(rec1, got) {
			t.Errorf("expected older record %v, got %v", rec1, got)
		}

		// それ以上の論理レコードは存在しない
		if next := iter.Next(); next != nil {
			t.Errorf("expected no more logical records, got %v", next)
		}
	})

	t.Run("正常形: 複数のフラグメントレコードが混在", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
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

		// 逆順で読み戻し（Iteratorは最新から読む）
		iter := logMgr.Iterater()
		got2 := iter.Next()
		if !bytes.Equal(largeRec2, got2) {
			t.Errorf("largeRec2 mismatch")
		}

		gotRec2 := iter.Next()
		if !bytes.Equal(rec2, gotRec2) {
			t.Errorf("rec2 mismatch")
		}

		got1 := iter.Next()
		if !bytes.Equal(largeRec1, got1) {
			t.Errorf("largeRec1 mismatch")
		}

		gotRec1 := iter.Next()
		if !bytes.Equal(rec1, gotRec1) {
			t.Errorf("rec1 mismatch")
		}
	})

	t.Run("正常形: 単一ブロックに収まるレコードは非フラグメントとして扱われる", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
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

		iter := logMgr.Iterater()
		reconstructed := iter.Next()
		if !bytes.Equal(smallRec, reconstructed) {
			t.Errorf("small record mismatch")
		}

		// フラグメントではないことを確認（先頭がfragMagicでない）
		if len(reconstructed) >= 4 {
			p := file.NewLogPage(reconstructed)
			if p.GetInt32(0) == fragMagic {
				t.Errorf("small record should not be fragmented")
			}
		}
	})
}

// (moved master LSN tests into TestLogMgr)
