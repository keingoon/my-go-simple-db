package log

import (
	"bytes"
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
	rec := make([]byte, int32Size+file.VarBytesLen(recstrlen))
	strpos := int32(0)
	intpos := int32(int32Size + file.VarBytesLen(recstrlen))
	p := file.NewLogPage(rec)
	p.SetStr(strpos, str)
	p.SetInt32(intpos, i)
	return rec
}

func TestLogMgr(t *testing.T) {
	t.Parallel()

	t.Run("NewLogMgr: 初回はヘッダと最初のデータブロックを初期化する", func(t *testing.T) {
		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fileMgr, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		hdr := file.NewLogPage(make([]byte, blocksize))
		if err := fileMgr.Read(file.NewBlockId(logfile, 0), hdr); err != nil {
			t.Fatal(err)
		}

		t.Run("logpageのboundaryがint32Sizeである", func(t *testing.T) {
			if got := logMgr.logpage.GetInt32(0); got != int32Size {
				t.Fatalf("boundaryは%vであるべきだが%vだった", int32Size, got)
			}
		})

		t.Run("currentblkがデータブロック1を指す", func(t *testing.T) {
			got := logMgr.currentblk
			if got == nil || got.FileName() != logfile || got.Number() != 1 {
				gotFile, gotNum := "<nil>", int32(-1)
				if got != nil {
					gotFile, gotNum = got.FileName(), got.Number()
				}
				t.Fatalf("currentblkは(%s, %d)であるべきだが(%s, %d)だった", logfile, 1, gotFile, gotNum)
			}
		})

		t.Run("ヘッダ: magicが正しい", func(t *testing.T) {
			if got := hdr.GetInt32(headerMagicOffset); got != logHeaderMagic {
				t.Fatalf("header magicは%vであるべきだが%vだった", logHeaderMagic, got)
			}
		})
		t.Run("ヘッダ: versionが正しい", func(t *testing.T) {
			if got := hdr.GetInt16(headerVersionOffset); got != logHeaderVersion {
				t.Fatalf("header versionは%vであるべきだが%vだった", logHeaderVersion, got)
			}
		})
		t.Run("ヘッダ: page sizeが正しい", func(t *testing.T) {
			if got := hdr.GetInt32(headerPageSizeOffset); got != blocksize {
				t.Fatalf("header page sizeは%vであるべきだが%vだった", blocksize, got)
			}
		})
		t.Run("ヘッダ: last checkpoint LSNが0である", func(t *testing.T) {
			want := blocksize + int32Size
			if got := hdr.GetInt32(headerLastCheckpointLSNOffset); got != want {
				t.Fatalf("header last checkpoint LSNは%vであるべきだが%vだった", want, got)
			}
		})
	})

	t.Run("NewLogMgr: 不正なヘッダ(magic不一致)ならエラーになる", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		// 不正なヘッダを持つログファイルを準備する（magic不一致）
		fileMgr, err := file.NewFileMgr(t.TempDir(), blocksize)
		if err != nil {
			t.Fatal(err)
		}
		blk0, err := fileMgr.Append(logfile)
		if err != nil {
			t.Fatal(err)
		}
		if blk0.Number() != 0 {
			t.Fatalf("ヘッダブロック番号は0であるべきだが%vだった", blk0.Number())
		}
		bad := file.NewLogPage(make([]byte, blocksize))
		// leave magic as 0 (invalid), optionally fill other fields
		if err := fileMgr.Write(blk0, bad); err != nil {
			t.Fatal(err)
		}

		if _, err := NewLogMgr(fileMgr, logfile); err == nil {
			t.Fatalf("ヘッダmagic不一致のためエラーであるべきだがnilだった")
		}
	})

	t.Run("Flush: 1件追加後にFlushするとlastSavedLSNが更新される", func(t *testing.T) {
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

		if logMgr.lastSavedLSN != startLSN {
			t.Errorf("lastSavedLSNは%vであるべきだが%vだった", startLSN, logMgr.lastSavedLSN)
		}
	})

	t.Run("Iterater: endBlkは最終ブロックを指す", func(t *testing.T) {
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
			t.Errorf("endBlk numberは%vであるべきだが%vだった", 1, iter.endBlk.Number())
		}
	})

	t.Run("Append: 1件追加する", func(t *testing.T) {
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

		rec := createLogRec(1, logrecord)
		lsn, err := logMgr.Append(rec)
		if err != nil {
			t.Fatal(err)
		}

		t.Run("返るLSNがレコード開始位置である", func(t *testing.T) {
			if lsn != startLSN {
				t.Fatalf("LSNは%vであるべきだが%vだった", startLSN, lsn)
			}
		})

		t.Run("メモリ上のlogpageにレコードが書き込まれる", func(t *testing.T) {
			raw := logMgr.logpage.GetBytes(startBoundary)
			if len(raw) < int(recTypeSize) {
				t.Fatalf("rawレコードが短すぎる: len=%d", len(raw))
			}
			p := file.NewLogPage(raw)
			if got := p.GetInt32(recTypeOffset); got != recTypeNormal {
				t.Fatalf("recTypeは%vであるべきだが%vだった", recTypeNormal, got)
			}
			if !bytes.Equal(rec, raw[int(recTypeSize):]) {
				t.Fatalf("logpageのレコード(payload)が一致しない")
			}
		})
	})

	t.Run("Append: 複数件追加するとブロックを跨いで書き込まれる", func(t *testing.T) {
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

		// 物理レコード: [len:int32][recType:int32][payload...]
		bytesPerRec := int32Size + recTypeSize + recLen
		freeBytes := blocksize - int32Size

		firstBlkRecCnt := freeBytes / bytesPerRec
		secondBlkRecCnt := count - firstBlkRecCnt

		firstBlkLastRecOffset := int32Size + (firstBlkRecCnt-1)*bytesPerRec
		secondBlkLastRecOffset := int32Size + (secondBlkRecCnt-1)*bytesPerRec
		lastSavedLSN := blocksize + firstBlkLastRecOffset
		latestLSN := blocksize*2 + secondBlkLastRecOffset

		t.Run("currentblkが最終データブロックを指す", func(t *testing.T) {
			got := logMgr.currentblk
			if got == nil || got.FileName() != logfile || got.Number() != 2 {
				gotFile, gotNum := "<nil>", int32(-1)
				if got != nil {
					gotFile, gotNum = got.FileName(), got.Number()
				}
				t.Fatalf("currentblkは(%s, %d)であるべきだが(%s, %d)だった", logfile, 2, gotFile, gotNum)
			}
		})

		t.Run("メモリ上のlogpageに最後のレコードが書き込まれる", func(t *testing.T) {
			raw := logMgr.logpage.GetBytes(secondBlkLastRecOffset)
			if len(raw) < int(recTypeSize) {
				t.Fatalf("rawレコードが短すぎる: len=%d", len(raw))
			}
			p := file.NewLogPage(raw)
			if got := p.GetInt32(recTypeOffset); got != recTypeNormal {
				t.Fatalf("recTypeは%vであるべきだが%vだった", recTypeNormal, got)
			}
			if !bytes.Equal(latestRec, raw[int(recTypeSize):]) {
				t.Fatalf("最後のレコード(payload)が一致しない")
			}
		})

		t.Run("latestLSNが期待通りである", func(t *testing.T) {
			if logMgr.latestLSN != latestLSN {
				t.Fatalf("latestLSNは%vであるべきだが%vだった", latestLSN, logMgr.latestLSN)
			}
		})

		t.Run("lastSavedLSNが期待通りである", func(t *testing.T) {
			if logMgr.lastSavedLSN != lastSavedLSN {
				t.Fatalf("lastSavedLSNは%vであるべきだが%vだった", lastSavedLSN, logMgr.lastSavedLSN)
			}
		})
	})

	t.Run("appendNewBlock: 新しいブロックを追加する", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		fileMgr, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		blk, err := logMgr.appendNewBlock()
		if err != nil {
			t.Fatal(err)
		}

		t.Run("返るブロックIDが期待通りである", func(t *testing.T) {
			if blk == nil || blk.FileName() != logfile || blk.Number() != 2 {
				gotFile, gotNum := "<nil>", int32(-1)
				if blk != nil {
					gotFile, gotNum = blk.FileName(), blk.Number()
				}
				t.Fatalf("blkは(%s, %d)であるべきだが(%s, %d)だった", logfile, 2, gotFile, gotNum)
			}
		})

		t.Run("メモリ上のlogpageのboundaryが初期値である", func(t *testing.T) {
			if recpos := logMgr.logpage.GetInt32(0); recpos != int32Size {
				t.Fatalf("boundaryは%vであるべきだが%vだった", int32Size, recpos)
			}
		})

		t.Run("ディスク上のブロックのboundaryが初期値である", func(t *testing.T) {
			writenBlkPage := file.NewLogPage(make([]byte, blocksize))
			fileMgr.Read(blk, writenBlkPage)
			if recposInBlock := writenBlkPage.GetInt32(0); recposInBlock != int32Size {
				t.Fatalf("block boundaryは%vであるべきだが%vだった", int32Size, recposInBlock)
			}
		})
	})

	t.Run("Flush: 1件追加後にFlushするとディスクへ書き込まれる", func(t *testing.T) {
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
		logpage := file.NewLogPage(make([]byte, blocksize))
		fileMgr.Read(blk, logpage)
		recGeted := logpage.GetBytes(startBoundary)

		t.Run("ディスク上のレコードが一致する", func(t *testing.T) {
			if len(recGeted) < int(recTypeSize) {
				t.Fatalf("ディスク上のrawレコードが短すぎる: len=%d", len(recGeted))
			}
			p := file.NewLogPage(recGeted)
			if got := p.GetInt32(recTypeOffset); got != recTypeNormal {
				t.Fatalf("recTypeは%vであるべきだが%vだった", recTypeNormal, got)
			}
			if !bytes.Equal(rec, recGeted[int(recTypeSize):]) {
				t.Fatalf("ディスク上のレコード(payload)が一致しない")
			}
		})

		t.Run("lastSavedLSNが更新される", func(t *testing.T) {
			if logMgr.lastSavedLSN != startLSN {
				t.Fatalf("lastSavedLSNは%vであるべきだが%vだった", startLSN, logMgr.lastSavedLSN)
			}
		})
	})

	t.Run("ReadRecordAt: Flush後にlastSavedLSNで読むと追加レコードが取れる", func(t *testing.T) {
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
			t.Errorf("レコードが一致しない")
		}
	})

	t.Run("ReadRecordAt: Flush後にlastSavedLSNで読むと最後に追加したレコードが取れる", func(t *testing.T) {
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
			t.Errorf("レコードが一致しない")
		}
	})

	// Last checkpoint LSN tests
	t.Run("LastCheckpointLSN: 初期値はfirstLSNである", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize = int32(256)
			logfile   = "logfile"
		)

		_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
		if err != nil {
			t.Fatal(err)
		}

		lastCheckpointLSN, err := logMgr.ReadLastCheckpointLSN()
		if err != nil {
			t.Fatal(err)
		}
		want := blocksize + int32Size
		if lastCheckpointLSN != want {
			t.Errorf("last checkpoint LSNは%vであるべきだが%vだった", want, lastCheckpointLSN)
		}
	})

	t.Run("LastCheckpointLSN: Write後にReadでき、再オープンしても保持される", func(t *testing.T) {
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
		if err := logMgr.WriteLastCheckpointLSN(want); err != nil {
			t.Fatal(err)
		}

		lastCheckpointLSN, err := logMgr.ReadLastCheckpointLSN()
		if err != nil {
			t.Fatal(err)
		}

		reopened, err := NewLogMgr(fileMgr, logfile)
		if err != nil {
			t.Fatal(err)
		}
		lastCheckpointLSN2, err := reopened.ReadLastCheckpointLSN()
		if err != nil {
			t.Fatal(err)
		}

		t.Run("Write直後にReadすると書いた値が返る", func(t *testing.T) {
			if lastCheckpointLSN != want {
				t.Fatalf("last checkpoint LSNは%vであるべきだが%vだった", want, lastCheckpointLSN)
			}
		})

		t.Run("再オープン後も同じ値が返る", func(t *testing.T) {
			if lastCheckpointLSN2 != want {
				t.Fatalf("last checkpoint LSNは%vであるべきだが%vだった", want, lastCheckpointLSN2)
			}
		})
	})
}

func TestLogIterator(t *testing.T) {
	t.Parallel()

	t.Run("Iterater: startLSNからIteratorを作る", func(t *testing.T) {
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

		t.Run("currentposがstartBoundaryである", func(t *testing.T) {
			if logIterator.currentpos != startBoundary {
				t.Fatalf("currentposは%vであるべきだが%vだった", startBoundary, logIterator.currentpos)
			}
		})
		t.Run("boundaryがstartBoundaryである", func(t *testing.T) {
			if logIterator.boundary != startBoundary {
				t.Fatalf("boundaryは%vであるべきだが%vだった", startBoundary, logIterator.boundary)
			}
		})
	})

	t.Run("HasNext: 既存レコード数と消費数に応じて真偽が決まる", func(t *testing.T) {
		t.Parallel()

		const (
			blocksize     = int32(256)
			logfile       = "logfile"
			logrecord     = "logrecord"
			startBoundary = int32Size
			startLSN      = int32(blocksize + startBoundary)
		)

		type tc struct {
			name      string
			recCount  int
			nextCalls int
			want      bool
		}

		tests := []tc{
			{name: "1件ある: Next未呼び出しならtrue", recCount: 1, nextCalls: 0, want: true},
			{name: "1件ある: 1回消費したらfalse", recCount: 1, nextCalls: 1, want: false},
			{name: "20件ある: 15回消費ならtrue", recCount: 20, nextCalls: 15, want: true},
			{name: "20件ある: 20回消費ならfalse", recCount: 20, nextCalls: 20, want: false},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				_, logMgr, err := initFileLogMgr(t.TempDir(), blocksize, logfile)
				if err != nil {
					t.Fatal(err)
				}

				for i := 0; i < tt.recCount; i++ {
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
				for i := 0; i < tt.nextCalls; i++ {
					iter.Next()
				}

				if got := iter.HasNext(); got != tt.want {
					t.Fatalf("HasNextは%vであるべきだが%vだった (recCount=%d, nextCalls=%d)", tt.want, got, tt.recCount, tt.nextCalls)
				}
			})
		}
	})

	t.Run("Next: 20件追加後に順に取り出せる", func(t *testing.T) {
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

		t.Run("レコードが追加順に返る", func(t *testing.T) {
			iter, err := logMgr.Iterater(startLSN)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < nextCalledCount; i++ {
				_, rec := iter.Next()
				want := createLogRec(int32(i+1), logrecord)
				if !bytes.Equal(rec, want) {
					t.Fatalf("Nextで返るレコードが期待と一致しない (index=%d)", i)
				}
			}
		})

		t.Run("返ったLSNでReadRecordAtすると同じレコードが取れる", func(t *testing.T) {
			iter, err := logMgr.Iterater(startLSN)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < nextCalledCount; i++ {
				lsn, rec := iter.Next()
				got, err := logMgr.ReadRecordAt(lsn)
				if err != nil {
					t.Fatalf("ReadRecordAt(lsn=%d)が失敗した: %v", lsn, err)
				}
				if !bytes.Equal(rec, got) {
					t.Fatalf("lsn=%dはNextが返したレコードを指すべきだが一致しない (index=%d)", lsn, i)
				}
			}
		})
	})

	t.Run("moveToBlock: 次ブロックへ移動すると指定offsetから読める", func(t *testing.T) {
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
			// 物理レコード: [len:int32][recType:int32][payload...]
			bytesPerRec = int32Size + recTypeSize + int32(len(rec))
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
		firstBlkRecCnt := (blocksize - int32Size) / bytesPerRec
		recFirstExpected := createLogRec(firstBlkRecCnt+1, logrecord)

		t.Run("currentposが指定offsetになる", func(t *testing.T) {
			if iter.currentpos != int32Size {
				t.Fatalf("currentposは%vであるべきだが%vだった", int32Size, iter.currentpos)
			}
		})

		t.Run("指定offsetから最初のレコードが読める", func(t *testing.T) {
			raw := p.GetBytes(int32Size)
			if len(raw) < int(recTypeSize) {
				t.Fatalf("rawレコードが短すぎる: len=%d", len(raw))
			}
			pr := file.NewLogPage(raw)
			if got := pr.GetInt32(recTypeOffset); got != recTypeNormal {
				t.Fatalf("recTypeは%vであるべきだが%vだった", recTypeNormal, got)
			}
			if !bytes.Equal(raw[int(recTypeSize):], recFirstExpected) {
				t.Fatalf("moveToBlock後の最初のレコードが期待と一致しない")
			}
		})
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
		lsn, reconstructed := iter.Next()
		t.Run("LSNがstartLSNである", func(t *testing.T) {
			if lsn != startLSN {
				t.Fatalf("LSNは%vであるべきだが%vだった", startLSN, lsn)
			}
		})
		t.Run("レコードが元と一致する", func(t *testing.T) {
			if !bytes.Equal(smallRec, reconstructed) {
				t.Fatalf("レコードが一致しない")
			}
		})
		t.Run("ディスク上のtypeがnormalである", func(t *testing.T) {
			raw := logMgr.logpage.GetBytes(startBoundary)
			if len(raw) < int(recTypeSize) {
				t.Fatalf("rawレコードが短すぎる: len=%d", len(raw))
			}
			p := file.NewLogPage(raw)
			if got := p.GetInt32(recTypeOffset); got != recTypeNormal {
				t.Fatalf("recTypeは%vであるべきだが%vだった", recTypeNormal, got)
			}
		})
	})

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
			t.Fatalf("Appendが失敗した: %v", err)
		}
		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}
		if !iter.HasNext() {
			t.Fatal("少なくとも1件のレコードがあるべき")
		}

		lsn, reconstructed := iter.Next()
		t.Run("LSNがstartLSNである", func(t *testing.T) {
			if lsn != startLSN {
				t.Fatalf("LSNは%vであるべきだが%vだった", startLSN, lsn)
			}
		})
		t.Run("復元されたレコードが元と一致する", func(t *testing.T) {
			if !bytes.Equal(largeRec, reconstructed) {
				t.Fatalf("復元されたレコードが一致しない")
			}
		})
	})

	t.Run("正常形: 通常レコードとフラグメントレコードが混在しても追加順に読める", func(t *testing.T) {
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

		// 通常レコード・フラグメントレコードが混在していても、
		// Iterator が追加順に論理レコードを返すことを確認する。
		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		// 実行は先に行い、検証をsubtestに分割する（1ケース=1期待値）
		lsn1, gotRec1 := iter.Next()
		lsn2, gotLarge1 := iter.Next()
		// rec1は単一レコードとして保存されるため、物理サイズはtype分だけ増える
		rec1BytesNeeded := int32Size + recTypeSize + int32(len(rec1))
		largeRec1lsn := startLSN + rec1BytesNeeded
		lsn3, gotRec2 := iter.Next()
		readBackRec2, err := logMgr.ReadRecordAt(lsn3)
		if err != nil {
			t.Fatalf("ReadRecordAt(rec2 lsn=%d)が失敗した: %v", lsn3, err)
		}
		lsn4, gotLarge2 := iter.Next()
		readBackLarge2, err := logMgr.ReadRecordAt(lsn4)
		if err != nil {
			t.Fatalf("ReadRecordAt(largeRec2 lsn=%d)が失敗した: %v", lsn4, err)
		}

		t.Run("1件目: LSNがstartLSNである", func(t *testing.T) {
			if lsn1 != startLSN {
				t.Fatalf("1件目のLSNは%vであるべきだが%vだった", startLSN, lsn1)
			}
		})
		t.Run("1件目: レコードがrec1である", func(t *testing.T) {
			if !bytes.Equal(rec1, gotRec1) {
				t.Fatalf("1件目のレコードがrec1と一致しない")
			}
		})
		t.Run("2件目: 大きいレコードlargeRec1のLSNが先頭の通常レコード直後である", func(t *testing.T) {
			if lsn2 != largeRec1lsn {
				t.Fatalf("2件目のLSNは%vであるべきだが%vだった", largeRec1lsn, lsn2)
			}
		})
		t.Run("2件目: フラグメント化されたlargeRec1が1件の論理レコードとして復元される", func(t *testing.T) {
			if !bytes.Equal(largeRec1, gotLarge1) {
				t.Fatalf("2件目のレコードがlargeRec1と一致しない")
			}
		})
		t.Run("3件目: largeRec1の次に通常レコードrec2が返る", func(t *testing.T) {
			if !bytes.Equal(rec2, gotRec2) {
				t.Fatalf("3件目のレコードがrec2と一致しない")
			}
		})
		t.Run("3件目: 返ってきたLSNでReadRecordAtしてもrec2が取れる", func(t *testing.T) {
			if !bytes.Equal(rec2, readBackRec2) {
				t.Fatalf("ReadRecordAt(lsn=%d)でrec2が取れない", lsn3)
			}
		})
		t.Run("4件目: 次の大きいレコードlargeRec2も1件の論理レコードとして復元される", func(t *testing.T) {
			if !bytes.Equal(largeRec2, gotLarge2) {
				t.Fatalf("4件目のレコードがlargeRec2と一致しない")
			}
		})
		t.Run("4件目: 返ってきたLSNでReadRecordAtしてもlargeRec2が取れる", func(t *testing.T) {
			if !bytes.Equal(largeRec2, readBackLarge2) {
				t.Fatalf("ReadRecordAt(lsn=%d)でlargeRec2が取れない", lsn4)
			}
		})
	})

	t.Run("異常形: LASTが欠けたフラグメントチェインはスキップされ、次の通常レコードが返る", func(t *testing.T) {
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

		// LAST を書かずに FIRST + CONT だけを人工的に挿入し、
		// 壊れたフラグメントチェインがスキップされる状況を作る。
		totalLen := 100
		dataFirst := bytes.Repeat([]byte{1}, 40)
		dataCont := bytes.Repeat([]byte{2}, 40)

		firstFrag := logMgr.buildFragment(dataFirst, totalLen, true /* first */, false /* isLast */)
		if err := logMgr.ensureAndWrite(firstFrag); err != nil {
			t.Fatalf("ensureAndWrite(firstFrag)が失敗した: %v", err)
		}

		contFrag := logMgr.buildFragment(dataCont, totalLen, false /* first */, false /* isLast */)
		if err := logMgr.ensureAndWrite(contFrag); err != nil {
			t.Fatalf("ensureAndWrite(contFrag)が失敗した: %v", err)
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

		// 実行は先に行い、検証をsubtestに分割する（1ケース=1期待値）
		lsn1, got1 := iter.Next()
		lsn2, got2 := iter.Next()

		t.Run("1件目: LSNがstartLSNである", func(t *testing.T) {
			if lsn1 != startLSN {
				t.Fatalf("1件目のLSNは%vであるべきだが%vだった", startLSN, lsn1)
			}
		})
		t.Run("1件目: レコードがrec1である", func(t *testing.T) {
			if !bytes.Equal(rec1, got1) {
				t.Fatalf("1件目のレコードがrec1と一致しない")
			}
		})
		t.Run("2件目: 壊れたフラグメントチェインはスキップされ、次の通常レコードrec2が返る", func(t *testing.T) {
			if !bytes.Equal(rec2, got2) {
				t.Fatalf("2件目のレコードがrec2と一致しない")
			}
		})
		t.Run("2件目: 返ってきたLSNでReadRecordAtしてもrec2が取れる", func(t *testing.T) {
			got, err := logMgr.ReadRecordAt(lsn2)
			if err != nil {
				t.Fatalf("ReadRecordAt(lsn=%d)が失敗した: %v", lsn2, err)
			}
			if !bytes.Equal(rec2, got) {
				t.Fatalf("ReadRecordAt(lsn=%d)でrec2が取れない", lsn2)
			}
		})
		t.Run("3件目: 壊れたフラグメントチェインは独立した論理レコードとして返らない", func(t *testing.T) {
			if iter.HasNext() {
				t.Fatalf("3件目のレコードがこれ以上無いはずだがある")
			}
		})
	})

	t.Run("異常形: FIRSTヘッダ検証失敗はスキップされる", func(t *testing.T) {
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

		rec1 := createLogRec(1, "normal1")
		if _, err := logMgr.Append(rec1); err != nil {
			t.Fatal(err)
		}

		badFirst := make([]byte, fragFirstPayloadOffset)
		p := file.NewLogPage(badFirst)
		if err := p.SetInt32(recTypeOffset, recTypeFragment); err != nil {
			t.Fatalf("不正FIRSTのtype書き込みに失敗した: %v", err)
		}
		if err := p.SetInt32(fragFlagsOffset, fragFirst); err != nil {
			t.Fatalf("不正FIRSTのflags書き込みに失敗した: %v", err)
		}
		if err := p.SetInt32(fragFirstTotalLenOffset, 0); err != nil {
			t.Fatalf("不正FIRSTのtotalLen書き込みに失敗した: %v", err)
		}
		if err := logMgr.ensureAndWrite(badFirst); err != nil {
			t.Fatalf("不正FIRSTの挿入に失敗した: %v", err)
		}

		rec2 := createLogRec(2, "normal2")
		if _, err := logMgr.Append(rec2); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		lsn1, got1 := iter.Next()
		lsn2, got2 := iter.Next()
		got, err := logMgr.ReadRecordAt(lsn2)
		if err != nil {
			t.Fatalf("ReadRecordAt(lsn=%d)が失敗した: %v", lsn2, err)
		}

		t.Run("1件目: LSNがstartLSNである", func(t *testing.T) {
			if lsn1 != startLSN {
				t.Fatalf("1件目のLSNは%vであるべきだが%vだった", startLSN, lsn1)
			}
		})
		t.Run("1件目: レコードがrec1である", func(t *testing.T) {
			if !bytes.Equal(rec1, got1) {
				t.Fatalf("1件目のレコードがrec1と一致しない")
			}
		})
		t.Run("2件目: レコードがrec2である", func(t *testing.T) {
			if !bytes.Equal(rec2, got2) {
				t.Fatalf("2件目のレコードがrec2と一致しない")
			}
		})
		t.Run("2件目: 返ってきたLSNでReadRecordAtするとrec2が取れる", func(t *testing.T) {
			if !bytes.Equal(rec2, got) {
				t.Fatalf("ReadRecordAt(lsn=%d)でrec2が取れない", lsn2)
			}
		})
		t.Run("3件目: レコードがこれ以上無い", func(t *testing.T) {
			if iter.HasNext() {
				t.Fatalf("3件目のレコードがこれ以上無いはずだがある")
			}
		})
	})

	t.Run("異常形: CONT/LAST単独はスキップされる", func(t *testing.T) {
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

		rec1 := createLogRec(1, "normal1")
		if _, err := logMgr.Append(rec1); err != nil {
			t.Fatal(err)
		}

		contOnly := logMgr.buildFragment([]byte{1, 2, 3, 4}, 16, false /* first */, false /* isLast */)
		if err := logMgr.ensureAndWrite(contOnly); err != nil {
			t.Fatalf("孤立CONTの挿入に失敗した: %v", err)
		}
		lastOnly := logMgr.buildFragment([]byte{5, 6, 7, 8}, 16, false /* first */, true /* isLast */)
		if err := logMgr.ensureAndWrite(lastOnly); err != nil {
			t.Fatalf("孤立LASTの挿入に失敗した: %v", err)
		}

		rec2 := createLogRec(2, "normal2")
		if _, err := logMgr.Append(rec2); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		lsn1, got1 := iter.Next()
		lsn2, got2 := iter.Next()
		got, err := logMgr.ReadRecordAt(lsn2)
		if err != nil {
			t.Fatalf("ReadRecordAt(lsn=%d)が失敗した: %v", lsn2, err)
		}

		t.Run("1件目: LSNがstartLSNである", func(t *testing.T) {
			if lsn1 != startLSN {
				t.Fatalf("1件目のLSNは%vであるべきだが%vだった", startLSN, lsn1)
			}
		})
		t.Run("1件目: レコードがrec1である", func(t *testing.T) {
			if !bytes.Equal(rec1, got1) {
				t.Fatalf("1件目のレコードがrec1と一致しない")
			}
		})
		t.Run("2件目: レコードがrec2である", func(t *testing.T) {
			if !bytes.Equal(rec2, got2) {
				t.Fatalf("2件目のレコードがrec2と一致しない")
			}
		})
		t.Run("2件目: 返ってきたLSNでReadRecordAtするとrec2が取れる", func(t *testing.T) {
			if !bytes.Equal(rec2, got) {
				t.Fatalf("ReadRecordAt(lsn=%d)でrec2が取れない", lsn2)
			}
		})
		t.Run("3件目: レコードがこれ以上無い", func(t *testing.T) {
			if iter.HasNext() {
				t.Fatalf("3件目のレコードがこれ以上無いはずだがある")
			}
		})
	})

	t.Run("異常形: 想定外フラグはスキップされる", func(t *testing.T) {
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

		rec1 := createLogRec(1, "normal1")
		if _, err := logMgr.Append(rec1); err != nil {
			t.Fatal(err)
		}

		badFlags := make([]byte, fragPayloadOffset)
		p := file.NewLogPage(badFlags)
		if err := p.SetInt32(recTypeOffset, recTypeFragment); err != nil {
			t.Fatalf("不正flagsレコードのtype書き込みに失敗した: %v", err)
		}
		if err := p.SetInt32(fragFlagsOffset, 99); err != nil {
			t.Fatalf("不正flagsレコードのflags書き込みに失敗した: %v", err)
		}
		if err := logMgr.ensureAndWrite(badFlags); err != nil {
			t.Fatalf("不正flagsレコードの挿入に失敗した: %v", err)
		}

		rec2 := createLogRec(2, "normal2")
		if _, err := logMgr.Append(rec2); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		lsn1, got1 := iter.Next()
		lsn2, got2 := iter.Next()
		got, err := logMgr.ReadRecordAt(lsn2)
		if err != nil {
			t.Fatalf("ReadRecordAt(lsn=%d)が失敗した: %v", lsn2, err)
		}

		t.Run("1件目: LSNがstartLSNである", func(t *testing.T) {
			if lsn1 != startLSN {
				t.Fatalf("1件目のLSNは%vであるべきだが%vだった", startLSN, lsn1)
			}
		})
		t.Run("1件目: レコードがrec1である", func(t *testing.T) {
			if !bytes.Equal(rec1, got1) {
				t.Fatalf("1件目のレコードがrec1と一致しない")
			}
		})
		t.Run("2件目: レコードがrec2である", func(t *testing.T) {
			if !bytes.Equal(rec2, got2) {
				t.Fatalf("2件目のレコードがrec2と一致しない")
			}
		})
		t.Run("2件目: 返ってきたLSNでReadRecordAtするとrec2が取れる", func(t *testing.T) {
			if !bytes.Equal(rec2, got) {
				t.Fatalf("ReadRecordAt(lsn=%d)でrec2が取れない", lsn2)
			}
		})
		t.Run("3件目: レコードがこれ以上無い", func(t *testing.T) {
			if iter.HasNext() {
				t.Fatalf("3件目のレコードがこれ以上無いはずだがある")
			}
		})
	})

	t.Run("異常形: 壊れた通常レコードはスキップされる", func(t *testing.T) {
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

		rec1 := createLogRec(1, "normal1")
		if _, err := logMgr.Append(rec1); err != nil {
			t.Fatal(err)
		}

		corruptRec := make([]byte, recTypeSize+4)
		p := file.NewLogPage(corruptRec)
		if err := p.SetInt32(recTypeOffset, 99); err != nil {
			t.Fatalf("壊れた通常レコードのtype書き込みに失敗した: %v", err)
		}
		if err := p.SetInt32(recTypeSize, 12345); err != nil {
			t.Fatalf("壊れた通常レコードのpayload書き込みに失敗した: %v", err)
		}
		if err := logMgr.ensureAndWrite(corruptRec); err != nil {
			t.Fatalf("壊れた通常レコードの挿入に失敗した: %v", err)
		}

		rec2 := createLogRec(2, "normal2")
		if _, err := logMgr.Append(rec2); err != nil {
			t.Fatal(err)
		}

		logMgr.Flush(logMgr.latestLSN)

		iter, err := logMgr.Iterater(startLSN)
		if err != nil {
			t.Fatal(err)
		}

		lsn1, got1 := iter.Next()
		lsn2, got2 := iter.Next()
		got, err := logMgr.ReadRecordAt(lsn2)
		if err != nil {
			t.Fatalf("ReadRecordAt(lsn=%d)が失敗した: %v", lsn2, err)
		}

		t.Run("1件目: LSNがstartLSNである", func(t *testing.T) {
			if lsn1 != startLSN {
				t.Fatalf("1件目のLSNは%vであるべきだが%vだった", startLSN, lsn1)
			}
		})
		t.Run("1件目: レコードがrec1である", func(t *testing.T) {
			if !bytes.Equal(rec1, got1) {
				t.Fatalf("1件目のレコードがrec1と一致しない")
			}
		})
		t.Run("2件目: レコードがrec2である", func(t *testing.T) {
			if !bytes.Equal(rec2, got2) {
				t.Fatalf("2件目のレコードがrec2と一致しない")
			}
		})
		t.Run("2件目: 返ってきたLSNでReadRecordAtするとrec2が取れる", func(t *testing.T) {
			if !bytes.Equal(rec2, got) {
				t.Fatalf("ReadRecordAt(lsn=%d)でrec2が取れない", lsn2)
			}
		})
		t.Run("3件目: レコードがこれ以上無い", func(t *testing.T) {
			if iter.HasNext() {
				t.Fatalf("3件目のレコードがこれ以上無いはずだがある")
			}
		})
	})
}
