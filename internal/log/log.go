package log

import (
	"fmt"
	"sync"

	"github.com/keingoon/simpledb/internal/file"
)

const (
	int32Size = 4
	int16Size = 2
)

// 物理レコードのペイロード共通ヘッダ:
// [recType:int32][payload...]
//
// - recTypeNormal: 通常ログレコード（payloadは呼び出し側が定義）
// - recTypeFragment: フラグメント（payloadは flags/totalLen/payload を含む）
const (
	recTypeSize     int32 = int32Size
	recTypeOffset   int32 = 0
	recTypeNormal   int32 = 1
	recTypeFragment int32 = 2
)

// ログファイルヘッダー (ブロック0) のレイアウト:
// magic(4) | version(2) | pageSize(4) | lastCheckpointLSN(4) | reserved(...)
const (
	headerMagicOffset             = 0
	headerVersionOffset           = headerMagicOffset + int32Size
	headerPageSizeOffset          = headerVersionOffset + int16Size
	headerLastCheckpointLSNOffset = headerPageSizeOffset + int32Size
)

const (
	// "LOGH" as bytes in little-endian: 'L','O','G','H'
	logHeaderMagic   int32 = 0x48474F4C
	logHeaderVersion int16 = 1
)

// 1つの物理レコード内に格納するフラグメントのレイアウト:
// FIRST: recType(4) | flags(4) | totalLen(4) | payload
// CONT:  recType(4) | flags(4) | payload
// LAST:  recType(4) | flags(4) | payload
const (
	fragFirst int32 = 1
	fragCont  int32 = 2
	fragLast  int32 = 3

	fragFlagsOffset         int32 = recTypeSize
	fragFirstTotalLenOffset int32 = fragFlagsOffset + int32Size
	fragFirstPayloadOffset  int32 = fragFirstTotalLenOffset + int32Size
	fragPayloadOffset       int32 = fragFlagsOffset + int32Size
)

type LogMgr struct {
	fm           *file.FileMgr
	logfile      string
	logpage      *file.Page
	currentblk   *file.BlockId
	latestLSN    int32
	lastSavedLSN int32
	mu           sync.RWMutex
}

func NewLogMgr(fm *file.FileMgr, logfile string) (*LogMgr, error) {
	logMgr := &LogMgr{fm: fm, logfile: logfile, logpage: nil, currentblk: nil, latestLSN: 0, lastSavedLSN: 0}

	b := make([]byte, fm.BlockSize())
	logpage := file.NewLogPage(b)
	logsize, err := fm.Length(logfile)
	if err != nil {
		return nil, fmt.Errorf("could not new log mgr: %w", err)
	}
	logMgr.logpage = logpage

	if logsize == 0 {
		// ログヘッダーブロックの初期化
		hdrBlk, err := fm.Append(logfile)
		if err != nil {
			return nil, fmt.Errorf("could not append header block: %w", err)
		}
		if hdrBlk.Number() != 0 {
			return nil, fmt.Errorf("unexpected header block number: %d", hdrBlk.Number())
		}

		hdr := file.NewLogPage(make([]byte, fm.BlockSize()))
		if err := hdr.SetInt32(headerMagicOffset, logHeaderMagic); err != nil {
			return nil, fmt.Errorf("could not write header magic: %w", err)
		}
		if err := hdr.SetInt16(headerVersionOffset, logHeaderVersion); err != nil {
			return nil, fmt.Errorf("could not write header version: %w", err)
		}
		if err := hdr.SetInt32(headerPageSizeOffset, fm.BlockSize()); err != nil {
			return nil, fmt.Errorf("could not write header page size: %w", err)
		}
		firstLSN := fm.BlockSize() + int32Size
		if err := hdr.SetInt32(headerLastCheckpointLSNOffset, firstLSN); err != nil {
			return nil, fmt.Errorf("could not write header last checkpoint lsn: %w", err)
		}
		if err := fm.Write(hdrBlk, hdr); err != nil {
			return nil, fmt.Errorf("could not write header block: %w", err)
		}
	} else {
		// ログヘッダーブロックの値チェックバリデーション
		hdrBlk := file.NewBlockId(logfile, 0)
		hdr := file.NewLogPage(make([]byte, fm.BlockSize()))
		if err := fm.Read(hdrBlk, hdr); err != nil {
			return nil, fmt.Errorf("could not read header block: %w", err)
		}
		if hdr.GetInt32(headerMagicOffset) != logHeaderMagic {
			return nil, fmt.Errorf("invalid log header magic")
		}
		if hdr.GetInt32(headerPageSizeOffset) != fm.BlockSize() {
			return nil, fmt.Errorf("mismatched page size in header")
		}
	}

	// ログデータブロックの追加
	if logsize <= 1 {
		currentblk, err := logMgr.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("could not create first data block: %w", err)
		}
		logMgr.currentblk = currentblk
	} else {
		currentblk := file.NewBlockId(logfile, logsize-1)
		if err := fm.Read(currentblk, logpage); err != nil {
			return nil, fmt.Errorf("could not load current log block: %w", err)
		}
		logMgr.currentblk = currentblk
	}

	return logMgr, nil
}

func (LogMgr *LogMgr) Flush(lsn int32) {
	if lsn > LogMgr.lastSavedLSN {
		LogMgr.flush()
	}
}

func (logMgr *LogMgr) Iterater(startLSN int32) (*LogIterator, error) {
	logsize, err := logMgr.fm.Length(logMgr.logfile)
	if err != nil {
		return nil, fmt.Errorf("could not get log size: %w", err)
	}
	endBlkNum := logsize - 1
	return newLogIterator(logMgr.fm, logMgr.logfile, startLSN, endBlkNum), nil
}

func (logMgr *LogMgr) Append(logrec []byte) (int32, error) {
	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()

	if logMgr.canFitAsSingle(logrec) {
		return logMgr.writeSingleRecord(logrec)
	}
	return logMgr.writeFragmentedRecord(logrec)
}

func (logMgr *LogMgr) ReadRecordAt(lsn int32) ([]byte, error) {
	logsize, err := logMgr.fm.Length(logMgr.logfile)
	if err != nil {
		return nil, fmt.Errorf("could not get log size: %w", err)
	}
	endBlkNum := logsize - 1
	iter := newLogIterator(logMgr.fm, logMgr.logfile, lsn, endBlkNum)
	if !iter.HasNext() {
		return nil, fmt.Errorf("could not read record at lsn: %d", lsn)
	}
	_, rec := iter.Next()

	return rec, nil
}

// レコードが単一ブロックに収まるか判定する
func (logMgr *LogMgr) canFitAsSingle(logrec []byte) bool {
	maxBytesInEmptyBlock := int(logMgr.fm.BlockSize()) - 2*int(int32Size)
	// 物理レコードのpayload先頭にrecTypeが入る分を考慮する
	return recTypeSize+int32(len(logrec)) <= int32(maxBytesInEmptyBlock)
}

// 単一レコードとして書き込む
func (logMgr *LogMgr) writeSingleRecord(logrec []byte) (int32, error) {
	// ディスク上は [recType:int32][payload...] として保存する
	rec := make([]byte, int(recTypeSize)+len(logrec))
	p := file.NewLogPage(rec)
	p.SetInt32(recTypeOffset, recTypeNormal)
	copy(rec[int(recTypeSize):], logrec)

	if err := logMgr.ensureAndWrite(rec); err != nil {
		return 0, fmt.Errorf("could not append logrec: %w", err)
	}
	return logMgr.latestLSN, nil
}

// フラグメントとして書き込む
func (logMgr *LogMgr) writeFragmentedRecord(logrec []byte) (int32, error) {
	totalLen := len(logrec)
	pos := 0
	first := true

	var firstRecLsn int32
	for pos < totalLen {
		chunk, err := logMgr.writeFragmentChunk(logrec, pos, totalLen, first)
		if err != nil {
			return 0, err
		}

		// 最初のフラグメントのLSNを取得
		if first {
			firstRecLsn = logMgr.latestLSN
		}

		pos += chunk
		first = false
	}

	return firstRecLsn, nil
}

// 1つのフラグメントチャンクを書き込む
func (logMgr *LogMgr) writeFragmentChunk(logrec []byte, pos int, totalLen int, first bool) (int, error) {
	chunkLimit := logMgr.calculateChunkLimit(first)
	if chunkLimit <= 0 {
		// ヘッダすら入らない場合は新しいブロックへ
		if err := logMgr.ensureNewBlock(); err != nil {
			return 0, fmt.Errorf("could not append fragmented logrec: %w", err)
		}
		// 新しいブロックで再試行
		return logMgr.writeFragmentChunk(logrec, pos, totalLen, first)
	}

	chunk := minInt(len(logrec)-pos, chunkLimit)
	isLast := pos+chunk == len(logrec)
	frag := logMgr.buildFragment(logrec[pos:pos+chunk], totalLen, first, isLast)

	// 無駄なブロック追加判定処理が発生しているが、汎用性を考えてこのようにしている
	if err := logMgr.ensureAndWrite(frag); err != nil {
		return 0, fmt.Errorf("could not append fragmented logrec: %w", err)
	}
	return chunk, nil
}

// フラグメントのバイト列を構築する
func (logMgr *LogMgr) buildFragment(chunk []byte, totalLen int, first bool, isLast bool) []byte {
	var headerLen int32
	if first {
		headerLen = fragFirstPayloadOffset
	} else {
		headerLen = fragPayloadOffset
	}

	frag := make([]byte, int(headerLen)+len(chunk))
	fp := file.NewLogPage(frag)
	fp.SetInt32(recTypeOffset, recTypeFragment)

	if first {
		fp.SetInt32(fragFlagsOffset, fragFirst)
		fp.SetInt32(fragFirstTotalLenOffset, int32(totalLen))
		copy(frag[fragFirstPayloadOffset:], chunk)
	} else {
		if isLast {
			fp.SetInt32(fragFlagsOffset, fragLast)
		} else {
			fp.SetInt32(fragFlagsOffset, fragCont)
		}
		copy(frag[fragPayloadOffset:], chunk)
	}
	return frag
}

// 現在のブロックで書き込めるチャンクの最大サイズを計算する
func (logMgr *LogMgr) calculateChunkLimit(first bool) int {
	boundary := logMgr.logpage.GetInt32(0)
	var headerLen int32
	if first {
		headerLen = fragFirstPayloadOffset
	} else {
		headerLen = fragPayloadOffset
	}
	return int(logMgr.fm.BlockSize() - (boundary + int32Size + headerLen))
}

// minInt は2つの整数の最小値を返す
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (logMgr *LogMgr) appendNewBlock() (*file.BlockId, error) {
	fileMgr, logfile, logpage := logMgr.fm, logMgr.logfile, logMgr.logpage
	blk, err := fileMgr.Append(logfile)
	if err != nil {
		return nil, fmt.Errorf("could not append new log block: %w", err)
	}
	logpage.SetInt32(0, int32Size)
	fileMgr.Write(blk, logpage)
	return blk, nil
}

func (logMgr *LogMgr) flush() {
	fileMgr, currentblk, logpage := logMgr.fm, logMgr.currentblk, logMgr.logpage
	fileMgr.Write(currentblk, logpage)
	logMgr.lastSavedLSN = logMgr.latestLSN
}

func (logMgr *LogMgr) canFit(bytesNeeded int) bool {
	boundary := logMgr.logpage.GetInt32(0)
	return boundary+int32(bytesNeeded) <= logMgr.fm.BlockSize()
}

func (logMgr *LogMgr) ensureNewBlock() error {
	logMgr.flush()
	blk, err := logMgr.appendNewBlock()
	if err != nil {
		return err
	}
	logMgr.currentblk = blk
	return nil
}

func (logMgr *LogMgr) ensureAndWrite(rec []byte) error {
	bytesNeeded := len(rec) + int(int32Size)
	if !logMgr.canFit(bytesNeeded) {
		if err := logMgr.ensureNewBlock(); err != nil {
			return err
		}
	}
	boundary := logMgr.logpage.GetInt32(0)
	logMgr.logpage.SetBytes(boundary, rec)

	recpos := boundary + int32(bytesNeeded)
	logMgr.logpage.SetInt32(0, recpos)

	blockSize := logMgr.fm.BlockSize()
	blkNumber := logMgr.currentblk.Number()
	logMgr.latestLSN = blockSize*blkNumber + boundary
	return nil
}

type LogIterator struct {
	fm         *file.FileMgr
	blk        *file.BlockId
	endBlk     *file.BlockId
	p          *file.Page
	currentpos int32
	boundary   int32
	currentLSN int32
}

func newLogIterator(fm *file.FileMgr, logfile string, startLsn, endBlkNum int32) *LogIterator {
	b := make([]byte, fm.BlockSize())
	p := file.NewLogPage(b)

	blockSize := fm.BlockSize()

	startBlkNum := startLsn / blockSize
	startOffset := startLsn % blockSize

	startBlk := file.NewBlockId(logfile, startBlkNum)
	endBlk := file.NewBlockId(logfile, endBlkNum)

	logIterator := &LogIterator{fm, startBlk, endBlk, p, 0, 0, startLsn}
	logIterator.moveToBlock(startBlk, startOffset)
	return logIterator
}

func (logIter *LogIterator) HasNext() bool {
	blk, currentpos, boundary := logIter.blk, logIter.currentpos, logIter.boundary

	if blk.Number() > logIter.endBlk.Number() {
		return false
	}

	if blk.Number() == logIter.endBlk.Number() && currentpos >= boundary {
		return false
	}

	return true
}

func (logIter *LogIterator) Next() (int32, []byte) {
	for {
		logIter.ensureCurrentBlock()

		rec := logIter.readPhysicalRecord()
		currentLSN := logIter.currentLSN
		logIter.nextToRecordPosition(len(rec))

		// フラグメントかどうか判定
		if !logIter.isFragment(rec) {
			// 非フラグメントのペイロード（共通ヘッダを剥がして返す）
			if len(rec) >= int(recTypeSize) {
				pr := file.NewLogPage(rec)
				if pr.GetInt32(recTypeOffset) == recTypeNormal {
					return currentLSN, rec[int(recTypeSize):]
				}
			}
			// 想定外（壊れている等）の場合は生のペイロードを返す
			return currentLSN, rec
		}

		flags := logIter.getFragmentFlags(rec)
		switch flags {
		case fragFirst:
			// FIRST から順方向にチェインを復元
			totalLen, ok := logIter.validateFragmentHeader(rec)
			if !ok {
				continue
			}
			if reconstructed, ok := logIter.reconstructFragmentChain(rec, totalLen); ok {
				return currentLSN, reconstructed
			}
			// 復元に失敗した場合は、この論理レコードをスキップして次の論理レコード探索を続ける
			continue
		case fragCont, fragLast:
			// CONT や FIRST が単独で出てきた場合は孤立フラグメントとみなしスキップ
			continue
		default:
			// 想定外のフラグ。非フラグメントとして扱う
			return -1, rec
		}
	}
}

// 物理レコード（1件）のペイロードを読み出す
func (logIter *LogIterator) readPhysicalRecord() []byte {
	raw := logIter.p.GetBytes(logIter.currentpos)
	// Page 内部バッファのエイリアスを避けるため、必ずディープコピーして返す
	rec := make([]byte, len(raw))
	copy(rec, raw)
	return rec
}

// 次のレコードの位置を計算する
func (logIter *LogIterator) nextToRecordPosition(recLen int) int32 {
	logIter.currentpos += int32Size + int32(recLen)
	logIter.currentLSN += int32Size + int32(recLen)
	return logIter.currentpos
}

// FIRST フラグメントから始まるフラグメント連鎖を順方向に復元する
// 戻り値: (復元された論理レコード, 成功したか)
func (logIter *LogIterator) reconstructFragmentChain(firstRec []byte, totalLen int32) ([]byte, bool) {
	buf := make([]byte, totalLen)
	bufLen := 0

	copy(buf[bufLen:], firstRec[fragFirstPayloadOffset:])
	bufLen += len(firstRec) - int(fragFirstPayloadOffset)

	for bufLen < int(totalLen) {
		logIter.ensureCurrentBlock()

		partRec := logIter.readPhysicalRecord()
		// フラグメントでなければ連鎖が途切れたとみなす
		if !logIter.isFragment(partRec) {
			return nil, false
		}
		// フラグメントの場合はレコードの位置を次に進める
		logIter.nextToRecordPosition(len(partRec))

		flags := logIter.getFragmentFlags(partRec)
		switch flags {
		case fragFirst:
			return nil, false
		case fragCont:
			copy(buf[bufLen:], partRec[fragPayloadOffset:])
			bufLen += len(partRec) - int(fragPayloadOffset)
		case fragLast:
			copy(buf[bufLen:], partRec[fragPayloadOffset:])
			bufLen += len(partRec) - int(fragPayloadOffset)
			if bufLen == int(totalLen) {
				return buf, true
			}
		}
	}

	// Last に到達せずに途中で指定の長さを超えてしまった場合は失敗とみなす
	return nil, false
}

func (logIter *LogIterator) moveToBlock(blk *file.BlockId, offset int32) {
	fileMgr, p := logIter.fm, logIter.p
	fileMgr.Read(blk, p)
	logIter.boundary = p.GetInt32(0)
	logIter.currentpos = offset
	logIter.currentLSN = blk.Number()*logIter.fm.BlockSize() + offset
}

// 現在位置がブロック境界に達している場合、次のブロックに移動する
func (logIter *LogIterator) ensureCurrentBlock() {
	if logIter.currentpos >= logIter.boundary && logIter.blk.Number() < logIter.endBlk.Number() {
		newBlk := file.NewBlockId(logIter.blk.FileName(), logIter.blk.Number()+1)
		logIter.blk = newBlk
		logIter.moveToBlock(newBlk, int32Size)
	}
}

// isFragment はレコードがフラグメントかどうかを判定する
func (logIter *LogIterator) isFragment(rec []byte) bool {
	if len(rec) < int(recTypeSize) {
		return false
	}
	pr := file.NewLogPage(rec)
	if pr.GetInt32(recTypeOffset) != recTypeFragment {
		return false
	}
	// flags領域を読むための最小長
	return len(rec) >= int(fragPayloadOffset)
}

// フラグメントレコードのフラグを取得する
func (logIter *LogIterator) getFragmentFlags(rec []byte) int32 {
	pr := file.NewLogPage(rec)
	return pr.GetInt32(fragFlagsOffset)
}

// FIRST フラグメントのヘッダを検証し、totalLen を返す
// 戻り値: (totalLen, 検証成功したか)
func (logIter *LogIterator) validateFragmentHeader(rec []byte) (int32, bool) {
	// recType(4) | flags(4) | totalLen(4)
	if len(rec) < int(recTypeSize+int32Size*2) {
		return 0, false
	}
	pr := file.NewLogPage(rec)
	totalLen := pr.GetInt32(fragFirstTotalLenOffset)
	if totalLen <= 0 {
		return 0, false
	}
	return totalLen, true
}

func (logMgr *LogMgr) ReadMasterLSN() (int32, error) {
	logMgr.mu.RLock()
	defer logMgr.mu.RUnlock()

	hdrBlk := file.NewBlockId(logMgr.logfile, 0)
	hdr := file.NewLogPage(make([]byte, logMgr.fm.BlockSize()))
	if err := logMgr.fm.Read(hdrBlk, hdr); err != nil {
		return 0, fmt.Errorf("could not read header block: %w", err)
	}
	if hdr.GetInt32(headerMagicOffset) != logHeaderMagic {
		return 0, fmt.Errorf("invalid log header magic")
	}
	return hdr.GetInt32(headerLastCheckpointLSNOffset), nil
}

func (logMgr *LogMgr) WriteMasterLSN(lsn int32) error {
	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()

	hdrBlk := file.NewBlockId(logMgr.logfile, 0)
	hdr := file.NewLogPage(make([]byte, logMgr.fm.BlockSize()))
	if err := logMgr.fm.Read(hdrBlk, hdr); err != nil {
		return fmt.Errorf("could not read header block: %w", err)
	}
	if hdr.GetInt32(headerMagicOffset) != logHeaderMagic {
		return fmt.Errorf("invalid log header magic")
	}
	if err := hdr.SetInt32(headerLastCheckpointLSNOffset, lsn); err != nil {
		return fmt.Errorf("could not set master LSN: %w", err)
	}
	if err := logMgr.fm.Write(hdrBlk, hdr); err != nil {
		return fmt.Errorf("could not write header block: %w", err)
	}
	return nil
}
