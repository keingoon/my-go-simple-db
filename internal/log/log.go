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

// 1つの物理レコード内に格納するフラグメントのレイアウト:
// FIRST: [fragMagic:int32][flags:int32=1][totalLen:int32] + データ断片
// CONT:  [fragMagic:int32][flags:int32=2]                 + データ断片
// LAST:  [fragMagic:int32][flags:int32=3]                 + データ断片
const (
	fragMagic     int32 = 0x47415246 // "FRAG"（リトルエンディアン）
	fragFlagFirst int32 = 1
	fragFlagCont  int32 = 2
	fragFlagLast  int32 = 3
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
		hdr := file.NewPage(fm.BlockSize())
		if err := hdr.SetInt32(headerMagicOffset, logHeaderMagic); err != nil {
			return nil, fmt.Errorf("could not write header magic: %w", err)
		}
		if err := hdr.SetInt16(headerVersionOffset, logHeaderVersion); err != nil {
			return nil, fmt.Errorf("could not write header version: %w", err)
		}
		if err := hdr.SetInt32(headerPageSizeOffset, fm.BlockSize()); err != nil {
			return nil, fmt.Errorf("could not write header page size: %w", err)
		}
		if err := hdr.SetInt32(headerLastCheckpointLSNOffset, 0); err != nil {
			return nil, fmt.Errorf("could not write header last checkpoint lsn: %w", err)
		}
		if err := fm.Write(hdrBlk, hdr); err != nil {
			return nil, fmt.Errorf("could not write header block: %w", err)
		}
	} else {
		// ログヘッダーブロックの値チェックバリデーション
		hdrBlk := file.NewBlockId(logfile, 0)
		hdr := file.NewPage(fm.BlockSize())
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

func (logMgr *LogMgr) Iterater() *LogIterator {
	logMgr.flush()
	return NewLogIterator(logMgr.fm, logMgr.currentblk)
}

func (logMgr *LogMgr) Append(logrec []byte) (int32, error) {
	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()

	if logMgr.canFitAsSingle(logrec) {
		return logMgr.writeSingleRecord(logrec)
	}
	return logMgr.writeFragmented(logrec)
}

// canFitAsSingle はレコードが単一ブロックに収まるか判定する
func (logMgr *LogMgr) canFitAsSingle(logrec []byte) bool {
	maxBytesInEmptyBlock := int(logMgr.fm.BlockSize()) - 2*int(int32Size)
	return len(logrec) <= maxBytesInEmptyBlock
}

// writeSingleRecord は単一レコードとして書き込む
func (logMgr *LogMgr) writeSingleRecord(logrec []byte) (int32, error) {
	if err := logMgr.ensureAndWrite(logrec); err != nil {
		return 0, fmt.Errorf("could not append logrec: %w", err)
	}
	logMgr.latestLSN += 1
	return logMgr.latestLSN, nil
}

// writeFragmented はフラグメントとして書き込む
func (logMgr *LogMgr) writeFragmented(logrec []byte) (int32, error) {
	totalLen := len(logrec)
	pos := 0
	first := true

	for pos < totalLen {
		chunk, err := logMgr.writeFragmentChunk(logrec, pos, totalLen, first)
		if err != nil {
			return 0, err
		}
		pos += chunk
		first = false
	}

	logMgr.latestLSN += 1
	return logMgr.latestLSN, nil
}

// writeFragmentChunk は1つのフラグメントチャンクを書き込む
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

// buildFragment はフラグメントのバイト列を構築する
func (logMgr *LogMgr) buildFragment(chunk []byte, totalLen int, first bool, isLast bool) []byte {
	var headerLen int
	if first {
		headerLen = int32Size * 3 // magic + flags + totalLen
	} else {
		headerLen = int32Size * 2 // magic + flags
	}

	frag := make([]byte, headerLen+len(chunk))
	fp := file.NewLogPage(frag)
	fp.SetInt32(0, fragMagic)

	if first {
		fp.SetInt32(int32Size, fragFlagFirst)
		fp.SetInt32(int32Size*2, int32(totalLen))
		copy(frag[int32Size*3:], chunk)
	} else {
		if isLast {
			fp.SetInt32(int32Size, fragFlagLast)
		} else {
			fp.SetInt32(int32Size, fragFlagCont)
		}
		copy(frag[int32Size*2:], chunk)
	}
	return frag
}

// calculateChunkLimit は現在のブロックで書き込めるチャンクの最大サイズを計算する
func (logMgr *LogMgr) calculateChunkLimit(first bool) int {
	boundary := logMgr.logpage.GetInt32(0)
	var headerLen int
	if first {
		headerLen = int32Size * 3
	} else {
		headerLen = int32Size * 2
	}
	return int(boundary) - 2*int(int32Size) - headerLen
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
	logpage.SetInt32(0, fileMgr.BlockSize())
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
	return boundary-int32(bytesNeeded) >= int32Size
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
	recpos := boundary - int32(bytesNeeded)
	logMgr.logpage.SetBytes(recpos, rec)
	logMgr.logpage.SetInt32(0, recpos)
	return nil
}

type LogIterator struct {
	fm         *file.FileMgr
	blk        *file.BlockId
	p          *file.Page
	currentpos int32
	boundary   int32
}

func NewLogIterator(fm *file.FileMgr, blk *file.BlockId) *LogIterator {
	b := make([]byte, fm.BlockSize())
	p := file.NewLogPage(b)

	logIterator := &LogIterator{fm, blk, p, 0, 0}
	logIterator.moveToBlock(blk)
	return logIterator
}

func (logIter *LogIterator) HasNext() bool {
	fileMgr, blk, currentpos := logIter.fm, logIter.blk, logIter.currentpos
	// ログのheaderブロックを除いてnext判定
	return currentpos < fileMgr.BlockSize() || blk.Number() > 1
}

func (logIter *LogIterator) Next() []byte {
	for logIter.HasNext() {
		logIter.ensureCurrentBlock()

		rec := logIter.readPhysicalRecord()

		// フラグメントかどうか判定
		if !logIter.isFragment(rec) {
			// 非フラグメントのペイロード
			return rec
		}

		flags := logIter.getFragmentFlags(rec)
		switch flags {
		case fragFlagLast:
			// LAST を起点にフラグメント連鎖を逆向きに復元する
			if reconstructed, ok := logIter.reconstructFragmentChain(rec); ok {
				return reconstructed
			}
			// 復元に失敗した場合は、この論理レコードをスキップして次の論理レコード探索を続ける
			continue
		case fragFlagCont, fragFlagFirst:
			// CONT や FIRST が単独で出てきた場合は孤立フラグメントとみなしスキップ
			continue
		default:
			// 想定外のフラグ。非フラグメントとして扱う
			return rec
		}
	}

	// これ以上読める論理レコードが無い場合
	return nil
}

// readPhysicalRecord は物理レコード（1件）のペイロードを読み出す
// 戻り値は Page 内部バッファとは独立したスライスであり、
// 呼び出し側で保持しても、その後の moveToBlock による内容破壊は起こらない。
func (logIter *LogIterator) readPhysicalRecord() []byte {
	raw := logIter.p.GetBytes(logIter.currentpos)
	logIter.currentpos += int32Size + int32(len(raw))

	// Page 内部バッファのエイリアスを避けるため、必ずディープコピーして返す
	rec := make([]byte, len(raw))
	copy(rec, raw)
	return rec
}

// reconstructFragmentChain は LAST フラグメントから始まるフラグメント連鎖を逆向きに復元する
// 戻り値: (復元された論理レコード, 成功したか)
func (logIter *LogIterator) reconstructFragmentChain(lastRec []byte) ([]byte, bool) {
	// LAST から始めて、前方に向かって CONT... → FIRST を探索する
	frags := [][]byte{lastRec}

	for logIter.HasNext() {
		logIter.ensureCurrentBlock()

		part := logIter.readPhysicalRecord()
		// フラグメントでなければ連鎖が途切れたとみなす
		if !logIter.isFragment(part) {
			return nil, false
		}

		flags := logIter.getFragmentFlags(part)
		switch flags {
		case fragFlagCont:
			// CONT は連鎖の一部として追加
			frags = append(frags, part)
		case fragFlagFirst:
			// FIRST に到達したら totalLen を取得して復元
			totalLen, ok := logIter.validateFragmentHeader(part)
			if !ok {
				return nil, false
			}
			frags = append(frags, part) // FIRST も含める

			buf := make([]byte, totalLen)
			sum := 0

			// frags は [LAST, CONT..., FIRST] の逆順なので、逆向きにコピーしていく
			for i := len(frags) - 1; i >= 0; i-- {
				fragment := frags[i]
				flag := logIter.getFragmentFlags(fragment)

				var dataOff int32
				if flag == fragFlagFirst {
					dataOff = int32Size * 3 // magic + flags + totalLen
				} else {
					dataOff = int32Size * 2 // magic + flags
				}

				dataLen := len(fragment) - int(dataOff)
				if sum+dataLen > int(totalLen) {
					// はみ出し（形式不正）
					return nil, false
				}
				copy(buf[sum:], fragment[dataOff:])
				sum += dataLen
			}

			if sum != int(totalLen) {
				// 未完成
				return nil, false
			}
			return buf, true
		default:
			// LAST が二度出てきた、または未知のフラグが出た場合は不正
			return nil, false
		}
	}

	// FIRST に到達しないままログが終わった場合は未完成とみなす
	return nil, false
}

func (logIter *LogIterator) moveToBlock(blk *file.BlockId) {
	fileMgr, p := logIter.fm, logIter.p
	fileMgr.Read(blk, p)
	logIter.boundary = p.GetInt32(0)
	logIter.currentpos = logIter.boundary
}

// ensureCurrentBlock は現在位置がブロック境界に達している場合、前のブロックに移動する
func (logIter *LogIterator) ensureCurrentBlock() {
	if logIter.currentpos == logIter.fm.BlockSize() && logIter.blk.Number() > 1 {
		newBlk := file.NewBlockId(logIter.blk.FileName(), logIter.blk.Number()-1)
		logIter.blk = newBlk
		logIter.moveToBlock(newBlk)
	}
}

// isFragment はレコードがフラグメントかどうかを判定する
func (logIter *LogIterator) isFragment(rec []byte) bool {
	if len(rec) < int32Size*2 {
		return false
	}
	pr := file.NewLogPage(rec)
	return pr.GetInt32(0) == fragMagic
}

// getFragmentFlags はフラグメントレコードのフラグを取得する
func (logIter *LogIterator) getFragmentFlags(rec []byte) int32 {
	pr := file.NewLogPage(rec)
	return pr.GetInt32(int32Size)
}

// validateFragmentHeader は FIRST フラグメントのヘッダを検証し、totalLen を返す
// 戻り値: (totalLen, 検証成功したか)
func (logIter *LogIterator) validateFragmentHeader(rec []byte) (int32, bool) {
	if len(rec) < int32Size*3 {
		return 0, false
	}
	pr := file.NewLogPage(rec)
	totalLen := pr.GetInt32(int32Size * 2)
	if totalLen <= 0 {
		return 0, false
	}
	return totalLen, true
}

func (logMgr *LogMgr) ReadMasterLSN() (int32, error) {
	logMgr.mu.RLock()
	defer logMgr.mu.RUnlock()

	hdrBlk := file.NewBlockId(logMgr.logfile, 0)
	hdr := file.NewPage(logMgr.fm.BlockSize())
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
	hdr := file.NewPage(logMgr.fm.BlockSize())
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
