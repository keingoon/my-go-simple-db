package file

import (
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type BlockId struct {
	filename string
	blknum   int32
}

func NewBlockId(filename string, blknum int32) *BlockId {
	return &BlockId{filename: filename, blknum: blknum}
}

func (b *BlockId) BlockId(filename string, blknum int32) {
	b.filename = filename
	b.blknum = blknum
}

func (b *BlockId) FileName() string {
	return b.filename
}

func (b *BlockId) Number() int32 {
	return b.blknum
}

func (b *BlockId) Equals(blk *BlockId) bool {
	return b.filename == blk.filename && b.blknum == blk.blknum
}

func (b *BlockId) ToString() string {
	return fmt.Sprintf("[file %s, block %d", b.filename, b.blknum)
}

func (b *BlockId) HashCode() uint64 {
	hash := fnv.New64a()
	data := []byte(b.ToString())
	hash.Write(data)

	return hash.Sum64()
}

const (
	int32Size         = 4
	int16Size         = 2
	boolSize          = 1
	dateSize          = 8  // 2038年問題があるため64bitで保存したい。unixtimeで秒まで保存。
	maxStrBytesLength = 1  // US ASCII のみを使用するので1文字1バイト想定
	pageHeaderSize    = 64 // ページヘッダーのサイズ
	pageTypeOffset    = 0
	pageLSNOffset     = int32Size
)

const (
	heap int32 = iota
	btreeInternal
	btreeLeaf
)

type Page struct {
	bb         []byte
	headerSize int32
	pageType   int32
}

func NewPage(blocksize int32) *Page {
	return &Page{bb: make([]byte, blocksize), headerSize: pageHeaderSize, pageType: heap}
}

func NewLogPage(b []byte) *Page {
	// Log pages don't have a page header; offsets are absolute within the given slice.
	return &Page{bb: b, headerSize: 0, pageType: heap}
}

func (p *Page) GetPageType() int32 {
	b := p.bb[pageTypeOffset : pageTypeOffset+int32Size]
	return int32(b[0]) | int32(b[1])<<8 | int32(b[2])<<16 | int32(b[3])<<24
}

func (p *Page) GetPageLSN() int32 {
	b := p.bb[pageLSNOffset : pageLSNOffset+int32Size]
	return int32(b[0]) | int32(b[1])<<8 | int32(b[2])<<16 | int32(b[3])<<24
}

func (p *Page) SetPageLSN(pageLSN int32) error {
	p.bb[pageLSNOffset] = byte(pageLSN)
	p.bb[pageLSNOffset+1] = byte(pageLSN >> 8)
	p.bb[pageLSNOffset+2] = byte(pageLSN >> 16)
	p.bb[pageLSNOffset+3] = byte(pageLSN >> 24)
	return nil
}

func (p *Page) GetInt32(offset int32) int32 {
	dataOffset := p.headerSize + offset
	b := p.bb[dataOffset : dataOffset+int32Size]
	return int32(b[0]) | int32(b[1])<<8 | int32(b[2])<<16 | int32(b[3])<<24
}

func (p *Page) SetInt32(offset int32, val int32) error {
	dataOffset := p.headerSize + offset
	if offset < 0 || dataOffset+int32Size > int32(len(p.bb)) {
		return fmt.Errorf("set int32 offset out bound from 0 to %d", int32(len(p.bb))-1-p.headerSize)
	}
	p.bb[dataOffset] = byte(val)
	p.bb[dataOffset+1] = byte(val >> 8)
	p.bb[dataOffset+2] = byte(val >> 16)
	p.bb[dataOffset+3] = byte(val >> 24)
	return nil
}

func (p *Page) GetBytes(offset int32) []byte {
	n := p.GetInt32(offset)
	dataOffset := p.headerSize + offset
	return p.bb[dataOffset+int32Size : dataOffset+int32Size+n]
}

func (p *Page) SetBytes(offset int32, b []byte) error {
	dataOffset := p.headerSize + offset
	if offset < 0 || dataOffset+int32Size+int32(len(b)) > int32(len(p.bb)) {
		return fmt.Errorf("set bytes offset out bound from 0 to %d", int32(len(p.bb))-1-p.headerSize)
	}
	if err := p.SetInt32(offset, int32(len(b))); err != nil {
		return fmt.Errorf("set bytes err: %w", err)
	}
	copy(p.bb[dataOffset+int32Size:dataOffset+int32Size+int32(len(b))], b)
	return nil
}

func (p *Page) GetStr(offset int32) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetStr(offset int32, s string) error {
	if err := p.SetBytes(offset, []byte(s)); err != nil {
		return fmt.Errorf("set str err: %w", err)
	}
	return nil
}

func (p *Page) GetInt16(offset int32) int16 {
	dataOffset := p.headerSize + offset
	b := p.bb[dataOffset : dataOffset+int16Size]
	return int16(b[0]) | int16(b[1])<<8
}

func (p *Page) SetInt16(offset int32, val int16) error {
	dataOffset := p.headerSize + offset
	if offset < 0 || dataOffset+int16Size > int32(len(p.bb)) {
		return fmt.Errorf("set int16 offset out bound from 0 to %d", int32(len(p.bb))-1-p.headerSize)
	}
	p.bb[dataOffset] = byte(val)
	p.bb[dataOffset+1] = byte(val >> 8)
	return nil
}

func (p *Page) GetBool(offset int32) bool {
	dataOffset := p.headerSize + offset
	return p.bb[dataOffset] == byte(1)
}

func (p *Page) SetBool(offset int32, val bool) error {
	dataOffset := p.headerSize + offset
	if offset < 0 || dataOffset+boolSize > int32(len(p.bb)) {
		return fmt.Errorf("set bool offset out bound from 0 to %d", int32(len(p.bb))-1-p.headerSize)
	}
	if val {
		p.bb[dataOffset] = byte(1)
	} else {
		p.bb[dataOffset] = byte(0)
	}

	return nil
}

func (p *Page) GetDate(offset int32) time.Time {
	dataOffset := p.headerSize + offset
	b := p.bb[dataOffset : dataOffset+dateSize]
	unix := int64(b[0]) | int64(b[1])<<8 | int64(b[2])<<16 | int64(b[3])<<24 | int64(b[4])<<32 | int64(b[5])<<40 | int64(b[6])<<48 | int64(b[7])<<56
	// UTCとして時刻は扱う
	return time.Unix(unix, 0).In(time.UTC)
}

func (p *Page) SetDate(offset int32, t time.Time) error {
	dataOffset := p.headerSize + offset
	if offset < 0 || dataOffset+dateSize > int32(len(p.bb)) {
		return fmt.Errorf("set date offset out bound from 0 to %d", int32(len(p.bb))-1-p.headerSize)
	}
	unix := t.Unix()
	p.bb[dataOffset] = byte(unix)
	p.bb[dataOffset+1] = byte(unix >> 8)
	p.bb[dataOffset+2] = byte(unix >> 16)
	p.bb[dataOffset+3] = byte(unix >> 24)
	p.bb[dataOffset+4] = byte(unix >> 32)
	p.bb[dataOffset+5] = byte(unix >> 40)
	p.bb[dataOffset+6] = byte(unix >> 48)
	p.bb[dataOffset+7] = byte(unix >> 56)
	return nil
}

func MaxLength(len int) int {
	return int32Size + (len * maxStrBytesLength)
}

const tmpFileNamePrefix = "temp"

type FileMgr struct {
	dbDirectoryPath string
	blocksize       int32
	isNew           bool
	openFiles       map[string]*os.File
	mu              sync.Mutex
	rwCnt           *RwCnt
	kmu             keyMutex
}

type RwCnt struct {
	r  int
	w  int
	mu sync.Mutex
}

func NewFileMgr(dbDirectoryPath string, blocksize int32) (*FileMgr, error) {
	var isNew = false
	fi, err := os.Stat(dbDirectoryPath)

	if err != nil {
		if os.IsNotExist(err) {
			isNew = true
		} else {
			return nil, fmt.Errorf("other exception of directory not exists %s: %w", dbDirectoryPath, err)
		}
		if err := os.Mkdir(dbDirectoryPath, 0755); err != nil {
			return nil, fmt.Errorf("could not create directory %s: %w", dbDirectoryPath, err)
		}
	} else {
		if !fi.IsDir() {
			return nil, fmt.Errorf("path %s is not directory: %w", dbDirectoryPath, err)
		}
		fs, err := os.ReadDir(dbDirectoryPath)
		if err != nil {
			return nil, fmt.Errorf("could not find directory %s: %w", dbDirectoryPath, err)
		}
		if len(fs) == 0 {
			isNew = true
		}
		for _, f := range fs {
			fname := f.Name()
			if strings.HasPrefix(fname, tmpFileNamePrefix) {
				os.Remove(path.Join(dbDirectoryPath, fname))
			}
		}
	}

	return &FileMgr{dbDirectoryPath: dbDirectoryPath, blocksize: blocksize, isNew: isNew, openFiles: map[string]*os.File{}, rwCnt: &RwCnt{}, kmu: keyMutex{}}, nil
}

func (mgr *FileMgr) Read(blk *BlockId, page *Page) error {
	filename := blk.FileName()
	mgr.kmu.Lock(filename)
	defer mgr.kmu.Unlock(filename)

	f, err := mgr.getFile(filename)
	if err != nil {
		return err
	}

	if _, err = f.ReadAt(page.bb, int64(blk.Number()*mgr.blocksize)); err != nil {
		return fmt.Errorf("could not read block to page %d, %w", blk.Number()*mgr.blocksize, err)
	}

	const readFlag = "r"
	mgr.incRwCnt(readFlag)

	return nil
}

func (mgr *FileMgr) Write(blk *BlockId, page *Page) error {
	filename := blk.FileName()
	mgr.kmu.Lock(filename)
	defer mgr.kmu.Unlock(filename)

	f, err := mgr.getFile(filename)
	if err != nil {
		return err
	}

	if _, err = f.WriteAt(page.bb, int64(blk.Number()*mgr.blocksize)); err != nil {
		return fmt.Errorf("could not write page to block %d, %w", blk.Number()*mgr.blocksize, err)
	}

	const readFlag = "w"
	mgr.incRwCnt(readFlag)

	return nil
}

func (mgr *FileMgr) Append(filename string) (*BlockId, error) {
	mgr.kmu.Lock(filename)
	defer mgr.kmu.Unlock(filename)

	f, err := mgr.getFile(filename)
	if err != nil {
		return nil, err
	}

	newblknum, err := mgr.LengthFromFileObj(f)
	if err != nil {
		return nil, err
	}
	blk := NewBlockId(filename, int32(newblknum))
	b := make([]byte, mgr.blocksize)

	if _, err = f.WriteAt(b, int64(blk.Number()*mgr.blocksize)); err != nil {
		return nil, fmt.Errorf("could not append block %d, %w", blk.Number()*mgr.blocksize, err)
	}
	return blk, nil
}

func (mgr *FileMgr) Length(filename string) (int32, error) {
	mgr.kmu.Lock(filename)
	defer mgr.kmu.Unlock(filename)

	f, err := mgr.getFile(filename)
	if err != nil {
		return 0, err
	}

	return mgr.LengthFromFileObj(f)
}

func (mgr *FileMgr) LengthFromFileObj(f *os.File) (int32, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("cound not find file %s, %w", f.Name(), err)
	}
	return int32(fi.Size()) / mgr.blocksize, err
}

func (mgr *FileMgr) IsNew() bool {
	return mgr.isNew
}

func (mgr *FileMgr) BlockSize() int32 {
	return mgr.blocksize
}

func (mgr *FileMgr) getFile(filename string) (*os.File, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	f, found := mgr.openFiles[filename]
	if !found {
		f, err := os.OpenFile(path.Join(mgr.dbDirectoryPath, filename), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("could not get file %s: %w", filename, err)
		}
		mgr.openFiles[filename] = f
		return f, nil
	}
	return f, nil
}

func (mgr *FileMgr) incRwCnt(rw string) {
	mgr.rwCnt.mu.Lock()
	defer mgr.rwCnt.mu.Unlock()
	switch rw {
	case "w":
		mgr.rwCnt.w += 1
	case "r":
		mgr.rwCnt.r += 1
	}
}

type keyMutex struct {
	mu  sync.Mutex
	kmu map[string]*sync.Mutex
}

func (kmu *keyMutex) mutexFor(key string) *sync.Mutex {
	kmu.mu.Lock()
	defer kmu.mu.Unlock()

	if kmu.kmu == nil {
		kmu.kmu = make(map[string]*sync.Mutex)
	}
	mu, ok := kmu.kmu[key]
	if ok {
		return mu
	}
	mu = &sync.Mutex{}
	kmu.kmu[key] = mu
	return mu
}

func (kmu *keyMutex) Lock(key string) {
	mu := kmu.mutexFor(key)
	mu.Lock()
}

func (kmu *keyMutex) Unlock(key string) {
	mu := kmu.mutexFor(key)
	mu.Unlock()
}
