package buffer

import (
	"sync"

	"github.com/keingoon/simpledb/internal/file"
)

type DirtyPageTable struct {
	table map[file.BlockId]DirtyPageEntry
	mu    sync.RWMutex
}

type DirtyPageEntry struct {
	blk    *file.BlockId
	recLSN int32
}

func NewDirtyPageEntry(blk *file.BlockId, recLSN int32) DirtyPageEntry {
	return DirtyPageEntry{blk, recLSN}
}

func (dptEty *DirtyPageEntry) GetRecLSN() int32 {
	return dptEty.recLSN
}

func newDirtyPageTable() *DirtyPageTable {
	return &DirtyPageTable{table: make(map[file.BlockId]DirtyPageEntry)}
}

var dirtyPageTableInstance = newDirtyPageTable()

func MarkDirtyPageTable(blk *file.BlockId, recLSN int32) {
	key := *blk
	dirtyPageTableInstance.mu.Lock()
	defer dirtyPageTableInstance.mu.Unlock()
	if _, ok := dirtyPageTableInstance.table[key]; !ok {
		dirtyPageTableInstance.table[key] = NewDirtyPageEntry(blk, recLSN)
	}
}

func CleanPageTable(blk *file.BlockId) {
	key := *blk
	dirtyPageTableInstance.mu.Lock()
	delete(dirtyPageTableInstance.table, key)
	dirtyPageTableInstance.mu.Unlock()
}

func GetSnapshotDirtyPageTable() map[file.BlockId]DirtyPageEntry {
	dirtyPageTableInstance.mu.RLock()
	defer dirtyPageTableInstance.mu.RUnlock()
	snapshot := make(map[file.BlockId]DirtyPageEntry, len(dirtyPageTableInstance.table))
	for k, v := range dirtyPageTableInstance.table {
		snapshot[k] = v
	}
	return snapshot
}
