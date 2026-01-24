package buffer

import (
	"math"
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

func NewDirtyPageTable() *DirtyPageTable {
	return &DirtyPageTable{table: make(map[file.BlockId]DirtyPageEntry)}
}

func (dpt *DirtyPageTable) MarkDirtyPageTable(blk *file.BlockId, recLSN int32) {
	key := *blk
	dpt.mu.Lock()
	defer dpt.mu.Unlock()
	if _, ok := dpt.table[key]; !ok {
		dpt.table[key] = NewDirtyPageEntry(blk, recLSN)
	}
}

func (dpt *DirtyPageTable) CleanPageTable(blk *file.BlockId) {
	key := *blk
	dpt.mu.Lock()
	defer dpt.mu.Unlock()
	delete(dpt.table, key)
}

func (dpt *DirtyPageTable) GetDirtyPage(blk *file.BlockId) (DirtyPageEntry, bool) {
	key := *blk
	dpt.mu.RLock()
	defer dpt.mu.RUnlock()
	if entry, ok := dpt.table[key]; ok {
		return entry, true
	}
	return DirtyPageEntry{}, false
}

func (dpt *DirtyPageTable) GetRecLSN(blk *file.BlockId) (int32, bool) {
	entry, ok := dpt.GetDirtyPage(blk)
	if !ok {
		return -1, false
	}
	return entry.recLSN, true
}

func (dpt *DirtyPageTable) GetMinRecLSN() (int32, bool) {
	dpt.mu.RLock()
	defer dpt.mu.RUnlock()
	minRecLSN := int32(math.MaxInt32)
	for _, entry := range dpt.table {
		if entry.recLSN < minRecLSN {
			minRecLSN = entry.recLSN
		}
	}
	if minRecLSN == int32(math.MaxInt32) {
		return -1, false
	}
	return minRecLSN, true
}

func (dpt *DirtyPageTable) GetSnapshotDirtyPageTable() map[file.BlockId]DirtyPageEntry {
	dpt.mu.RLock()
	defer dpt.mu.RUnlock()
	snapshot := make(map[file.BlockId]DirtyPageEntry, len(dpt.table))
	for k, v := range dpt.table {
		snapshot[k] = v
	}
	return snapshot
}
