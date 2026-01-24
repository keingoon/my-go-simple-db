package recovery

import (
	"fmt"
	"sync"
)

const (
	running int32 = iota
	commited
	undo
)

type ActiveTrxTableEntry struct {
	txnum   int32
	status  int32
	lastLSN int32
}

func newActiveTrxEntry(txnum int32, status int32, lastLSN int32) ActiveTrxTableEntry {
	return ActiveTrxTableEntry{txnum, status, lastLSN}
}

func (atEty *ActiveTrxTableEntry) getTxnum() int32 {
	return atEty.txnum
}

func (atEty *ActiveTrxTableEntry) getStatus() int32 {
	return atEty.status
}

func (atEty *ActiveTrxTableEntry) getLastLSN() int32 {
	return atEty.lastLSN
}

type ActiveTrxTable struct {
	table map[int32]ActiveTrxTableEntry
	mu    sync.RWMutex
}

func newActiveTrxTable() *ActiveTrxTable {
	return &ActiveTrxTable{
		table: make(map[int32]ActiveTrxTableEntry),
	}
}

func (at *ActiveTrxTable) getTable() []ActiveTrxTableEntry {
	entries := make([]ActiveTrxTableEntry, 0)
	for _, entry := range at.table {
		entries = append(entries, entry)
	}
	return entries
}

func (at *ActiveTrxTable) addActiveTrx(txnum int32, status int32, lastLSN int32) {
	at.mu.Lock()
	defer at.mu.Unlock()
	at.table[txnum] = newActiveTrxEntry(txnum, status, lastLSN)
}

func (at *ActiveTrxTable) removeActiveTrx(txnum int32) {
	at.mu.Lock()
	defer at.mu.Unlock()
	delete(at.table, txnum)
}

func (at *ActiveTrxTable) getActiveTrx(txnum int32) (*ActiveTrxTableEntry, bool) {
	at.mu.RLock()
	defer at.mu.RUnlock()
	if entry, ok := at.table[txnum]; ok {
		return &entry, true
	}
	return nil, false
}

func (at *ActiveTrxTable) getActiveTrxsByStatus(status int32) ([]*ActiveTrxTableEntry, bool) {
	at.mu.RLock()
	defer at.mu.RUnlock()
	entries := make([]*ActiveTrxTableEntry, 0)
	for _, entry := range at.table {
		if entry.status == status {
			entries = append(entries, &entry)
		}
	}
	return entries, len(entries) > 0
}

func (at *ActiveTrxTable) getLastLSN(txnum int32) (int32, error) {
	entry, ok := at.getActiveTrx(txnum)
	if !ok {
		return -1, fmt.Errorf("transaction %d not found", txnum)
	}
	return entry.lastLSN, nil
}

func (at *ActiveTrxTable) getSnapshotActiveTrxTable() map[int32]ActiveTrxTableEntry {
	at.mu.RLock()
	defer at.mu.RUnlock()
	snapshot := make(map[int32]ActiveTrxTableEntry, len(at.table))
	for k, v := range at.table {
		snapshot[k] = v
	}
	return snapshot
}
