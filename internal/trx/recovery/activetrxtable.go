package recovery

import "sync"

const (
	running int32 = iota
	committed
	undo
)

type activeTrxTableEntry struct {
	txnum   int32
	status  int32
	lastLSN int32
}

func newActiveTrxEntry(txnum int32, status int32, lastLSN int32) activeTrxTableEntry {
	return activeTrxTableEntry{txnum, status, lastLSN}
}

type activeTrxTable struct {
	table map[int32]activeTrxTableEntry
	mu    sync.RWMutex
}

func newActiveTrxTable() *activeTrxTable {
	return &activeTrxTable{
		table: make(map[int32]activeTrxTableEntry),
	}
}

var activeTrxTableInstance = newActiveTrxTable()

func addActiveTrxFromTable(txnum int32, status int32, lastLSN int32) {
	activeTrxTableInstance.mu.Lock()
	defer activeTrxTableInstance.mu.Unlock()
	activeTrxTableInstance.table[txnum] = newActiveTrxEntry(txnum, status, lastLSN)
}

func removeActiveTrxFromTable(txnum int32) {
	activeTrxTableInstance.mu.Lock()
	defer activeTrxTableInstance.mu.Unlock()
	delete(activeTrxTableInstance.table, txnum)
}

func getSnapshotActiveTrxTable() map[int32]activeTrxTableEntry {
	activeTrxTableInstance.mu.RLock()
	defer activeTrxTableInstance.mu.RUnlock()
	snapshot := make(map[int32]activeTrxTableEntry, len(activeTrxTableInstance.table))
	for k, v := range activeTrxTableInstance.table {
		snapshot[k] = v
	}
	return snapshot
}
