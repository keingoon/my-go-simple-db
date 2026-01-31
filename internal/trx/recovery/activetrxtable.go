package recovery

import (
	"fmt"
	"sync"

	"github.com/keingoon/simpledb/internal/trx/recovery/logrecord"
)

const (
	running int32 = iota
	commited
	aborting
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

func (at *ActiveTrxTable) setLastLSN(txnum int32, lastLSN int32) {
	at.mu.Lock()
	defer at.mu.Unlock()
	e, ok := at.table[txnum]
	if !ok {
		// トランザクションが存在しない場合は作成
		at.table[txnum] = newActiveTrxEntry(txnum, running, lastLSN)
		return
	}
	e.lastLSN = lastLSN
	at.table[txnum] = e
}

func (at *ActiveTrxTable) setTrx(txnum int32, status int32, lastLSN int32) {
	at.mu.Lock()
	defer at.mu.Unlock()
	e, ok := at.table[txnum]
	if !ok {
		// トランザクションが存在しない場合は作成
		at.table[txnum] = newActiveTrxEntry(txnum, status, lastLSN)
		return
	}
	e.lastLSN = lastLSN
	e.status = status
	at.table[txnum] = e
}

func (at *ActiveTrxTable) removeTrx(txnum int32) {
	at.mu.Lock()
	defer at.mu.Unlock()
	delete(at.table, txnum)
}

func (at *ActiveTrxTable) getTrx(txnum int32) (ActiveTrxTableEntry, bool) {
	at.mu.RLock()
	defer at.mu.RUnlock()
	if entry, ok := at.table[txnum]; ok {
		return entry, true
	}
	return ActiveTrxTableEntry{}, false
}

func (at *ActiveTrxTable) getTrxsByStatus(status int32) ([]ActiveTrxTableEntry, bool) {
	at.mu.RLock()
	defer at.mu.RUnlock()
	entries := make([]ActiveTrxTableEntry, 0)
	for _, entry := range at.table {
		if entry.status == status {
			entries = append(entries, entry)
}
	}
	return entries, len(entries) > 0
}

func (at *ActiveTrxTable) getLastLSN(txnum int32) (int32, error) {
	entry, ok := at.getTrx(txnum)
	if !ok {
		return -1, fmt.Errorf("transaction %d not found", txnum)
	}
	return entry.lastLSN, nil
}

func (at *ActiveTrxTable) getSnapshotTrxTable() map[int32]logrecord.CheckpointTxnEntry {
	at.mu.RLock()
	defer at.mu.RUnlock()

	snapshot := make(map[int32]logrecord.CheckpointTxnEntry, len(at.table))
	for txnum, v := range at.table {
		snapshot[txnum] = logrecord.CheckpointTxnEntry{
			Status:  v.status,
			LastLSN: v.lastLSN,
		}
	}
	return snapshot
}
