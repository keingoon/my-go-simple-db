package logrecord

import (
	"fmt"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type CheckpointEndRecord struct {
	lsn      int32
	beginLSN int32
	att      map[int32]CheckpointTxnEntry
	dpt      map[file.BlockId]buffer.DirtyPageEntry
}

// CheckpointTxnEntry is a serialized snapshot entry for ATT in CHECKPOINT-END.
// This type intentionally lives in logrecord package to avoid depending on recovery's in-memory ATT types.
type CheckpointTxnEntry struct {
	Status  int32
	LastLSN int32
}

func NewCheckpointEndRecord(p *file.Page, lsn int32) *CheckpointEndRecord {
	beginPos := int32Size
	beginLSN := p.GetInt32(int32(beginPos))

	att := make(map[int32]CheckpointTxnEntry)
	attCntPos := beginPos + int32Size
	attCnt := p.GetInt32(int32(attCntPos))
	attEntryPos := attCntPos + int32Size
	for i := 0; i < int(attCnt); i++ {
		tPos := attEntryPos
		txnum := p.GetInt32(int32(tPos))
		statusPos := tPos + int32Size
		status := p.GetInt32(int32(statusPos))
		lastLSNPos := statusPos + int32Size
		lastLSN := p.GetInt32(int32(lastLSNPos))
		att[txnum] = CheckpointTxnEntry{
			Status:  status,
			LastLSN: int32(lastLSN),
		}
		attEntryPos = lastLSNPos + int32Size
	}

	dpt := make(map[file.BlockId]buffer.DirtyPageEntry)
	dptCntPos := attEntryPos
	dptCnt := p.GetInt32(int32(dptCntPos))
	dptEntryPos := dptCntPos + int32Size
	for i := 0; i < int(dptCnt); i++ {
		fnamePos := dptEntryPos
		fname := p.GetStr(int32(fnamePos))
		blkPos := fnamePos + int32Size + len(fname)
		blknum := p.GetInt32(int32(blkPos))
		blk := file.NewBlockId(fname, blknum)
		recLSNPos := blkPos + int32Size
		recLSN := p.GetInt32(int32(recLSNPos))
		dpt[*blk] = buffer.NewDirtyPageEntry(*blk, recLSN)
		dptEntryPos = recLSNPos + int32Size
	}

	return &CheckpointEndRecord{lsn, beginLSN, att, dpt}
}

func (r *CheckpointEndRecord) Op() int32 {
	return checkpointEnd
}

func (r *CheckpointEndRecord) PrevLSN() int32 {
	return -1
}

func (r *CheckpointEndRecord) TxNumber() int32 {
	return -1
}

func (r *CheckpointEndRecord) BeginLSN() int32 {
	return r.beginLSN
}

func (r *CheckpointEndRecord) ActiveTrxTable() map[int32]CheckpointTxnEntry {
	return r.att
}

func (r *CheckpointEndRecord) DirtyPageTable() map[file.BlockId]buffer.DirtyPageEntry {
	return r.dpt
}

func (r *CheckpointEndRecord) String() string {
	return "<CHECKPOINT-END>"
}

// Layout:
// [op:int32][beginLSN:int32]
// [attCount:int32] { [txId:int32][status:int32][lastLSN:int32] } * attCount
// [dptCount:int32] { [fileName:str][blknum:int32][recLSN:int32] } * dptCount
func WriteCheckpointEndToLog(lm *log.LogMgr, beginLSN int32, attSnapShot map[int32]CheckpointTxnEntry, dptSnapShot map[file.BlockId]buffer.DirtyPageEntry) (int32, error) {
	// Compute total record size
	// fixed header: op + beginLSN + attCount + dptCount (note: dptCount written after ATT section)
	total := int32(0)
	total += int32Size // op
	total += int32Size // beginLSN
	total += int32Size // attCount

	attCount := int32(len(attSnapShot))
	total += attCount * (3 * int32Size) // each ATT entry: txId, status, lastLSN

	total += int32Size // dptCount
	// DPT entries: fileName (len+bytes) + blknum + recLSN
	for blk := range dptSnapShot {
		// Ensure we can call methods with pointer receiver
		b := blk // value copy
		fname := (&b).FileName()
		total += int32Size + int32(len(fname)) // str length + bytes
		total += int32Size                     // blknum
		total += int32Size                     // recLSN
	}

	enc := newRecordEncoder(total)

	if err := enc.PutInt32(checkpointEnd); err != nil {
		return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
	}
	if err := enc.PutInt32(beginLSN); err != nil {
		return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
	}

	// ATT section
	if err := enc.PutInt32(attCount); err != nil {
		return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
	}
	for txId, entry := range attSnapShot {
		if err := enc.PutInt32(txId); err != nil {
			return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
		}
		if err := enc.PutInt32(entry.Status); err != nil {
			return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
		}
		if err := enc.PutInt32(entry.LastLSN); err != nil {
			return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
		}
	}

	// DPT section
	dptCount := int32(len(dptSnapShot))
	if err := enc.PutInt32(dptCount); err != nil {
		return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
	}
	for blk, dptEntry := range dptSnapShot {
		b := blk // value copy
		if err := enc.PutBlock(&b); err != nil {
			return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
		}
		if err := enc.PutInt32(dptEntry.GetRecLSN()); err != nil {
			return -1, fmt.Errorf("could not encode checkpoint end record: %w", err)
		}
	}

	lsn, err := lm.Append(enc.Bytes())
	if err != nil {
		return -1, fmt.Errorf("could not write checkpoint end to log: %w", err)
	}
	return lsn, nil
}
