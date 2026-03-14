package logrecord

import (
	"context"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

// SetDateRecord represents a SETDATE log record
// Date is stored as int64 (e.g., unix seconds)
type SetDateRecord struct {
	lsn     int32
	prevLSN int32
	txnum   int32
	blk     *file.BlockId
	offset  int32
	oldVal  time.Time
	newVal  time.Time
}

func NewSetDateRecord(p *file.Page, lsn int32) *SetDateRecord {
	prevPos := int32Size
	prevLSN := p.GetInt32(int32(prevPos))
	tPos := prevPos + int32Size
	txnum := p.GetInt32(int32(tPos))
	fPos := tPos + int32Size
	filename := p.GetStr(int32(fPos))
	bPos := fPos + file.VarBytesLen(len(filename))
	blknum := p.GetInt32(int32(bPos))
	blk := file.NewBlockId(filename, blknum)
	oPos := bPos + int32Size
	offset := p.GetInt32(int32(oPos))
	oldPos := oPos + int32Size
	oldVal := p.GetDate(int32(oldPos))
	newPos := oldPos + dateSize
	newVal := p.GetDate(int32(newPos))

	return &SetDateRecord{
		lsn,
		prevLSN,
		txnum,
		blk,
		offset,
		oldVal,
		newVal,
	}
}

func (r *SetDateRecord) Op() int32 { return setDate }

func (r *SetDateRecord) PrevLSN() int32 { return r.prevLSN }

func (r *SetDateRecord) TxNumber() int32 { return r.txnum }

func (r *SetDateRecord) Blk() *file.BlockId { return r.blk }

func (r *SetDateRecord) String() string {
	return fmt.Sprintf("<SETDATE %d %s %d %s %s>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal)
}

func (r *SetDateRecord) UndoPage(ctx context.Context, txAccess *access.Transaction, clrLSN int32) error {
	if err := txAccess.Pin(ctx, r.blk); err != nil {
		return err
	}
	if err := txAccess.ApplyDate(ctx, clrLSN, r.txnum, r.blk, r.offset, r.oldVal); err != nil {
		txAccess.Unpin(ctx, r.blk)
		return err
	}
	txAccess.Unpin(ctx, r.blk)
	return nil
}

func (r *SetDateRecord) RedoPage(ctx context.Context, txAccess *access.Transaction) error {
	if err := txAccess.Pin(ctx, r.blk); err != nil {
		return err
	}
	if err := txAccess.ApplyDate(ctx, r.lsn, r.txnum, r.blk, r.offset, r.newVal); err != nil {
		txAccess.Unpin(ctx, r.blk)
		return err
	}
	txAccess.Unpin(ctx, r.blk)
	return nil
}

func (r *SetDateRecord) WriteCLR(ctx context.Context, txAccess *access.Transaction, lm *log.LogMgr, prevLSN int32) (int32, error) {
	undoNextLSN := r.prevLSN
	return WriteCompensationSetDateToLog(lm, prevLSN, r.txnum, r.blk, r.offset, r.newVal, r.oldVal, undoNextLSN)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:time.Time][newVal:time.Time]
func WriteSetDateToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal time.Time, newVal time.Time) (int32, error) {
	recLen := int32Size + int32Size + int32Size + int32(file.VarBytesLen(len(blk.FileName()))) + int32Size + int32Size + dateSize + dateSize
	enc := newRecordEncoder(recLen)
	if err := enc.PutInt32(setDate); err != nil {
		return -1, fmt.Errorf("could not encode set date record: %w", err)
	}
	if err := enc.PutInt32(prevLSN); err != nil {
		return -1, fmt.Errorf("could not encode set date record: %w", err)
	}
	if err := enc.PutInt32(txnum); err != nil {
		return -1, fmt.Errorf("could not encode set date record: %w", err)
	}
	if err := enc.PutBlock(blk); err != nil {
		return -1, fmt.Errorf("could not encode set date record: %w", err)
	}
	if err := enc.PutInt32(offset); err != nil {
		return -1, fmt.Errorf("could not encode set date record: %w", err)
	}
	if err := enc.PutDate(oldVal); err != nil {
		return -1, fmt.Errorf("could not encode set date record: %w", err)
	}
	if err := enc.PutDate(newVal); err != nil {
		return -1, fmt.Errorf("could not encode set date record: %w", err)
	}
	lsn, err := lm.Append(enc.Bytes())
	if err != nil {
		return -1, fmt.Errorf("could not write date record to log: %w", err)
	}
	return lsn, nil
}
