package logrecord

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type CompensationSetStrRecord struct {
	lsn         int32
	prevLSN     int32
	txnum       int32
	blk         *file.BlockId
	offset      int32
	oldVal      string
	newVal      string
	undoNextLSN int32
}

func NewCompensationSetStrRecord(p *file.Page, lsn int32) *CompensationSetStrRecord {
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
	oldVal := p.GetStr(int32(oldPos))
	newPos := oldPos + file.VarBytesLen(len(oldVal))
	newVal := p.GetStr(int32(newPos))
	undoNextPos := newPos + file.VarBytesLen(len(newVal))
	undoNextLSN := p.GetInt32(int32(undoNextPos))

	return &CompensationSetStrRecord{
		lsn,
		prevLSN,
		txnum,
		blk,
		offset,
		oldVal,
		newVal,
		undoNextLSN,
	}
}

func (r *CompensationSetStrRecord) Op() int32 { return compensationSetStr }

func (r *CompensationSetStrRecord) PrevLSN() int32 { return r.prevLSN }

func (r *CompensationSetStrRecord) TxNumber() int32 { return r.txnum }

func (r *CompensationSetStrRecord) Blk() *file.BlockId { return r.blk }

func (r *CompensationSetStrRecord) UndoNextLSN() int32 { return r.undoNextLSN }

func (r *CompensationSetStrRecord) String() string {
	return fmt.Sprintf("<COMPENSATION SETSTR %d %s %d %s %s %d>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal, r.undoNextLSN)
}

func (r *CompensationSetStrRecord) RedoPage(ctx context.Context, txAccess *access.Transaction) error {
	if err := txAccess.Pin(ctx, r.blk); err != nil {
		return err
	}
	if err := txAccess.ApplyStr(ctx, r.lsn, r.txnum, r.blk, r.offset, r.newVal); err != nil {
		txAccess.Unpin(ctx, r.blk)
		return err
	}
	txAccess.Unpin(ctx, r.blk)
	return nil
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:string][newVal:string][undoNextLSN:int32]
func WriteCompensationSetStrToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal string, newVal string, undoNextLSN int32) (int32, error) {
	recLen := int32Size + int32Size + int32Size + int32(file.VarBytesLen(len(blk.FileName()))) + int32Size + int32Size + int32(file.VarBytesLen(len(oldVal))) + int32(file.VarBytesLen(len(newVal))) + int32Size
	enc := newRecordEncoder(recLen)
	if err := enc.PutInt32(compensationSetStr); err != nil {
		return -1, fmt.Errorf("could not encode compensation set str record: %w", err)
	}
	if err := enc.PutInt32(prevLSN); err != nil {
		return -1, fmt.Errorf("could not encode compensation set str record: %w", err)
	}
	if err := enc.PutInt32(txnum); err != nil {
		return -1, fmt.Errorf("could not encode compensation set str record: %w", err)
	}
	if err := enc.PutBlock(blk); err != nil {
		return -1, fmt.Errorf("could not encode compensation set str record: %w", err)
	}
	if err := enc.PutInt32(offset); err != nil {
		return -1, fmt.Errorf("could not encode compensation set str record: %w", err)
	}
	if err := enc.PutStr(oldVal); err != nil {
		return -1, fmt.Errorf("could not encode compensation set str record: %w", err)
	}
	if err := enc.PutStr(newVal); err != nil {
		return -1, fmt.Errorf("could not encode compensation set str record: %w", err)
	}
	if err := enc.PutInt32(undoNextLSN); err != nil {
		return -1, fmt.Errorf("could not encode compensation set str record: %w", err)
	}
	lsn, err := lm.Append(enc.Bytes())
	if err != nil {
		return -1, fmt.Errorf("could not write compensation set str record to log: %w", err)
	}
	return lsn, nil
}
