package logrecord

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

// CompensationSetBoolRecord represents a COMPENSATION SETBOOL log record
type CompensationSetBoolRecord struct {
	lsn         int32
	prevLSN     int32
	txnum       int32
	blk         *file.BlockId
	offset      int32
	oldVal      bool
	newVal      bool
	undoNextLSN int32
}

func NewCompensationSetBoolRecord(p *file.Page, lsn int32) *CompensationSetBoolRecord {
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
	oldVal := p.GetBool(int32(oldPos))
	newPos := oldPos + boolSize
	newVal := p.GetBool(int32(newPos))
	undoNextPos := newPos + boolSize
	undoNextLSN := p.GetInt32(int32(undoNextPos))

	return &CompensationSetBoolRecord{
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

func (r *CompensationSetBoolRecord) Op() int32 { return compensationSetBool }

func (r *CompensationSetBoolRecord) PrevLSN() int32 { return r.prevLSN }

func (r *CompensationSetBoolRecord) TxNumber() int32 { return r.txnum }

func (r *CompensationSetBoolRecord) Blk() *file.BlockId { return r.blk }

func (r *CompensationSetBoolRecord) UndoNextLSN() int32 { return r.undoNextLSN }

func (r *CompensationSetBoolRecord) String() string {
	return fmt.Sprintf("<COMPENSATION SETBOOL %d %s %d %t %t %d>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal, r.undoNextLSN)
}

func (r *CompensationSetBoolRecord) RedoPage(ctx context.Context, txAccess *access.Transaction) error {
	if err := txAccess.Pin(ctx, r.blk); err != nil {
		return err
	}
	if err := txAccess.ApplyBool(ctx, r.lsn, r.txnum, r.blk, r.offset, r.newVal); err != nil {
		txAccess.Unpin(ctx, r.blk)
		return err
	}
	txAccess.Unpin(ctx, r.blk)
	return nil
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:bool][newVal:bool][undoNextLSN:int32]
func WriteCompensationSetBoolToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal bool, newVal bool, undoNextLSN int32) (int32, error) {
	recLen := int32Size + int32Size + int32Size + int32(file.VarBytesLen(len(blk.FileName()))) + int32Size + int32Size + boolSize + boolSize + int32Size
	enc := newRecordEncoder(recLen)
	if err := enc.PutInt32(compensationSetBool); err != nil {
		return -1, fmt.Errorf("could not encode compensation set bool record: %w", err)
	}
	if err := enc.PutInt32(prevLSN); err != nil {
		return -1, fmt.Errorf("could not encode compensation set bool record: %w", err)
	}
	if err := enc.PutInt32(txnum); err != nil {
		return -1, fmt.Errorf("could not encode compensation set bool record: %w", err)
	}
	if err := enc.PutBlock(blk); err != nil {
		return -1, fmt.Errorf("could not encode compensation set bool record: %w", err)
	}
	if err := enc.PutInt32(offset); err != nil {
		return -1, fmt.Errorf("could not encode compensation set bool record: %w", err)
	}
	if err := enc.PutBool(oldVal); err != nil {
		return -1, fmt.Errorf("could not encode compensation set bool record: %w", err)
	}
	if err := enc.PutBool(newVal); err != nil {
		return -1, fmt.Errorf("could not encode compensation set bool record: %w", err)
	}
	if err := enc.PutInt32(undoNextLSN); err != nil {
		return -1, fmt.Errorf("could not encode compensation set bool record: %w", err)
	}
	lsn, err := lm.Append(enc.Bytes())
	if err != nil {
		return -1, fmt.Errorf("could not write compensation set bool record to log: %w", err)
	}
	return lsn, nil
}
