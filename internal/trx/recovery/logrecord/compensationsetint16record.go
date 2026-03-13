package logrecord

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type CompensationSetInt16Record struct {
	lsn         int32
	prevLSN     int32
	txnum       int32
	blk         *file.BlockId
	offset      int32
	oldVal      int16
	newVal      int16
	undoNextLSN int32
}

func NewCompensationSetInt16Record(p *file.Page, lsn int32) *CompensationSetInt16Record {
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
	oldVal := p.GetInt16(int32(oldPos))
	newPos := oldPos + int16Size
	newVal := p.GetInt16(int32(newPos))
	undoNextPos := newPos + int16Size
	undoNextLSN := p.GetInt32(int32(undoNextPos))

	return &CompensationSetInt16Record{
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

func (r *CompensationSetInt16Record) Op() int32 {
	return compensationSetInt16
}

func (r *CompensationSetInt16Record) PrevLSN() int32 { return r.prevLSN }

func (r *CompensationSetInt16Record) TxNumber() int32 {
	return r.txnum
}

func (r *CompensationSetInt16Record) Blk() *file.BlockId { return r.blk }

func (r *CompensationSetInt16Record) UndoNextLSN() int32 { return r.undoNextLSN }

func (r *CompensationSetInt16Record) String() string {
	return fmt.Sprintf("<COMPENSATION SETINT16 %d %s %d %d %d %d>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal, r.undoNextLSN)
}

func (r *CompensationSetInt16Record) RedoPage(ctx context.Context, txAccess *access.Transaction) error {
	if err := txAccess.Pin(ctx, r.blk); err != nil {
		return err
	}
	if err := txAccess.ApplyInt16(ctx, r.lsn, r.txnum, r.blk, r.offset, r.newVal); err != nil {
		txAccess.Unpin(ctx, r.blk)
		return err
	}
	txAccess.Unpin(ctx, r.blk)
	return nil
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:int16][newVal:int16][undoNextLSN:int32]
func WriteCompensationSetInt16ToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal int16, newVal int16, undoNextLSN int32) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	fPos := tPos + int32Size
	bPos := fPos + file.VarBytesLen(len(blk.FileName()))
	oPos := bPos + int32Size
	oldPos := oPos + int32Size
	newPos := oldPos + int16Size
	undoNextPos := newPos + int16Size
	rec := make([]byte, undoNextPos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, compensationSetInt16)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	p.SetStr(int32(fPos), blk.FileName())
	p.SetInt32(int32(bPos), blk.Number())
	p.SetInt32(int32(oPos), offset)
	p.SetInt16(int32(oldPos), oldVal)
	p.SetInt16(int32(newPos), newVal)
	p.SetInt32(int32(undoNextPos), undoNextLSN)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write int16 record to log: %w", err)
	}
	return lsn, nil
}
