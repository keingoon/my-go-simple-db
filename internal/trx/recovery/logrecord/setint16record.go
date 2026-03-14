package logrecord

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type SetInt16Record struct {
	lsn     int32
	prevLSN int32
	txnum   int32
	blk     *file.BlockId
	offset  int32
	oldVal  int16
	newVal  int16
}

func NewSetInt16Record(p *file.Page, lsn int32) *SetInt16Record {
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

	return &SetInt16Record{
		lsn,
		prevLSN,
		txnum,
		blk,
		offset,
		oldVal,
		newVal,
	}
}

func (r *SetInt16Record) Op() int32 { return setInt16 }

func (r *SetInt16Record) PrevLSN() int32 { return r.prevLSN }

func (r *SetInt16Record) TxNumber() int32 { return r.txnum }

func (r *SetInt16Record) Blk() *file.BlockId { return r.blk }

func (r *SetInt16Record) String() string {
	return fmt.Sprintf("<SETINT16 %d %s %d %d %d>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal)
}

func (r *SetInt16Record) UndoPage(ctx context.Context, txAccess *access.Transaction, clrLSN int32) error {
	if err := txAccess.Pin(ctx, r.blk); err != nil {
		return err
	}
	if err := txAccess.ApplyInt16(ctx, clrLSN, r.txnum, r.blk, r.offset, r.oldVal); err != nil {
		txAccess.Unpin(ctx, r.blk)
		return err
	}
	txAccess.Unpin(ctx, r.blk)
	return nil
}

func (r *SetInt16Record) RedoPage(ctx context.Context, txAccess *access.Transaction) error {
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

func (r *SetInt16Record) WriteCLR(ctx context.Context, txAccess *access.Transaction, lm *log.LogMgr, prevLSN int32) (int32, error) {
	undoNextLSN := r.prevLSN
	return WriteCompensationSetInt16ToLog(lm, prevLSN, r.txnum, r.blk, r.offset, r.newVal, r.oldVal, undoNextLSN)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:int16][newVal:int16]
func WriteSetInt16ToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal int16, newVal int16) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	fPos := tPos + int32Size
	bPos := fPos + file.VarBytesLen(len(blk.FileName()))
	oPos := bPos + int32Size
	oldPos := oPos + int32Size
	newPos := oldPos + int16Size
	rec := make([]byte, newPos+int16Size)
	p := file.NewLogPage(rec)
	if err := p.SetInt32(0, setInt16); err != nil {
		return -1, fmt.Errorf("could not encode set int16 record: %w", err)
	}
	if err := p.SetInt32(int32(prevPos), prevLSN); err != nil {
		return -1, fmt.Errorf("could not encode set int16 record: %w", err)
	}
	if err := p.SetInt32(int32(tPos), txnum); err != nil {
		return -1, fmt.Errorf("could not encode set int16 record: %w", err)
	}
	if err := p.SetStr(int32(fPos), blk.FileName()); err != nil {
		return -1, fmt.Errorf("could not encode set int16 record: %w", err)
	}
	if err := p.SetInt32(int32(bPos), blk.Number()); err != nil {
		return -1, fmt.Errorf("could not encode set int16 record: %w", err)
	}
	if err := p.SetInt32(int32(oPos), offset); err != nil {
		return -1, fmt.Errorf("could not encode set int16 record: %w", err)
	}
	if err := p.SetInt16(int32(oldPos), oldVal); err != nil {
		return -1, fmt.Errorf("could not encode set int16 record: %w", err)
	}
	if err := p.SetInt16(int32(newPos), newVal); err != nil {
		return -1, fmt.Errorf("could not encode set int16 record: %w", err)
	}
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write int16 record to log: %w", err)
	}
	return lsn, nil
}
