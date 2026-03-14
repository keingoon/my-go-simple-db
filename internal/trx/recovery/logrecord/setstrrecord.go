package logrecord

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type SetStrRecord struct {
	lsn     int32
	prevLSN int32
	txnum   int32
	blk     *file.BlockId
	offset  int32
	oldVal  string
	newVal  string
}

func NewSetStrRecord(p *file.Page, lsn int32) *SetStrRecord {
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

	return &SetStrRecord{
		lsn,
		prevLSN,
		txnum,
		blk,
		offset,
		oldVal,
		newVal,
	}
}

func (r *SetStrRecord) Op() int32 { return setStr }

func (r *SetStrRecord) PrevLSN() int32 { return r.prevLSN }

func (r *SetStrRecord) TxNumber() int32 { return r.txnum }

func (r *SetStrRecord) Blk() *file.BlockId { return r.blk }

func (r *SetStrRecord) String() string {
	return fmt.Sprintf("<SETSTR %d %s %d %s %s>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal)
}

func (r *SetStrRecord) UndoPage(ctx context.Context, txAccess *access.Transaction, clrLSN int32) error {
	if err := txAccess.Pin(ctx, r.blk); err != nil {
		return err
	}
	if err := txAccess.ApplyStr(ctx, clrLSN, r.txnum, r.blk, r.offset, r.oldVal); err != nil {
		txAccess.Unpin(ctx, r.blk)
		return err
	}
	txAccess.Unpin(ctx, r.blk)
	return nil
}

func (r *SetStrRecord) RedoPage(ctx context.Context, txAccess *access.Transaction) error {
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

func (r *SetStrRecord) WriteCLR(ctx context.Context, txAccess *access.Transaction, lm *log.LogMgr, prevLSN int32) (int32, error) {
	undoNextLSN := r.prevLSN
	return WriteCompensationSetStrToLog(lm, prevLSN, r.txnum, r.blk, r.offset, r.newVal, r.oldVal, undoNextLSN)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:string][newVal:string]
func WriteSetStrToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal string, newVal string) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	fPos := tPos + int32Size
	bPos := fPos + file.VarBytesLen(len(blk.FileName()))
	oPos := bPos + int32Size
	oldPos := oPos + int32Size
	newPos := oldPos + file.VarBytesLen(len(oldVal))
	recLen := newPos + file.VarBytesLen(len(newVal))
	rec := make([]byte, recLen)
	p := file.NewLogPage(rec)
	if err := p.SetInt32(0, setStr); err != nil {
		return -1, fmt.Errorf("could not encode set str record: %w", err)
	}
	if err := p.SetInt32(int32(prevPos), prevLSN); err != nil {
		return -1, fmt.Errorf("could not encode set str record: %w", err)
	}
	if err := p.SetInt32(int32(tPos), txnum); err != nil {
		return -1, fmt.Errorf("could not encode set str record: %w", err)
	}
	if err := p.SetStr(int32(fPos), blk.FileName()); err != nil {
		return -1, fmt.Errorf("could not encode set str record: %w", err)
	}
	if err := p.SetInt32(int32(bPos), blk.Number()); err != nil {
		return -1, fmt.Errorf("could not encode set str record: %w", err)
	}
	if err := p.SetInt32(int32(oPos), offset); err != nil {
		return -1, fmt.Errorf("could not encode set str record: %w", err)
	}
	if err := p.SetStr(int32(oldPos), oldVal); err != nil {
		return -1, fmt.Errorf("could not encode set str record: %w", err)
	}
	if err := p.SetStr(int32(newPos), newVal); err != nil {
		return -1, fmt.Errorf("could not encode set str record: %w", err)
	}
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write str record to log: %w", err)
	}
	return lsn, nil
}
