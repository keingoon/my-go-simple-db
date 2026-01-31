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
	bPos := fPos + file.MaxLength(len(filename))
	blknum := p.GetInt32(int32(bPos))
	blk := file.NewBlockId(filename, blknum)
	oPos := bPos + int32Size
	offset := p.GetInt32(int32(oPos))
	oldPos := oPos + int32Size
	oldVal := p.GetStr(int32(oldPos))
	newPos := oldPos + file.MaxLength(len(oldVal))
	newVal := p.GetStr(int32(newPos))
	undoNextPos := newPos + file.MaxLength(len(newVal))
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

func (r *CompensationSetStrRecord) RedoPage(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.ApplyStr(ctx, r.lsn, r.txnum, r.blk, r.offset, r.newVal)
	txAccess.Unpin(ctx, r.blk)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:string][newVal:string][undoNextLSN:int32]
func WriteCompensationSetStrToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal string, newVal string, undoNextLSN int32) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	fPos := tPos + int32Size
	bPos := fPos + file.MaxLength(len(blk.FileName()))
	oPos := bPos + int32Size
	oldPos := oPos + int32Size
	newPos := oldPos + file.MaxLength(len(oldVal))
	undoNextPos := newPos + file.MaxLength(len(newVal))
	rec := make([]byte, undoNextPos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, compensationSetStr)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	p.SetStr(int32(fPos), blk.FileName())
	p.SetInt32(int32(bPos), blk.Number())
	p.SetInt32(int32(oPos), offset)
	p.SetStr(int32(oldPos), oldVal)
	p.SetStr(int32(newPos), newVal)
	p.SetInt32(int32(undoNextPos), undoNextLSN)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write compensation set str record to log: %w", err)
	}
	return lsn, nil
}
