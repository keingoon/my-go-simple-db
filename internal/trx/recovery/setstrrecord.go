package recovery

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
	offset  int32
	oldVal  string
	newVal  string
	blk     *file.BlockId
}

func NewSetStrRecord(p *file.Page, lsn int32) *SetStrRecord {
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

	return &SetStrRecord{
		lsn,
		prevLSN,
		txnum,
		offset,
		oldVal,
		newVal,
		blk,
	}
}

func (r *SetStrRecord) Op() int32 {
	return setStr
}

func (r *SetStrRecord) TxNumber() int32 {
	return r.txnum
}

func (r *SetStrRecord) String() string {
	return fmt.Sprintf("<SETSTR %d %s %d %s %s>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal)
}

func (r *SetStrRecord) Undo(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.SetStr(ctx, r.blk, r.offset, r.oldVal, false, nil) // don't log the undo!
	txAccess.Unpin(ctx, r.blk)
}

func (r *SetStrRecord) Redo(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.SetStr(ctx, r.blk, r.offset, r.newVal, false, nil) // don't log the undo!
	txAccess.Unpin(ctx, r.blk)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:string][newVal:string]
func WriteSetStrToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal string, newVal string) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	fPos := tPos + int32Size
	bPos := fPos + file.MaxLength(len(blk.FileName()))
	oPos := bPos + int32Size
	oldPos := oPos + int32Size
	newPos := oldPos + file.MaxLength(len(oldVal))
	recLen := newPos + file.MaxLength(len(newVal))
	rec := make([]byte, recLen)
	p := file.NewLogPage(rec)
	p.SetInt32(0, setStr)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	p.SetStr(int32(fPos), blk.FileName())
	p.SetInt32(int32(bPos), blk.Number())
	p.SetInt32(int32(oPos), offset)
	p.SetStr(int32(oldPos), oldVal)
	p.SetStr(int32(newPos), newVal)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write str record to log: %w", err)
	}
	return lsn, nil
}
