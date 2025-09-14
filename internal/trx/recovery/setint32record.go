package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

type SetInt32Record struct {
	txnum  int32
	offset int32
	val    int32
	blk    *file.BlockId
}

func NewSetInt32Record(p *file.Page) *SetInt32Record {
	tpos := int32Size
	txnum := p.GetInt32(int32(tpos))
	fpos := tpos + int32Size
	filename := p.GetStr(int32(fpos))
	bpos := fpos + file.MaxLength(len(filename))
	blknum := p.GetInt32(int32(bpos))
	blk := file.NewBlockId(filename, blknum)
	opos := bpos + int32Size
	offset := p.GetInt32(int32(opos))
	vpos := opos + int32Size
	val := p.GetInt32(int32(vpos))

	return &SetInt32Record{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (r *SetInt32Record) Op() int32 {
	return setInt32
}

func (r *SetInt32Record) TxNumber() int32 {
	return r.txnum
}

func (r *SetInt32Record) String() string {
	return fmt.Sprintf("<SETINT32 %d %s %d %d>", r.txnum, r.blk.ToString(), r.offset, r.val)
}

func (r *SetInt32Record) Undo(ctx context.Context, tx undoContext) {
	tx.Pin(ctx, r.blk)
	tx.SetInt32(ctx, r.blk, r.offset, r.val, false) // don't log the undo!
	tx.Unpin(ctx, r.blk)
}

func WriteSetInt32ToLog(lm *log.LogMgr, txnum int32, blk *file.BlockId, offset int32, val int32) (int32, error) {
	tpos := int32Size
	fpos := tpos + int32Size
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + int32Size
	vpos := opos + int32Size
	rec := make([]byte, vpos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, setInt32)
	p.SetInt32(int32(tpos), txnum)
	p.SetStr(int32(fpos), blk.FileName())
	p.SetInt32(int32(bpos), blk.Number())
	p.SetInt32(int32(opos), offset)
	p.SetInt32(int32(vpos), val)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write int32 record to log: %w", err)
	}
	return lsn, nil
}
