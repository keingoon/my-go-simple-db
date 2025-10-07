package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type SetInt16Record struct {
	txnum  int32
	offset int32
	val    int16
	blk    *file.BlockId
}

func NewSetInt16Record(p *file.Page) *SetInt16Record {
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
	val := p.GetInt16(int32(vpos))

	return &SetInt16Record{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (r *SetInt16Record) Op() int32 {
	return setInt16
}

func (r *SetInt16Record) TxNumber() int32 {
	return r.txnum
}

func (r *SetInt16Record) String() string {
	return fmt.Sprintf("<SETINT16 %d %s %d %d>", r.txnum, r.blk.ToString(), r.offset, r.val)
}

func (r *SetInt16Record) Undo(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.XLock(ctx, r.blk)
	txAccess.SetInt16(ctx, r.blk, r.offset, r.val, false, nil) // don't log the undo!
	txAccess.Unlock(ctx, r.blk)
	txAccess.Unpin(ctx, r.blk)
}

func WriteSetInt16ToLog(lm *log.LogMgr, txnum int32, blk *file.BlockId, offset int32, val int16) (int32, error) {
	tpos := int32Size
	fpos := tpos + int32Size
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + int32Size
	vpos := opos + int32Size
	rec := make([]byte, vpos+int16Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, setInt16)
	p.SetInt32(int32(tpos), txnum)
	p.SetStr(int32(fpos), blk.FileName())
	p.SetInt32(int32(bpos), blk.Number())
	p.SetInt32(int32(opos), offset)
	p.SetInt16(int32(vpos), val)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write int16 record to log: %w", err)
	}
	return lsn, nil
}
