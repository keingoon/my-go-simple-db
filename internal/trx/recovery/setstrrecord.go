package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

type SetStrRecord struct {
	txnum  int32
	offset int32
	val    string
	blk    *file.BlockId
}

func NewSetStrRecord(p *file.Page) *SetStrRecord {
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
	val := p.GetStr(int32(vpos))

	return &SetStrRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (r *SetStrRecord) Op() int32 {
	return setStr
}

func (r *SetStrRecord) TxNumber() int32 {
	return r.txnum
}

func (r *SetStrRecord) String() string {
	return fmt.Sprintf("<SETSTR %d %s %d %s>", r.txnum, r.blk.ToString(), r.offset, r.val)
}

func (r *SetStrRecord) Undo(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.XLock(ctx, r.blk)
	txAccess.SetStr(ctx, r.blk, r.offset, r.val, false, nil) // don't log the undo!
	txAccess.Unlock(ctx, r.blk)
	txAccess.Unpin(ctx, r.blk)
}

func WriteSetStrToLog(lm *log.LogMgr, txnum int32, blk *file.BlockId, offset int32, val string) (int32, error) {
	tpos := int32Size
	fpos := tpos + int32Size
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + int32Size
	vpos := opos + int32Size
	recLen := vpos + file.MaxLength(len(val))
	rec := make([]byte, recLen)
	p := file.NewLogPage(rec)
	p.SetInt32(0, setStr)
	p.SetInt32(int32(tpos), txnum)
	p.SetStr(int32(fpos), blk.FileName())
	p.SetInt32(int32(bpos), blk.Number())
	p.SetInt32(int32(opos), offset)
	p.SetStr(int32(vpos), val)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write str record to log: %w", err)
	}
	return lsn, nil
}
