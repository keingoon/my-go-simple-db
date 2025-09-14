package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

// SetBoolRecord represents a SETBOOL log record
type SetBoolRecord struct {
	txnum  int32
	offset int32
	val    bool
	blk    *file.BlockId
}

func NewSetBoolRecord(p *file.Page) *SetBoolRecord {
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
	val := p.GetBool(int32(vpos))

	return &SetBoolRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (r *SetBoolRecord) Op() int32 { return setBool }

func (r *SetBoolRecord) TxNumber() int32 { return r.txnum }

func (r *SetBoolRecord) String() string {
	return fmt.Sprintf("<SETBOOL %d %s %d %t>", r.txnum, r.blk.ToString(), r.offset, r.val)
}

func (r *SetBoolRecord) Undo(ctx context.Context, tx undoContext) {
	tx.Pin(ctx, r.blk)
	tx.SetBool(ctx, r.blk, r.offset, r.val, false) // don't log the undo!
	tx.Unpin(ctx, r.blk)
}

func WriteSetBoolToLog(lm *log.LogMgr, txnum int32, blk *file.BlockId, offset int32, val bool) (int32, error) {
	tpos := int32Size
	fpos := tpos + int32Size
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + int32Size
	vpos := opos + int32Size
	rec := make([]byte, vpos+boolSize)
	p := file.NewLogPage(rec)
	p.SetInt32(0, setBool)
	p.SetInt32(int32(tpos), txnum)
	p.SetStr(int32(fpos), blk.FileName())
	p.SetInt32(int32(bpos), blk.Number())
	p.SetInt32(int32(opos), offset)
	p.SetBool(int32(vpos), val)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write bool record to log: %w", err)
	}
	return lsn, nil
}
