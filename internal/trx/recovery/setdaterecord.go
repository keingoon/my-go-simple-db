package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

// SetDateRecord represents a SETDATE log record
// Date is stored as int64 (e.g., unix seconds)
type SetDateRecord struct {
	txnum  int32
	offset int32
	val    time.Time
	blk    *file.BlockId
}

func NewSetDateRecord(p *file.Page) *SetDateRecord {
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
	val := p.GetDate(int32(vpos))

	return &SetDateRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (r *SetDateRecord) Op() int32 { return setDate }

func (r *SetDateRecord) TxNumber() int32 { return r.txnum }

func (r *SetDateRecord) String() string {
	return fmt.Sprintf("<SETDATE %d %s %d %s>", r.txnum, r.blk.ToString(), r.offset, r.val)
}

func (r *SetDateRecord) Undo(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.XLock(ctx, r.blk)
	txAccess.SetDate(ctx, r.blk, r.offset, r.val, false, nil) // don't log the undo!
	txAccess.Unlock(ctx, r.blk)
	txAccess.Unpin(ctx, r.blk)
}

func WriteSetDateToLog(lm *log.LogMgr, txnum int32, blk *file.BlockId, offset int32, val time.Time) (int32, error) {
	tpos := int32Size
	fpos := tpos + int32Size
	bpos := fpos + file.MaxLength(len(blk.FileName()))
	opos := bpos + int32Size
	vpos := opos + int32Size
	rec := make([]byte, vpos+dateSize)
	p := file.NewLogPage(rec)
	p.SetInt32(0, setDate)
	p.SetInt32(int32(tpos), txnum)
	p.SetStr(int32(fpos), blk.FileName())
	p.SetInt32(int32(bpos), blk.Number())
	p.SetInt32(int32(opos), offset)
	p.SetDate(int32(vpos), val)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write date record to log: %w", err)
	}
	return lsn, nil
}
