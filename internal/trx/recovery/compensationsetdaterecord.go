package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

// CompensationSetDateRecord represents a COMPENSATION SETDATE log record
// Date is stored as int64 (e.g., unix seconds)
type CompensationSetDateRecord struct {
	lsn         int32
	prevLSN     int32
	txnum       int32
	blk         *file.BlockId
	offset      int32
	oldVal      time.Time
	newVal      time.Time
	undoNextLSN int32
}

func NewCompensationSetDateRecord(p *file.Page, lsn int32) *CompensationSetDateRecord {
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
	oldVal := p.GetDate(int32(oldPos))
	newPos := oldPos + dateSize
	newVal := p.GetDate(int32(newPos))
	undoNextPos := newPos + dateSize
	undoNextLSN := p.GetInt32(int32(undoNextPos))

	return &CompensationSetDateRecord{
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

func (r *CompensationSetDateRecord) Op() int32 { return compensationSetDate }

func (r *CompensationSetDateRecord) PrevLSN() int32 { return r.prevLSN }

func (r *CompensationSetDateRecord) TxNumber() int32 { return r.txnum }

func (r *CompensationSetDateRecord) Blk() *file.BlockId { return r.blk }

func (r *CompensationSetDateRecord) UndoNextLSN() int32 { return r.undoNextLSN }

func (r *CompensationSetDateRecord) String() string {
	return fmt.Sprintf("<COMPENSATION SETDATE %d %s %d %s %s %d>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal, r.undoNextLSN)
}

// Compensation SetDate log cannot be undone
func (r *CompensationSetDateRecord) Undo(ctx context.Context, txAccess *access.Transaction) {}

func (r *CompensationSetDateRecord) Redo(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.ApplyDate(ctx, r.lsn, r.txnum, r.blk, r.offset, r.newVal)
	txAccess.Unpin(ctx, r.blk)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:time.Time][newVal:time.Time][undoNextLSN:int32]
func WriteCompensationSetDateToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal time.Time, newVal time.Time, undoNextLSN int32) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	fPos := tPos + int32Size
	bPos := fPos + file.MaxLength(len(blk.FileName()))
	oPos := bPos + int32Size
	oldPos := oPos + int32Size
	newPos := oldPos + dateSize
	undoNextPos := newPos + dateSize
	rec := make([]byte, undoNextPos+int32Size)
	p := file.NewLogPage(rec)
	p.SetInt32(0, compensationSetDate)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	p.SetStr(int32(fPos), blk.FileName())
	p.SetInt32(int32(bPos), blk.Number())
	p.SetInt32(int32(oPos), offset)
	p.SetDate(int32(oldPos), oldVal)
	p.SetDate(int32(newPos), newVal)
	p.SetInt32(int32(undoNextPos), undoNextLSN)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write compensation set date record to log: %w", err)
	}
	return lsn, nil
}
