package recovery

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
)

// SetBoolRecord represents a SETBOOL log record
type SetBoolRecord struct {
	lsn     int32
	prevLSN int32
	txnum   int32
	blk     *file.BlockId
	offset  int32
	oldVal  bool
	newVal  bool
}

func NewSetBoolRecord(p *file.Page, lsn int32) *SetBoolRecord {
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
	oldVal := p.GetBool(int32(oldPos))
	newPos := oldPos + boolSize
	newVal := p.GetBool(int32(newPos))

	return &SetBoolRecord{
		lsn,
		prevLSN,
		txnum,
		blk,
		offset,
		oldVal,
		newVal,
	}
}

func (r *SetBoolRecord) Op() int32 { return setBool }

func (r *SetBoolRecord) PrevLSN() int32 { return r.prevLSN }

func (r *SetBoolRecord) TxNumber() int32 { return r.txnum }

func (r *SetBoolRecord) Blk() *file.BlockId { return r.blk }

func (r *SetBoolRecord) String() string {
	return fmt.Sprintf("<SETBOOL %d %s %d %t %t>", r.txnum, r.blk.ToString(), r.offset, r.oldVal, r.newVal)
}

func (r *SetBoolRecord) Undo(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.ApplyBool(ctx, r.lsn, r.txnum, r.blk, r.offset, r.oldVal)
	txAccess.Unpin(ctx, r.blk)
}

func (r *SetBoolRecord) Redo(ctx context.Context, txAccess *access.Transaction) {
	txAccess.Pin(ctx, r.blk)
	txAccess.ApplyBool(ctx, r.lsn, r.txnum, r.blk, r.offset, r.newVal) // TODO: add CLR log fn
	txAccess.Unpin(ctx, r.blk)
}

// Layout:
// [op:int32][prevLSN:int32][txnum:int32][fileName:str][blknum:int32][offset:int32][oldVal:bool][newVal:bool]
func WriteSetBoolToLog(lm *log.LogMgr, prevLSN int32, txnum int32, blk *file.BlockId, offset int32, oldVal bool, newVal bool) (int32, error) {
	prevPos := int32Size
	tPos := prevPos + int32Size
	fPos := tPos + int32Size
	bPos := fPos + file.MaxLength(len(blk.FileName()))
	oPos := bPos + int32Size
	oldPos := oPos + int32Size
	newPos := oldPos + boolSize
	rec := make([]byte, newPos+boolSize)
	p := file.NewLogPage(rec)
	p.SetInt32(0, setBool)
	p.SetInt32(int32(prevPos), prevLSN)
	p.SetInt32(int32(tPos), txnum)
	p.SetStr(int32(fPos), blk.FileName())
	p.SetInt32(int32(bPos), blk.Number())
	p.SetInt32(int32(oPos), offset)
	p.SetBool(int32(oldPos), oldVal)
	p.SetBool(int32(newPos), newVal)
	lsn, err := lm.Append(rec)
	if err != nil {
		return -1, fmt.Errorf("could not write bool record to log: %w", err)
	}
	return lsn, nil
}
