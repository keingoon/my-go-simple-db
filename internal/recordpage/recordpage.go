package recordpage

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/record"
	"github.com/keingoon/simpledb/internal/trx/tx"
)

const (
	Empty int32 = iota
	Used
)

type RecordPage struct {
	tx     *tx.TransactionMgr
	blk    *file.BlockId
	layout *record.Layout
}

func NewRecordPage(ctx context.Context, tx *tx.TransactionMgr, blk *file.BlockId, layout *record.Layout) *RecordPage {
	tx.Pin(ctx, blk)
	return &RecordPage{tx, blk, layout}
}

func (rp *RecordPage) GetInt(ctx context.Context, slot int32, fldname string) (int32, error) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetInt32(ctx, rp.blk, fldpos)
}

func (rp *RecordPage) GetString(ctx context.Context, slot int32, fldname string) (string, error) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetStr(ctx, rp.blk, fldpos)
}

func (rp *RecordPage) SetInt(ctx context.Context, slot int32, fldname string, val int32) error {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.SetInt32(ctx, rp.blk, fldpos, val, true)
}

func (rp *RecordPage) SetStr(ctx context.Context, slot int32, fldname string, val string) error {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.SetStr(ctx, rp.blk, fldpos, val, true)
}

func (rp *RecordPage) Delete(ctx context.Context, slot int32) error {
	return rp.setFlag(ctx, slot, Empty)
}

func (rp *RecordPage) Format(ctx context.Context) {
	slot := int32(0)
	for rp.isValidSlot(slot) {
		rp.tx.SetInt32(ctx, rp.blk, rp.offset(slot), Empty, false)
		sch := rp.layout.Schema()
		for _, fldname := range sch.Fields() {
			fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
			if sch.FldType(fldname) == record.Integer {
				rp.tx.SetInt32(ctx, rp.blk, fldpos, 0, false)
			} else if sch.FldType(fldname) == record.Varchar {
				rp.tx.SetStr(ctx, rp.blk, fldpos, "", false)
			}
		}
	}
}

func (rp *RecordPage) NextAfter(ctx context.Context, slot int32) (int32, error) {
	return rp.searchAfter(ctx, slot, Used)
}

func (rp *RecordPage) InsertAfter(ctx context.Context, slot int32) (int32, error) {
	newslot, err := rp.searchAfter(ctx, slot, Empty)
	if err != nil {
		return -1, fmt.Errorf("searchafter error: %w", err)
	}

	if err := rp.setFlag(ctx, newslot, Used); err != nil {
		return -1, fmt.Errorf("setflag error: %w", err)
	}

	return newslot, nil
}

func (rp *RecordPage) Block() *file.BlockId {
	return rp.blk
}

func (rp *RecordPage) setFlag(ctx context.Context, slot int32, flag int32) error {
	slotpos := rp.offset(slot)
	return rp.tx.SetInt32(ctx, rp.blk, slotpos, flag, true)
}

func (rp *RecordPage) searchAfter(ctx context.Context, slot int32, flag int32) (int32, error) {
	slot++
	for rp.isValidSlot(slot) {
		slotpos := rp.offset(slot)
		slotFlag, err := rp.tx.GetInt32(ctx, rp.blk, slotpos)
		if err != nil {
			return -1, fmt.Errorf("getint32 error: %w", err)
		}
		if slotFlag == flag {
			return slot, nil
		}
		slot++
	}
	return -1, nil
}

func (rp *RecordPage) isValidSlot(slot int32) bool {
	return rp.offset(slot+1) <= rp.tx.Blocksize()
}

func (rp *RecordPage) offset(slot int32) int32 {
	return slot * rp.layout.SlotSize()
}
