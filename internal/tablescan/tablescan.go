package tablescan

import (
	"context"
	"fmt"
	"time"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/record"
	"github.com/keingoon/simpledb/internal/recordpage"
	"github.com/keingoon/simpledb/internal/trx/tx"
)

type TableScan struct {
	tx          *tx.TransactionMgr
	layout      *record.Layout
	rp          *recordpage.RecordPage
	filename    string
	currentslot int32
}

func NewTableScan(ctx context.Context, tx *tx.TransactionMgr, tblname string, layout *record.Layout) (*TableScan, error) {
	filename := tblname + ".tbl"
	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		filename: filename,
	}
	size, err := ts.tx.Size(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("could not get size: %w", err)
	}
	if size == 0 {
		if err := ts.moveToNewBlock(ctx); err != nil {
			return nil, fmt.Errorf("could not moveToNewBlock: %w", err)
		}
	} else {
		if err := ts.moveToBlock(ctx, 0); err != nil {
			return nil, fmt.Errorf("could not moveToBlock: %w", err)
		}
	}
	return ts, nil
}

func (ts *TableScan) GetRid() *record.RID {
	return record.NewRID(ts.rp.Block().Number(), ts.currentslot)
}

func (ts *TableScan) MoveToRid(ctx context.Context, rid *record.RID) {
	ts.Close(ctx)
	blk := file.NewBlockId(ts.filename, rid.BlockNumber())
	ts.rp = recordpage.NewRecordPage(ctx, ts.tx, blk, ts.layout)
	ts.currentslot = rid.Slot()
}

func (ts *TableScan) HasField(fldname string) bool {
	return ts.layout.Schema().HasField(fldname)
}

func (ts *TableScan) Close(ctx context.Context) {
	if ts.rp != nil {
		ts.tx.Unpin(ctx, ts.rp.Block())
	}
}

func (ts *TableScan) SetInt(ctx context.Context, fldname string, val int32) error {
	return ts.rp.SetInt(ctx, ts.currentslot, fldname, val)
}

func (ts *TableScan) GetInt(ctx context.Context, fldname string) (int32, error) {
	return ts.rp.GetInt(ctx, ts.currentslot, fldname)
}

func (ts *TableScan) SetSmallInt(ctx context.Context, fldname string, val int16) error {
	return ts.rp.SetSmallInt(ctx, ts.currentslot, fldname, val)
}

func (ts *TableScan) GetSmallInt(ctx context.Context, fldname string) (int16, error) {
	return ts.rp.GetSmallInt(ctx, ts.currentslot, fldname)
}

func (ts *TableScan) SetBool(ctx context.Context, fldname string, val bool) error {
	return ts.rp.SetBool(ctx, ts.currentslot, fldname, val)
}

func (ts *TableScan) GetBool(ctx context.Context, fldname string) (bool, error) {
	return ts.rp.GetBool(ctx, ts.currentslot, fldname)
}

func (ts *TableScan) SetDate(ctx context.Context, fldname string, val time.Time) error {
	return ts.rp.SetDate(ctx, ts.currentslot, fldname, val)
}

func (ts *TableScan) GetDate(ctx context.Context, fldname string) (time.Time, error) {
	return ts.rp.GetDate(ctx, ts.currentslot, fldname)
}

func (ts *TableScan) SetString(ctx context.Context, fldname string, val string) error {
	return ts.rp.SetStr(ctx, ts.currentslot, fldname, val)
}

func (ts *TableScan) GetString(ctx context.Context, fldname string) (string, error) {
	return ts.rp.GetString(ctx, ts.currentslot, fldname)
}

func (ts *TableScan) Insert(ctx context.Context) error {
	var err error
	ts.currentslot, err = ts.rp.InsertAfter(ctx, ts.currentslot)
	if err != nil {
		return fmt.Errorf("could not insert: %w", err)
	}
	for ts.currentslot < 0 {
		isAtLastBlk, err := ts.atLastBlock(ctx)
		if err != nil {
			return fmt.Errorf("could not get atLastBlock: %w", err)
		}
		if isAtLastBlk {
			if err := ts.moveToNewBlock(ctx); err != nil {
				return fmt.Errorf("could not moveToNewBlock: %w", err)
			}
		} else {
			if err := ts.moveToBlock(ctx, ts.rp.Block().Number()+1); err != nil {
				return fmt.Errorf("could not moveToBlock: %w", err)
			}
		}
		ts.currentslot, err = ts.rp.InsertAfter(ctx, ts.currentslot)
		if err != nil {
			return fmt.Errorf("could not insert: %w", err)
		}
	}
	return nil
}

func (ts *TableScan) Delete(ctx context.Context) error {
	return ts.rp.Delete(ctx, ts.currentslot)
}

func (ts *TableScan) BeforeFirst(ctx context.Context) error {
	return ts.moveToBlock(ctx, 0)
}

func (ts *TableScan) Next(ctx context.Context) (bool, error) {
	var err error
	ts.currentslot, err = ts.rp.NextAfter(ctx, ts.currentslot)
	if err != nil {
		return false, fmt.Errorf("could not next: %w", err)
	}

	for ts.currentslot < 0 {
		isAtLastBlk, err := ts.atLastBlock(ctx)
		if err != nil {
			return false, fmt.Errorf("could not get atLastBlock: %w", err)
		}
		if isAtLastBlk {
			return false, nil
		}
		if err := ts.moveToBlock(ctx, ts.rp.Block().Number()+1); err != nil {
			return false, fmt.Errorf("could not moveToBlock: %w", err)
		}
		ts.currentslot, err = ts.rp.NextAfter(ctx, ts.currentslot)
		if err != nil {
			return false, fmt.Errorf("could not next: %w", err)
		}
	}

	return true, nil
}

func (ts *TableScan) moveToNewBlock(ctx context.Context) error {
	ts.Close(ctx)
	blk, err := ts.tx.Append(ctx, ts.filename)
	if err != nil {
		return fmt.Errorf("could not append: %w", err)
	}
	rp := recordpage.NewRecordPage(ctx, ts.tx, blk, ts.layout)
	rp.Format(ctx)

	ts.rp = rp
	ts.currentslot = -1

	return nil
}

func (ts *TableScan) moveToBlock(ctx context.Context, blknum int32) error {
	ts.Close(ctx)
	blk := file.NewBlockId(ts.filename, blknum)
	rp := recordpage.NewRecordPage(ctx, ts.tx, blk, ts.layout)

	ts.rp = rp
	ts.currentslot = -1

	return nil
}

func (ts *TableScan) atLastBlock(ctx context.Context) (bool, error) {
	size, err := ts.tx.Size(ctx, ts.filename)
	if err != nil {
		return false, fmt.Errorf("could not get size: %w", err)
	}
	return ts.rp.Block().Number() == size-1, nil
}
