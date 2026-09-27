package tablescan

import (
	"context"
	"fmt"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/record"
	"github.com/keingoon/simpledb/internal/recordpage"
	"github.com/keingoon/simpledb/internal/trx/tx"
)

type TableScan struct {
	ctx         context.Context
	tx          *tx.TransactionMgr
	layout      *record.Layout
	rp          *recordpage.RecordPage
	filename    string
	currentslot int32
}

func NewTableScan(ctx context.Context, tx *tx.TransactionMgr, tblname string, layout *record.Layout) (*TableScan, error) {
	filename := tblname + ".tbl"
	ts := &TableScan{
		ctx:      ctx,
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

func (ts *TableScan) close(ctx context.Context) {
	if ts.rp != nil {
		ts.tx.Unpin(ctx, ts.rp.Block())
	}
}

func (ts *TableScan) moveToNewBlock(ctx context.Context) error {
	ts.close(ctx)
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
	ts.close(ctx)
	blk := file.NewBlockId(ts.filename, blknum)
	rp := recordpage.NewRecordPage(ctx, ts.tx, blk, ts.layout)

	ts.rp = rp
	ts.currentslot = -1

	return nil
}
