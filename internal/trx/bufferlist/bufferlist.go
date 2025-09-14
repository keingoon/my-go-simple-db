package bufferlist

import (
	"context"
	"fmt"
	"slices"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
)

type BufferList struct {
	buffers map[*file.BlockId]*buffer.Buffer
	pins    []*file.BlockId
	bm      *buffer.BufferMgr
}

func NewBufferList(bm *buffer.BufferMgr) *BufferList {
	return &BufferList{
		buffers: make(map[*file.BlockId]*buffer.Buffer),
		pins:    make([]*file.BlockId, 0),
		bm:      bm,
	}
}

func (bl *BufferList) GetBuffer(blk *file.BlockId) *buffer.Buffer {
	return bl.buffers[blk]
}

func (bl *BufferList) Pin(ctx context.Context, blk *file.BlockId) error {
	buff, err := bl.bm.Pin(ctx, blk)
	if err != nil {
		return fmt.Errorf("buffer list could not pin: %w", err)
	}
	bl.buffers[blk] = buff
	bl.pins = append(bl.pins, blk)
	return nil
}

func (bl *BufferList) Unpin(ctx context.Context, blk *file.BlockId) {
	buff := bl.buffers[blk]
	bl.bm.Unpin(ctx, buff)

	bl.pins = slices.DeleteFunc(bl.pins, func(pinnedBlk *file.BlockId) bool {
		return pinnedBlk == blk
	})

	if !slices.Contains(bl.pins, blk) {
		delete(bl.buffers, blk)
	}
}

func (bl *BufferList) UnpinAll(ctx context.Context) {
	for _, pinnedBlk := range bl.pins {
		buff := bl.buffers[pinnedBlk]
		bl.bm.Unpin(ctx, buff)
	}
	bl.buffers = make(map[*file.BlockId]*buffer.Buffer)
	bl.pins = make([]*file.BlockId, 0)
}
