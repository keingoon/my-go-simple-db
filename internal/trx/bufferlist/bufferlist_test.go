package bufferlist

import (
	"context"
	"testing"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
)

func initBufferList(t *testing.T) (*buffer.BufferMgr, *BufferList) {
	const (
		blocksize = int32(256)
		logfile   = "logfile"
		numbuffs  = 10
		numwaits  = 10
	)

	fileMgr, err := file.NewFileMgr(t.TempDir(), blocksize)
	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}

	logMgr, err := log.NewLogMgr(fileMgr, logfile)
	if err != nil {
		t.Fatalf("Failed to create LogMgr: %v", err)
	}

	bufferMgr := buffer.NewBufferMgr(fileMgr, logMgr, numbuffs, numwaits)
	bufferList := NewBufferList(bufferMgr)

	return bufferMgr, bufferList
}

func TestBufferList(t *testing.T) {
	t.Parallel()

	t.Run("NewBufferList", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)

		if bl == nil {
			t.Fatal("NewBufferList returned nil")
		}

		if bl.bm == nil {
			t.Error("BufferMgr not set correctly")
		}

		if bl.buffers == nil {
			t.Error("buffers map not initialized")
		}

		if bl.pins == nil {
			t.Error("pins slice not initialized")
		}

		if len(bl.buffers) != 0 {
			t.Error("buffers map should be empty initially")
		}

		if len(bl.pins) != 0 {
			t.Error("pins slice should be empty initially")
		}
	})

	t.Run("GetBuffer for non-existent block", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		blk := file.NewBlockId("testfile", 0)

		buff := bl.GetBuffer(blk)
		if buff != nil {
			t.Error("GetBuffer should return nil for non-existent block")
		}
	})

	t.Run("GetBuffer for pinned block", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		buff := bl.GetBuffer(blk)
		if buff == nil {
			t.Error("GetBuffer should return buffer for pinned block")
		}

		if buff.Block() == nil || !buff.Block().Equals(blk) {
			t.Error("Buffer should be associated with the correct block")
		}
	})

	t.Run("Pin single block", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		// Verify block is in buffers map
		buff := bl.buffers[blk]
		if buff == nil {
			t.Error("Buffer should not be nil")
		}

		// Verify block is in pins slice
		found := false
		for _, pinnedBlk := range bl.pins {
			if pinnedBlk == blk {
				found = true
				break
			}
		}
		if !found {
			t.Error("Block should be in pins slice after pinning")
		}
	})

	t.Run("Pin same block multiple times", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Pin the same block twice
		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin same block again: %v", err)
		}

		// Should have two entries in pins slice for the same block
		count := 0
		for _, pinnedBlk := range bl.pins {
			if pinnedBlk == blk {
				count++
			}
		}
		if count != 2 {
			t.Errorf("Expected 2 pins for same block, got %d", count)
		}
	})

	t.Run("Unpin single pin", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Pin a block
		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		// Unpin once
		bl.Unpin(ctx, blk)

		// Should have no pins
		count := 0
		for _, pinnedBlk := range bl.pins {
			if pinnedBlk == blk {
				count++
			}
		}
		if count != 0 {
			t.Errorf("Expected 0 pins after unpin, got %d", count)
		}

		// Buffer should be removed
		if _, exists := bl.buffers[blk]; exists {
			t.Error("Buffer should be removed after unpin")
		}
	})

	t.Run("Unpin multiple pins", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Pin a block twice
		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block again: %v", err)
		}

		// Unpin once
		bl.Unpin(ctx, blk)
		// Unpin again
		bl.Unpin(ctx, blk)

		// Should have no pins
		count := 0
		for _, pinnedBlk := range bl.pins {
			if pinnedBlk == blk {
				count++
			}
		}
		if count != 0 {
			t.Errorf("Expected 0 pins after second unpin, got %d", count)
		}

		// Buffer should be removed
		if _, exists := bl.buffers[blk]; exists {
			t.Error("Buffer should be removed after all unpins")
		}
	})

	t.Run("UnpinAll with multiple blocks", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()

		// Pin multiple blocks
		blk1 := file.NewBlockId("testfile1", 0)
		blk2 := file.NewBlockId("testfile1", 1)
		blk3 := file.NewBlockId("testfile2", 0)

		err := bl.Pin(ctx, blk1)
		if err != nil {
			t.Fatalf("Failed to pin testfile1 block1: %v", err)
		}

		err = bl.Pin(ctx, blk2)
		if err != nil {
			t.Fatalf("Failed to pin testfile1 block2: %v", err)
		}

		err = bl.Pin(ctx, blk3)
		if err != nil {
			t.Fatalf("Failed to pin testfile2 block3: %v", err)
		}

		// Unpin all
		bl.UnpinAll(ctx)

		// Verify all pins and buffers are cleared
		if len(bl.pins) != 0 {
			t.Errorf("Expected 0 pins after UnpinAll, got %d", len(bl.pins))
		}

		if len(bl.buffers) != 0 {
			t.Errorf("Expected 0 buffers after UnpinAll, got %d", len(bl.buffers))
		}
	})

	t.Run("UnpinAll with multiple pins on same block", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Pin the same block multiple times
		err := bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block: %v", err)
		}

		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block again: %v", err)
		}

		err = bl.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("Failed to pin block third time: %v", err)
		}

		// Unpin all
		bl.UnpinAll(ctx)

		// Verify all pins and buffers are cleared
		if len(bl.pins) != 0 {
			t.Errorf("Expected 0 pins after UnpinAll, got %d", len(bl.pins))
		}

		if len(bl.buffers) != 0 {
			t.Errorf("Expected 0 buffers after UnpinAll, got %d", len(bl.buffers))
		}
	})

	t.Run("Unpin non-existent block", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Unpin a block that was never pinned
		// This should not panic or cause errors
		bl.Unpin(ctx, blk)

		// Verify state is still clean
		if len(bl.pins) != 0 {
			t.Errorf("Expected 0 pins, got %d", len(bl.pins))
		}

		if len(bl.buffers) != 0 {
			t.Errorf("Expected 0 buffers, got %d", len(bl.buffers))
		}
	})
}

func TestBufferListConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("Concurrent pin and unpin operations", func(t *testing.T) {
		t.Parallel()

		_, bl := initBufferList(t)
		ctx := context.Background()

		// Test concurrent pinning and unpinning
		done := make(chan bool, 2)

		// Goroutine 1: Pin and unpin block1
		go func() {
			defer func() { done <- true }()
			blk := file.NewBlockId("testfile1", 0)
			for i := 0; i < 10; i++ {
				err := bl.Pin(ctx, blk)
				if err != nil {
					t.Errorf("Failed to pin in goroutine 1: %v", err)
					return
				}
				bl.Unpin(ctx, blk)
			}
		}()

		// Goroutine 2: Pin and unpin block2
		go func() {
			defer func() { done <- true }()
			blk := file.NewBlockId("testfile2", 0)
			for i := 0; i < 10; i++ {
				err := bl.Pin(ctx, blk)
				if err != nil {
					t.Errorf("Failed to pin in goroutine 2: %v", err)
					return
				}
				bl.Unpin(ctx, blk)
			}
		}()

		// Wait for both goroutines to complete
		<-done
		<-done

		// Verify final state is clean
		if len(bl.pins) != 0 {
			t.Errorf("Expected 0 pins after concurrent operations, got %d", len(bl.pins))
		}

		if len(bl.buffers) != 0 {
			t.Errorf("Expected 0 buffers after concurrent operations, got %d", len(bl.buffers))
		}
	})
}
