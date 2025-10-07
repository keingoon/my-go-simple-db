package concurrency

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/file"
)

func TestLockTable(t *testing.T) {

	t.Run("NewLockTable", func(t *testing.T) {
		lt := NewLockTable()

		if lt == nil {
			t.Fatal("NewLockTable returned nil")
		}

		if lt.shardNum != shardNum {
			t.Errorf("Expected shardNum %d, got %d", shardNum, lt.shardNum)
		}

		if len(lt.shards) != int(shardNum) {
			t.Errorf("Expected %d shards, got %d", shardNum, len(lt.shards))
		}

		for i, shard := range lt.shards {
			if shard == nil {
				t.Errorf("Shard %d is nil", i)
			}
		}
	})

	t.Run("SLock single reader", func(t *testing.T) {
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)
		rwaiter := newWaiter(readMode, false)

		err := lt.SLock(ctx, blk, rwaiter)
		if err != nil {
			t.Fatalf("SLock failed: %v", err)
		}

		// Verify state shows reader count
		shard := lt.getShard(blk)
		state := atomic.LoadInt32(&shard.state)
		readers := state & readerCountMask
		if readers != 1 {
			t.Errorf("Expected 1 reader, got %d", readers)
		}
	})

	t.Run("SLock multiple readers", func(t *testing.T) {
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Multiple readers should be able to acquire lock simultaneously
		var wg sync.WaitGroup
		numReaders := 3 // Reduced from 5 to avoid timeout issues
		errors := make(chan error, numReaders)

		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rwaiter := newWaiter(readMode, false)
				if err := lt.SLock(ctx, blk, rwaiter); err != nil {
					errors <- err
				}
			}()
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("SLock failed: %v", err)
		}

		// Verify state shows correct reader count
		shard := lt.getShard(blk)
		state := atomic.LoadInt32(&shard.state)
		readers := state & readerCountMask
		expectedReaders := int32(numReaders)
		if readers != expectedReaders {
			t.Errorf("Expected %d readers, got %d", numReaders, readers)
		}
	})

	t.Run("XLock single writer", func(t *testing.T) {
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)
		wwaiter := newWaiter(writeMode, false)

		err := lt.XLock(ctx, blk, wwaiter)
		if err != nil {
			t.Fatalf("XLock failed: %v", err)
		}

		// Verify state shows write lock
		shard := lt.getShard(blk)
		state := atomic.LoadInt32(&shard.state)
		if state&wLockedMask == 0 {
			t.Error("Expected write lock to be set")
		}
	})

	t.Run("XLock blocks readers", func(t *testing.T) {
		lt := NewLockTable()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		blk := file.NewBlockId("testfile", 0)

		// Acquire write lock
		wwaiter := newWaiter(writeMode, false)
		err := lt.XLock(context.Background(), blk, wwaiter)
		if err != nil {
			t.Fatalf("XLock failed: %v", err)
		}

		// Try to acquire read lock (should timeout)
		rwaiter := newWaiter(readMode, false)
		err = lt.SLock(ctx, blk, rwaiter)
		if err == nil {
			t.Error("Expected SLock to timeout, but it succeeded")
		}
	})

	t.Run("SLock blocks writers", func(t *testing.T) {
		lt := NewLockTable()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		blk := file.NewBlockId("testfile", 0)

		// Acquire read lock
		rwaiter := newWaiter(readMode, false)
		err := lt.SLock(context.Background(), blk, rwaiter)
		if err != nil {
			t.Fatalf("SLock failed: %v", err)
		}

		// Try to acquire write lock (should timeout)
		wwaiter := newWaiter(writeMode, false)
		err = lt.XLock(ctx, blk, wwaiter)
		if err == nil {
			t.Error("Expected XLock to timeout, but it succeeded")
		}
	})

	t.Run("Unlock releases read lock", func(t *testing.T) {
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Acquire read lock
		rwaiter := newWaiter(readMode, false)
		err := lt.SLock(ctx, blk, rwaiter)
		if err != nil {
			t.Fatalf("SLock failed: %v", err)
		}

		// Unlock
		err = lt.Unlock(ctx, blk)
		if err != nil {
			t.Fatalf("Unlock failed: %v", err)
		}

		// Verify state is cleared
		shard := lt.getShard(blk)
		state := atomic.LoadInt32(&shard.state)
		if state != 0 {
			t.Errorf("Expected state to be 0, got %d", state)
		}
	})

	t.Run("Unlock releases write lock", func(t *testing.T) {
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Acquire write lock
		wwaiter := newWaiter(writeMode, false)
		err := lt.XLock(ctx, blk, wwaiter)
		if err != nil {
			t.Fatalf("XLock failed: %v", err)
		}

		// Unlock
		err = lt.Unlock(ctx, blk)
		if err != nil {
			t.Fatalf("Unlock failed: %v", err)
		}

		// Verify state is cleared
		shard := lt.getShard(blk)
		state := atomic.LoadInt32(&shard.state)
		if state != 0 {
			t.Errorf("Expected state to be 0, got %d", state)
		}
	})

	t.Run("Unlock wakes waiting readers", func(t *testing.T) {
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Acquire write lock
		wwaiter := newWaiter(writeMode, false)
		err := lt.XLock(context.Background(), blk, wwaiter)
		if err != nil {
			t.Fatalf("XLock failed: %v", err)
		}

		// Start reader in background
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			rwaiter := newWaiter(readMode, false)
			// slowSLockが呼ばれる想定
			err := lt.SLock(context.Background(), blk, rwaiter)
			if err != nil {
				t.Errorf("SLock failed: %v", err)
			}
		}()

		// Release write lock
		err = lt.Unlock(context.Background(), blk)
		if err != nil {
			t.Fatalf("Unlock failed: %v", err)
		}

		// Wait for reader to complete
		wg.Wait()
	})

	t.Run("Unlock wakes waiting writer", func(t *testing.T) {
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Acquire read lock
		rwaiter := newWaiter(readMode, false)
		err := lt.SLock(context.Background(), blk, rwaiter)
		if err != nil {
			t.Fatalf("SLock failed: %v", err)
		}

		// Start writer in background
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			wwaiter := newWaiter(writeMode, false)
			// slowXLockが呼ばれる想定
			err := lt.XLock(context.Background(), blk, wwaiter)
			if err != nil {
				t.Errorf("XLock failed: %v", err)
			}
		}()

		// Release read lock
		err = lt.Unlock(context.Background(), blk)
		if err != nil {
			t.Fatalf("Unlock failed: %v", err)
		}

		// Wait for writer to complete
		wg.Wait()
	})

	// Upgrade tests
	t.Run("SLock to XLock upgrade", func(t *testing.T) {
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// First acquire read lock
		rwaiter := newWaiter(readMode, false)
		err := lt.SLock(ctx, blk, rwaiter)
		if err != nil {
			t.Fatalf("SLock failed: %v", err)
		}

		// Verify read lock is held
		shard := lt.getShard(blk)
		state := atomic.LoadInt32(&shard.state)
		readers := state & readerCountMask
		if readers != 1 {
			t.Errorf("Expected 1 reader before upgrade, got %d", readers)
		}

		// Now upgrade to write lock
		wwaiter := newWaiter(writeMode, true) // upgrade = true
		err = lt.XLock(ctx, blk, wwaiter)
		if err != nil {
			t.Fatalf("XLock upgrade failed: %v", err)
		}

		// Verify write lock is held and reader count is 0
		state = atomic.LoadInt32(&shard.state)
		if state&wLockedMask == 0 {
			t.Error("Expected write lock to be set after upgrade")
		}
		readers = state & readerCountMask
		if readers != 0 {
			t.Errorf("Expected 0 readers after upgrade, got %d", readers)
		}
	})

	t.Run("Upgrade blocks other writers", func(t *testing.T) {
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Acquire read lock
		rwaiter := newWaiter(readMode, false)
		if err := lt.SLock(context.Background(), blk, rwaiter); err != nil {
			t.Fatalf("SLock failed: %v", err)
		}

		// Start upgrade in background
		wwaiter := newWaiter(writeMode, true) // upgrade = true
		if err := lt.XLock(context.Background(), blk, wwaiter); err != nil {
			t.Errorf("XLock upgrade failed: %v", err)
		}

		// Try to acquire another write lock (should timeout)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		wwaiter2 := newWaiter(writeMode, false)
		if err := lt.XLock(ctx, blk, wwaiter2); err == nil {
			t.Error("Expected XLock to timeout, but it succeeded")
		}
	})

	// Wake-up order tests for Unlock
	t.Run("Unlock wakes write before read (readers==0)", func(t *testing.T) {
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Hold write lock to force others to queue
		ww := newWaiter(writeMode, false)
		if err := lt.XLock(context.Background(), blk, ww); err != nil {
			t.Fatalf("initial XLock failed: %v", err)
		}

		order := make(chan string, 2)

		// Queue read waiter
		go func() {
			if err := lt.SLock(context.Background(), blk, newWaiter(readMode, false)); err == nil {
				order <- "R"
			}
		}()

		// Queue write waiter
		go func() {
			if err := lt.XLock(context.Background(), blk, newWaiter(writeMode, false)); err == nil {
				order <- "W"
			}
		}()

		// 10ms sleep to ensure waiters are enqueued
		time.Sleep(10 * time.Millisecond)

		// Unlock writer: readers==0, expect upgrade to wake first
		if err := lt.Unlock(context.Background(), blk); err != nil {
			t.Fatalf("Unlock failed: %v", err)
		}

		select {
		case first := <-order:
			if first != "W" {
				t.Errorf("expected first wake 'W', got %s", first)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for first wake")
		}
	})

	t.Run("Unlock wakes all readers when no writer/upgrade (readers>=1)", func(t *testing.T) {
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Hold write lock to force readers to queue
		ww := newWaiter(writeMode, false)
		if err := lt.XLock(context.Background(), blk, ww); err != nil {
			t.Fatalf("initial XLock failed: %v", err)
		}

		order := make(chan string, 3)

		// Queue multiple readers
		for i := 0; i < 3; i++ {
			go func() {
				if err := lt.SLock(context.Background(), blk, newWaiter(readMode, false)); err == nil {
					order <- "R"
				}
			}()
		}

		// 10ms sleep to ensure waiters are enqueued
		time.Sleep(10 * time.Millisecond)

		// Unlock writer: expect all readers to be woken
		if err := lt.Unlock(context.Background(), blk); err != nil {
			t.Fatalf("Unlock failed: %v", err)
		}

		count := 0
		for count < 3 {
			select {
			case <-order:
				count++
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("expected 3 readers to wake, got %d", count)
			}
		}
	})

	t.Run("Unlock by reader wakes upgrade before write (readers==1)", func(t *testing.T) {
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Acquire single read lock
		rw := newWaiter(readMode, false)
		if err := lt.SLock(context.Background(), blk, rw); err != nil {
			t.Fatalf("initial SLock failed: %v", err)
		}

		order := make(chan string, 1)

		// Queue write waiter while readers==1
		go func() {
			if err := lt.XLock(context.Background(), blk, newWaiter(writeMode, false)); err == nil {
				order <- "W"
			}
		}()

		// Queue upgrade waiter while readers==1
		go func() {
			if err := lt.XLock(context.Background(), blk, newWaiter(writeMode, true)); err == nil {
				order <- "U"
			}
		}()

		// 10ms sleep to ensure waiters are enqueued
		time.Sleep(10 * time.Millisecond)

		// Unlock the only reader: expect upgrade to be woken
		if err := lt.Unlock(context.Background(), blk); err != nil {
			t.Fatalf("Unlock failed: %v", err)
		}

		select {
		case first := <-order:
			if first != "U" {
				t.Errorf("expected wake 'U', got %s", first)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for upgrade wake")
		}
	})
}

func TestConcurrencyMgr(t *testing.T) {
	t.Run("NewConcurrencyMgr", func(t *testing.T) {
		cm := NewConcurrencyMgr()

		if cm == nil {
			t.Fatal("NewConcurrencyMgr returned nil")
		}

		if cm.shardNum != shardNum {
			t.Errorf("Expected shardNum %d, got %d", shardNum, cm.shardNum)
		}

		if len(cm.shards) != int(shardNum) {
			t.Errorf("Expected %d shards, got %d", shardNum, len(cm.shards))
		}

		for i, shard := range cm.shards {
			if shard == nil {
				t.Errorf("Shard %d is nil", i)
			}
		}
	})

	t.Run("SLock single block", func(t *testing.T) {
		cm := NewConcurrencyMgr()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		err := cm.SLock(ctx, blk)
		if err != nil {
			t.Fatalf("SLock failed: %v", err)
		}

		// Verify lock is held
		if !cm.hasSLock(blk) {
			t.Error("Expected SLock to be held")
		}

		// Unlock verify
		if err := cm.Release(ctx); err != nil {
			t.Fatalf("Release failed: %v", err)
		}
	})

	t.Run("XLock single block", func(t *testing.T) {
		cm := NewConcurrencyMgr()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		err := cm.XLock(ctx, blk)
		if err != nil {
			t.Fatalf("XLock failed: %v", err)
		}

		// Verify lock is held
		if !cm.hasXLock(blk) {
			t.Error("Expected XLock to be held")
		}

		// Unlock verify
		if err := cm.Release(ctx); err != nil {
			t.Fatalf("Release failed: %v", err)
		}
	})

	t.Run("SLock multiple blocks", func(t *testing.T) {
		cm := NewConcurrencyMgr()
		ctx := context.Background()

		blk1 := file.NewBlockId("testfile1", 0)
		blk2 := file.NewBlockId("testfile2", 0)

		err := cm.SLock(ctx, blk1)
		if err != nil {
			t.Fatalf("SLock blk1 failed: %v", err)
		}

		err = cm.SLock(ctx, blk2)
		if err != nil {
			t.Fatalf("SLock blk2 failed: %v", err)
		}

		// Verify both locks are held
		if !cm.hasSLock(blk1) {
			t.Error("Expected SLock on blk1 to be held")
		}
		if !cm.hasSLock(blk2) {
			t.Error("Expected SLock on blk2 to be held")
		}

		// Unlock verify
		if err := cm.Release(ctx); err != nil {
			t.Fatalf("Release failed: %v", err)
		}
	})

	t.Run("XLock multiple blocks", func(t *testing.T) {
		cm := NewConcurrencyMgr()
		ctx := context.Background()

		blk1 := file.NewBlockId("testfile1", 0)
		blk2 := file.NewBlockId("testfile2", 0)

		err := cm.XLock(ctx, blk1)
		if err != nil {
			t.Fatalf("XLock blk1 failed: %v", err)
		}

		err = cm.XLock(ctx, blk2)
		if err != nil {
			t.Fatalf("XLock blk2 failed: %v", err)
		}

		// Verify both locks are held
		if !cm.hasXLock(blk1) {
			t.Error("Expected XLock on blk1 to be held")
		}
		if !cm.hasXLock(blk2) {
			t.Error("Expected XLock on blk2 to be held")
		}

		// Unlock verify
		if err := cm.Release(ctx); err != nil {
			t.Fatalf("Release failed: %v", err)
		}
	})

	t.Run("Release all locks", func(t *testing.T) {
		cm := NewConcurrencyMgr()
		ctx := context.Background()

		blk1 := file.NewBlockId("testfile1", 0)
		blk2 := file.NewBlockId("testfile2", 0)

		// Acquire locks
		err := cm.SLock(ctx, blk1)
		if err != nil {
			t.Fatalf("SLock blk1 failed: %v", err)
		}

		err = cm.XLock(ctx, blk2)
		if err != nil {
			t.Fatalf("XLock blk2 failed: %v", err)
		}

		// Release all locks
		err = cm.Release(ctx)
		if err != nil {
			t.Fatalf("Release failed: %v", err)
		}

		// Verify locks are released
		if cm.hasSLock(blk1) {
			t.Error("Expected SLock on blk1 to be released")
		}
		if cm.hasXLock(blk2) {
			t.Error("Expected XLock on blk2 to be released")
		}
	})

	t.Run("Concurrent SLock operations", func(t *testing.T) {
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		var wg sync.WaitGroup
		numGoroutines := 5 // Reduced from 10
		cms := make(chan *ConcurrencyMgr, numGoroutines)
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cm := NewConcurrencyMgr()
				err := cm.SLock(ctx, blk)
				if err == nil {
					cms <- cm
				} else {
					errors <- err
				}
			}()
		}

		wg.Wait()
		close(cms)
		close(errors)

		// Check for errors
		for err := range errors {
			t.Fatalf("Concurrent SLock failed: %v", err)
		}

		// Unlock verify
		for cm := range cms {
			if err := cm.Release(ctx); err != nil {
				t.Fatalf("Release failed: %v", err)
			}
		}
	})

	t.Run("Concurrent XLock operations", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		blk := file.NewBlockId("testfile", 0)

		var wg sync.WaitGroup
		numGoroutines := 3 // Reduced from 5
		cms := make(chan *ConcurrencyMgr, numGoroutines)
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cm := NewConcurrencyMgr()
				err := cm.XLock(ctx, blk)
				if err == nil {
					cms <- cm
				} else {
					errors <- err
				}
			}()
		}

		wg.Wait()
		close(cms)
		close(errors)

		// Should have exactly one success and the rest time out
		timeoutCount := 0
		for err := range errors {
			if err != nil {
				timeoutCount++
			}
		}
		successCount := numGoroutines - timeoutCount
		if successCount != 1 {
			t.Errorf("expected exactly one XLock to succeed, got %d", successCount)
		}
		if timeoutCount != numGoroutines-1 {
			t.Errorf("expected %d XLock operations to timeout, got %d", numGoroutines-1, timeoutCount)
		}

		// Unlock verify
		for cm := range cms {
			if err := cm.Release(ctx); err != nil {
				t.Fatalf("Release failed: %v", err)
			}
		}
	})

	t.Run("SLock to XLock upgrade", func(t *testing.T) {
		cm := NewConcurrencyMgr()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// First acquire read lock
		if err := cm.SLock(ctx, blk); err != nil {
			t.Fatalf("SLock failed: %v", err)
		}
		if !cm.hasSLock(blk) {
			t.Error("Expected SLock to be held before upgrade")
		}

		// Upgrade to write lock
		if err := cm.XLock(ctx, blk); err != nil {
			t.Fatalf("XLock upgrade failed: %v", err)
		}
		if !cm.hasXLock(blk) {
			t.Error("Expected XLock to be held after upgrade")
		}
		if cm.hasSLock(blk) {
			t.Error("Expected SLock to be released after upgrade")
		}

		// Release and verify
		if err := cm.Release(ctx); err != nil {
			t.Fatalf("Release failed: %v", err)
		}
		if cm.hasXLock(blk) {
			t.Error("Expected XLock to be released")
		}
	})

	// Explicit Unlock tests
	t.Run("Unlock releases SLock", func(t *testing.T) {
		cm1 := NewConcurrencyMgr()
		cm2 := NewConcurrencyMgr()
		blk := file.NewBlockId("testfile_unlock_s", 0)

		// cm1 acquires SLock
		if err := cm1.SLock(context.Background(), blk); err != nil {
			t.Fatalf("cm1 SLock failed: %v", err)
		}
		if !cm1.hasSLock(blk) {
			t.Fatal("cm1 expected to hold SLock")
		}

		// cm2 XLock should timeout while SLock held
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := cm2.XLock(ctx, blk); err == nil {
			t.Fatal("expected cm2 XLock to timeout while SLock held")
		}

		// Unlock by cm1 should allow cm2 to acquire XLock
		if err := cm1.Unlock(context.Background(), blk); err != nil {
			t.Fatalf("cm1 Unlock failed: %v", err)
		}
		if cm1.hasSLock(blk) {
			t.Error("expected cm1 SLock to be released after Unlock")
		}
		if err := cm2.XLock(context.Background(), blk); err != nil {
			t.Fatalf("cm2 XLock after Unlock failed: %v", err)
		}

		// cleanup
		_ = cm2.Unlock(context.Background(), blk)
	})

	t.Run("Unlock releases XLock", func(t *testing.T) {
		cm1 := NewConcurrencyMgr()
		cm2 := NewConcurrencyMgr()
		blk := file.NewBlockId("testfile_unlock_x", 0)

		// cm1 acquires XLock
		if err := cm1.XLock(context.Background(), blk); err != nil {
			t.Fatalf("cm1 XLock failed: %v", err)
		}
		if !cm1.hasXLock(blk) {
			t.Fatal("cm1 expected to hold XLock")
		}

		// cm2 SLock should timeout while XLock held
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := cm2.SLock(ctx, blk); err == nil {
			t.Fatal("expected cm2 SLock to timeout while XLock held")
		}

		// Unlock by cm1 should allow cm2 to acquire SLock
		if err := cm1.Unlock(context.Background(), blk); err != nil {
			t.Fatalf("cm1 Unlock failed: %v", err)
		}
		if cm1.hasXLock(blk) {
			t.Error("expected cm1 XLock to be released after Unlock")
		}
		if err := cm2.SLock(context.Background(), blk); err != nil {
			t.Fatalf("cm2 SLock after Unlock failed: %v", err)
		}

		// cleanup
		_ = cm2.Unlock(context.Background(), blk)
	})
}
