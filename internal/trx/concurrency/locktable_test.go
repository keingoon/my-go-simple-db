package concurrency

import (
	"context"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/file"
)

func TestLockTable(t *testing.T) {
	t.Parallel()

	t.Run("LockTable: NewLockTableはnilでない", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		if lt == nil {
			t.Fatal("NewLockTableがnilを返した")
		}
	})

	t.Run("LockTable: SLock(単独)は成功する", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)
		rwaiter := newWaiter(readMode, false, false)

		err := lt.SLock(ctx, blk, rwaiter)
		if err != nil {
			t.Fatalf("SLockが失敗した: %v", err)
		}
	})

	t.Run("LockTable: SLockは複数読者で共存できる", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// 複数回SLockしてもブロックされず成功する（内部のreader count増加はブラックボックスでは観測しない）
		r1 := newWaiter(readMode, false, false)
		r2 := newWaiter(readMode, false, false)
		r3 := newWaiter(readMode, false, false)

		if err := lt.SLock(ctx, blk, r1); err != nil {
			t.Fatalf("r1 SLockが失敗した: %v", err)
		}
		if err := lt.SLock(ctx, blk, r2); err != nil {
			t.Fatalf("r2 SLockが失敗した: %v", err)
		}
		if err := lt.SLock(ctx, blk, r3); err != nil {
			t.Fatalf("r3 SLockが失敗した: %v", err)
		}

		// 読者がいる間はXLockが取れない（タイムアウトする）
		ctxW, cancelW := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancelW()
		if err := lt.XLock(ctxW, blk, newWaiter(writeMode, false, false)); err == nil {
			t.Fatalf("読者がいる間はXLockはタイムアウトすべき")
		}

		// cleanup
		_ = lt.Unlock(ctx, blk, r1)
		_ = lt.Unlock(ctx, blk, r2)
		_ = lt.Unlock(ctx, blk, r3)
	})

	t.Run("LockTable: XLock(単独)は成功する", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)
		wwaiter := newWaiter(writeMode, false, false)

		err := lt.XLock(ctx, blk, wwaiter)
		if err != nil {
			t.Fatalf("XLockが失敗した: %v", err)
		}
	})

	t.Run("LockTable: XLock中はSLockがブロックされる（タイムアウトする）", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		blk := file.NewBlockId("testfile", 0)

		// Acquire write lock
		wwaiter := newWaiter(writeMode, false, false)
		err := lt.XLock(context.Background(), blk, wwaiter)
		if err != nil {
			t.Fatalf("XLockが失敗した: %v", err)
		}

		// SLockはタイムアウトする
		rwaiter := newWaiter(readMode, false, false)
		err = lt.SLock(ctx, blk, rwaiter)
		if err == nil {
			t.Fatal("SLockはタイムアウトすべきだが成功した")
		}
	})

	t.Run("LockTable: SLock中はXLockがブロックされる（タイムアウトする）", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		blk := file.NewBlockId("testfile", 0)

		// Acquire read lock
		rwaiter := newWaiter(readMode, false, false)
		err := lt.SLock(context.Background(), blk, rwaiter)
		if err != nil {
			t.Fatalf("SLockが失敗した: %v", err)
		}

		// XLockはタイムアウトする
		wwaiter := newWaiter(writeMode, false, false)
		err = lt.XLock(ctx, blk, wwaiter)
		if err == nil {
			t.Fatal("XLockはタイムアウトすべきだが成功した")
		}
	})

	t.Run("LockTable: Unlock(SLock)すると後続XLockが成功する", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Acquire read lock
		rwaiter := newWaiter(readMode, false, false)
		err := lt.SLock(ctx, blk, rwaiter)
		if err != nil {
			t.Fatalf("SLockが失敗した: %v", err)
		}

		// Unlock
		err = lt.Unlock(ctx, blk, rwaiter)
		if err != nil {
			t.Fatalf("Unlockが失敗した: %v", err)
		}

		// 後続XLockが成功する
		wwaiter := newWaiter(writeMode, false, false)
		if err := lt.XLock(ctx, blk, wwaiter); err != nil {
			t.Fatalf("Unlock後のXLockは成功すべきだが%vだった", err)
		}
	})

	t.Run("LockTable: Unlock(XLock)すると後続SLockが成功する", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// Acquire write lock
		wwaiter := newWaiter(writeMode, false, false)
		err := lt.XLock(ctx, blk, wwaiter)
		if err != nil {
			t.Fatalf("XLockが失敗した: %v", err)
		}

		// Unlock
		err = lt.Unlock(ctx, blk, wwaiter)
		if err != nil {
			t.Fatalf("Unlockが失敗した: %v", err)
		}

		// 後続SLockが成功する
		rwaiter := newWaiter(readMode, false, false)
		if err := lt.SLock(ctx, blk, rwaiter); err != nil {
			t.Fatalf("Unlock後のSLockは成功すべきだが%vだった", err)
		}
	})

	t.Run("LockTable: Unlock(XLock)すると待機中のSLockが進む", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Acquire write lock
		wwaiter := newWaiter(writeMode, false, false)
		err := lt.XLock(context.Background(), blk, wwaiter)
		if err != nil {
			t.Fatalf("XLockが失敗した: %v", err)
		}

		// Start reader in background
		done := make(chan error, 1)
		go func() {
			rwaiter := newWaiter(readMode, false, false)
			done <- lt.SLock(context.Background(), blk, rwaiter)
		}()

		// Release write lock
		err = lt.Unlock(context.Background(), blk, wwaiter)
		if err != nil {
			t.Fatalf("Unlockが失敗した: %v", err)
		}

		// Wait for reader to complete
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("待機中のSLockは成功すべきだが%vだった", err)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("待機中のSLockが完了しない")
		}
	})

	t.Run("LockTable: Unlock(SLock)すると待機中のXLockが進む", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Acquire read lock
		rwaiter := newWaiter(readMode, false, false)
		err := lt.SLock(context.Background(), blk, rwaiter)
		if err != nil {
			t.Fatalf("SLockが失敗した: %v", err)
		}

		// Start writer in background
		done := make(chan error, 1)
		go func() {
			wwaiter := newWaiter(writeMode, false, false)
			done <- lt.XLock(context.Background(), blk, wwaiter)
		}()

		// Release read lock
		err = lt.Unlock(context.Background(), blk, rwaiter)
		if err != nil {
			t.Fatalf("Unlockが失敗した: %v", err)
		}

		// Wait for writer to complete
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("待機中のXLockは成功すべきだが%vだった", err)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("待機中のXLockが完了しない")
		}
	})

	// Upgrade tests
	t.Run("LockTable: SLockからXLockへアップグレードできる", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		ctx := context.Background()
		blk := file.NewBlockId("testfile", 0)

		// First acquire read lock
		rwaiter := newWaiter(readMode, false, false)
		err := lt.SLock(ctx, blk, rwaiter)
		if err != nil {
			t.Fatalf("SLockが失敗した: %v", err)
		}

		// Now upgrade to write lock
		wwaiter := newWaiter(writeMode, true, false) // upgrade = true, upgraded = false
		err = lt.XLock(ctx, blk, wwaiter)
		if err != nil {
			t.Fatalf("XLock(アップグレード)が失敗した: %v", err)
		}

		// cleanup
		if err := lt.Unlock(ctx, blk, wwaiter); err != nil {
			t.Fatalf("Unlockが失敗した: %v", err)
		}
	})

	t.Run("LockTable: アップグレード中は他のSLockがブロックされる（タイムアウトする）", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// setup: SLock -> XLock(Upgrade)
		rwaiter := newWaiter(readMode, false, false)
		if err := lt.SLock(context.Background(), blk, rwaiter); err != nil {
			t.Fatalf("setup SLockが失敗した: %v", err)
		}
		wwaiter := newWaiter(writeMode, true, false) // upgrade = true, upgraded = false
		if err := lt.XLock(context.Background(), blk, wwaiter); err != nil {
			t.Fatalf("setup XLock(アップグレード)が失敗した: %v", err)
		}

		// アップグレード後は書きロック中なので、新規SLockはタイムアウトする
		ctxR, cancelR := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancelR()
		if err := lt.SLock(ctxR, blk, newWaiter(readMode, false, false)); err == nil {
			t.Fatalf("他のSLockはタイムアウトすべきだが成功した")
		}

		// cleanup
		if err := lt.Unlock(context.Background(), blk, wwaiter); err != nil {
			t.Fatalf("cleanup Unlockが失敗した: %v", err)
		}
	})

	t.Run("LockTable: アップグレード中は他のXLockがブロックされる（タイムアウトする）", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Acquire read lock
		rwaiter := newWaiter(readMode, false, false)
		if err := lt.SLock(context.Background(), blk, rwaiter); err != nil {
			t.Fatalf("SLockが失敗した: %v", err)
		}

		// Start upgrade in background
		wwaiter := newWaiter(writeMode, true, false) // upgrade = true, upgraded = false
		if err := lt.XLock(context.Background(), blk, wwaiter); err != nil {
			t.Fatalf("XLock(アップグレード)が失敗した: %v", err)
		}

		// Try to acquire another write lock (should timeout)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		wwaiter2 := newWaiter(writeMode, false, false)
		if err := lt.XLock(ctx, blk, wwaiter2); err == nil {
			t.Fatalf("他のXLockはタイムアウトすべきだが成功した")
		}

		// cleanup
		if err := lt.Unlock(context.Background(), blk, wwaiter); err != nil {
			t.Fatalf("cleanup Unlockが失敗した: %v", err)
		}
	})

	// Upgrade blocks other reader
	t.Run("LockTable: アップグレード待機中は新規SLockがブロックされる（タイムアウトする）", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		blk := file.NewBlockId("testfile_upgrade_blocks_reader", 0)

		// Hold two readers so upgrade cannot complete (readers==2)
		if err := lt.SLock(context.Background(), blk, newWaiter(readMode, false, false)); err != nil {
			t.Fatalf("r1 SLockが失敗した: %v", err)
		}
		if err := lt.SLock(context.Background(), blk, newWaiter(readMode, false, false)); err != nil {
			t.Fatalf("r2 SLockが失敗した: %v", err)
		}

		// Start an upgrade in background (expected to block and later timeout)
		ctxUp, cancelUp := context.WithTimeout(context.Background(), 200*time.Millisecond)
		u := newWaiter(writeMode, true, false)
		done := make(chan struct{})
		go func() {
			_ = lt.XLock(ctxUp, blk, u)
			close(done)
		}()

		// Give the upgrader a moment to enter waiting/reserved state
		time.Sleep(10 * time.Millisecond)

		// While upgrade is pending, new SLock should time out
		ctxR3, cancelR3 := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancelR3()
		if err := lt.SLock(ctxR3, blk, newWaiter(readMode, false, false)); err == nil {
			t.Fatal("アップグレード待機中はSLockがタイムアウトすべきだが成功した")
		}

		// cleanup: cancel upgrade and wait
		cancelUp()
		<-done
	})

	// Wake-up order tests for Unlock
	t.Run("LockTable: 白箱: Unlockは(readers==0)でwriteを先に起床する", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Hold write lock to force others to queue
		ww := newWaiter(writeMode, false, false)
		if err := lt.XLock(context.Background(), blk, ww); err != nil {
			t.Fatalf("初期XLockが失敗した: %v", err)
		}

		order := make(chan string, 2)

		// Queue read waiter
		go func() {
			if err := lt.SLock(context.Background(), blk, newWaiter(readMode, false, false)); err == nil {
				order <- "R"
			}
		}()

		// Queue write waiter
		go func() {
			if err := lt.XLock(context.Background(), blk, newWaiter(writeMode, false, false)); err == nil {
				order <- "W"
			}
		}()

		// 10ms sleep to ensure waiters are enqueued（安定化が必要なら同期方法に置換する）
		time.Sleep(10 * time.Millisecond)

		// Unlock writer: readers==0, expect upgrade to wake first
		if err := lt.Unlock(context.Background(), blk, ww); err != nil {
			t.Fatalf("Unlockが失敗した: %v", err)
		}

		select {
		case first := <-order:
			if first != "W" {
				t.Errorf("最初の起床は'W'であるべきだが%qだった", first)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("起床を待ってタイムアウトした")
		}
	})

	t.Run("LockTable: 白箱: Unlockは(writer/upgradeなし)で全readerを起床する", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Hold write lock to force readers to queue
		ww := newWaiter(writeMode, false, false)
		if err := lt.XLock(context.Background(), blk, ww); err != nil {
			t.Fatalf("初期XLockが失敗した: %v", err)
		}

		order := make(chan string, 3)

		// Queue multiple readers
		for i := 0; i < 3; i++ {
			go func() {
				if err := lt.SLock(context.Background(), blk, newWaiter(readMode, false, false)); err == nil {
					order <- "R"
				}
			}()
		}

		// 10ms sleep to ensure waiters are enqueued（安定化が必要なら同期方法に置換する）
		time.Sleep(10 * time.Millisecond)

		// Unlock writer: expect all readers to be woken
		if err := lt.Unlock(context.Background(), blk, ww); err != nil {
			t.Fatalf("Unlockが失敗した: %v", err)
		}

		count := 0
		for count < 3 {
			select {
			case <-order:
				count++
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("起床したreaderは3つであるべきだが%dつだった", count)
			}
		}
	})

	t.Run("LockTable: 白箱: Unlock(SLock)は(readers==1)でupgradeをwriteより先に起床する", func(t *testing.T) {
		t.Parallel()
		lt := NewLockTable()
		blk := file.NewBlockId("testfile", 0)

		// Acquire single read lock
		rw := newWaiter(readMode, false, false)
		if err := lt.SLock(context.Background(), blk, rw); err != nil {
			t.Fatalf("初期SLockが失敗した: %v", err)
		}

		order := make(chan string, 1)

		// Queue write waiter while readers==1
		go func() {
			if err := lt.XLock(context.Background(), blk, newWaiter(writeMode, false, false)); err == nil {
				order <- "W"
			}
		}()

		// Queue upgrade waiter while readers==1
		go func() {
			if err := lt.XLock(context.Background(), blk, newWaiter(writeMode, true, false)); err == nil {
				order <- "U"
			}
		}()

		// 10ms sleep to ensure waiters are enqueued（安定化が必要なら同期方法に置換する）
		time.Sleep(10 * time.Millisecond)

		// Unlock the only reader: expect upgrade to be woken
		if err := lt.Unlock(context.Background(), blk, rw); err != nil {
			t.Fatalf("Unlockが失敗した: %v", err)
		}

		select {
		case first := <-order:
			if first != "U" {
				t.Errorf("起床は'U'であるべきだが%qだった", first)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("upgrade起床を待ってタイムアウトした")
		}
	})
}
