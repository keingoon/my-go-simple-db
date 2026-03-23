package concurrency

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/file"
)

func TestConcurrencyMgr(t *testing.T) {
	t.Run("ConcurrencyMgr: NewConcurrencyMgrはnilでない", func(t *testing.T) {
		cm := NewConcurrencyMgr(NewLockTable())
		if cm == nil {
			t.Fatal("NewConcurrencyMgrがnilを返した")
		}
	})

	t.Run("ConcurrencyMgr: SLockの基本", func(t *testing.T) {
		locktbl := NewLockTable()
		cm := NewConcurrencyMgr(locktbl)
		other := NewConcurrencyMgr(locktbl)
		blk := file.NewBlockId("testfile_slock", 0)

		t.Run("SLockできる", func(t *testing.T) {
			if err := cm.SLock(context.Background(), blk); err != nil {
				t.Fatalf("SLockが失敗した: %v", err)
			}
		})

		t.Run("SLock中は他ConcurrencyMgrのXLockがタイムアウトする", func(t *testing.T) {
			if err := cm.SLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のSLockが失敗した: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
			defer cancel()
			if err := other.XLock(ctx, blk); err == nil {
				t.Fatal("SLock中のXLockはタイムアウトすべきだが成功した")
			}
		})

		t.Run("Release後は他ConcurrencyMgrがXLockできる", func(t *testing.T) {
			if err := cm.SLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のSLockが失敗した: %v", err)
			}
			if err := cm.Release(context.Background()); err != nil {
				t.Fatalf("Releaseが失敗した: %v", err)
			}
			if err := other.XLock(context.Background(), blk); err != nil {
				t.Fatalf("Release後のXLockは成功すべきだが%vだった", err)
			}
		})
	})

	t.Run("ConcurrencyMgr: XLockの基本", func(t *testing.T) {
		locktbl := NewLockTable()
		cm := NewConcurrencyMgr(locktbl)
		other := NewConcurrencyMgr(locktbl)
		blk := file.NewBlockId("testfile_xlock", 0)

		t.Run("XLockできる", func(t *testing.T) {
			if err := cm.XLock(context.Background(), blk); err != nil {
				t.Fatalf("XLockが失敗した: %v", err)
			}
		})

		t.Run("XLock中は他ConcurrencyMgrのSLockがタイムアウトする", func(t *testing.T) {
			if err := cm.XLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のXLockが失敗した: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
			defer cancel()
			if err := other.SLock(ctx, blk); err == nil {
				t.Fatal("XLock中のSLockはタイムアウトすべきだが成功した")
			}
		})

		t.Run("Release後は他ConcurrencyMgrがSLockできる", func(t *testing.T) {
			if err := cm.XLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のXLockが失敗した: %v", err)
			}
			if err := cm.Release(context.Background()); err != nil {
				t.Fatalf("Releaseが失敗した: %v", err)
			}
			if err := other.SLock(context.Background(), blk); err != nil {
				t.Fatalf("Release後のSLockは成功すべきだが%vだった", err)
			}
		})
	})

	t.Run("ConcurrencyMgr: 同一ブロックのシーケンス", func(t *testing.T) {
		t.Run("同一ブロックへのSLockを2回呼んでも成功する（no-op）", func(t *testing.T) {
			cm := NewConcurrencyMgr(NewLockTable())
			blk := file.NewBlockId("testfile_s_to_s", 0)

			if err := cm.SLock(context.Background(), blk); err != nil {
				t.Fatalf("1回目のSLockが失敗した: %v", err)
			}
			if err := cm.SLock(context.Background(), blk); err != nil {
				t.Fatalf("2回目のSLockが失敗した: %v", err)
			}
		})

		t.Run("同一ブロックへのXLock後にSLockを呼んでも成功する（no-op）", func(t *testing.T) {
			cm := NewConcurrencyMgr(NewLockTable())
			blk := file.NewBlockId("testfile_x_to_s", 0)

			if err := cm.XLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のXLockが失敗した: %v", err)
			}
			if err := cm.SLock(context.Background(), blk); err != nil {
				t.Fatalf("XLock後のSLockが失敗した: %v", err)
			}
		})

		t.Run("SLockからXLockへアップグレードできる", func(t *testing.T) {
			cm := NewConcurrencyMgr(NewLockTable())
			blk := file.NewBlockId("testfile_upgrade_same_block", 0)

			if err := cm.SLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のSLockが失敗した: %v", err)
			}
			if err := cm.XLock(context.Background(), blk); err != nil {
				t.Fatalf("XLock(アップグレード)が失敗した: %v", err)
			}
		})

		t.Run("アップグレード後は他ConcurrencyMgrのSLockがタイムアウトする", func(t *testing.T) {
			locktbl := NewLockTable()
			cm := NewConcurrencyMgr(locktbl)
			other := NewConcurrencyMgr(locktbl)
			blk := file.NewBlockId("testfile_upgrade_blocks_reader", 0)

			if err := cm.SLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のSLockが失敗した: %v", err)
			}
			if err := cm.XLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のXLock(アップグレード)が失敗した: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
			defer cancel()
			if err := other.SLock(ctx, blk); err == nil {
				t.Fatal("アップグレード後のSLockはタイムアウトすべきだが成功した")
			}
		})
	})

	t.Run("ConcurrencyMgr: 複数ブロック", func(t *testing.T) {
		t.Run("複数ブロックにSLockできる", func(t *testing.T) {
			cm := NewConcurrencyMgr(NewLockTable())
			blk1 := file.NewBlockId("testfile1", 0)
			blk2 := file.NewBlockId("testfile2", 0)

			if err := cm.SLock(context.Background(), blk1); err != nil {
				t.Fatalf("blk1のSLockが失敗した: %v", err)
			}
			if err := cm.SLock(context.Background(), blk2); err != nil {
				t.Fatalf("blk2のSLockが失敗した: %v", err)
			}
		})

		t.Run("複数ブロックにXLockできる", func(t *testing.T) {
			cm := NewConcurrencyMgr(NewLockTable())
			blk1 := file.NewBlockId("testfile1", 0)
			blk2 := file.NewBlockId("testfile2", 0)

			if err := cm.XLock(context.Background(), blk1); err != nil {
				t.Fatalf("blk1のXLockが失敗した: %v", err)
			}
			if err := cm.XLock(context.Background(), blk2); err != nil {
				t.Fatalf("blk2のXLockが失敗した: %v", err)
			}
		})
	})

	t.Run("ConcurrencyMgr: Unlockは他ConcurrencyMgrの待機を進める", func(t *testing.T) {
		t.Run("Unlock(SLock)後に他ConcurrencyMgrがXLockできる", func(t *testing.T) {
			locktbl := NewLockTable()
			cm1 := NewConcurrencyMgr(locktbl)
			cm2 := NewConcurrencyMgr(locktbl)
			blk := file.NewBlockId("testfile_unlock_s", 0)

			if err := cm1.SLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のSLockが失敗した: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
			defer cancel()
			if err := cm2.XLock(ctx, blk); err == nil {
				t.Fatal("SLock中のXLockはタイムアウトすべきだが成功した")
			}
			if err := cm1.Unlock(context.Background(), blk); err != nil {
				t.Fatalf("Unlockが失敗した: %v", err)
			}
			if err := cm2.XLock(context.Background(), blk); err != nil {
				t.Fatalf("Unlock後のXLockは成功すべきだが%vだった", err)
			}
		})

		t.Run("Unlock(XLock)後に他ConcurrencyMgrがSLockできる", func(t *testing.T) {
			locktbl := NewLockTable()
			cm1 := NewConcurrencyMgr(locktbl)
			cm2 := NewConcurrencyMgr(locktbl)
			blk := file.NewBlockId("testfile_unlock_x", 0)

			if err := cm1.XLock(context.Background(), blk); err != nil {
				t.Fatalf("前提のXLockが失敗した: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
			defer cancel()
			if err := cm2.SLock(ctx, blk); err == nil {
				t.Fatal("XLock中のSLockはタイムアウトすべきだが成功した")
			}
			if err := cm1.Unlock(context.Background(), blk); err != nil {
				t.Fatalf("Unlockが失敗した: %v", err)
			}
			if err := cm2.SLock(context.Background(), blk); err != nil {
				t.Fatalf("Unlock後のSLockは成功すべきだが%vだった", err)
			}
		})
	})

	t.Run("ConcurrencyMgr: 並行XLockは同時に1つだけ成功する", func(t *testing.T) {
		locktbl := NewLockTable()
		blk := file.NewBlockId("testfile_concurrent_xlock", 0)

		const n = 3
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(n)

		results := make(chan error, n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				<-start
				cm := NewConcurrencyMgr(locktbl)
				ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
				defer cancel()
				results <- cm.XLock(ctx, blk)
			}()
		}

		close(start)
		wg.Wait()
		close(results)

		success := 0
		timeout := 0
		for err := range results {
			if err == nil {
				success++
				continue
			}
			timeout++
		}

		if success != 1 {
			t.Fatalf("成功したXLockは1件であるべきだが%d件だった", success)
		}
		if timeout != n-1 {
			t.Fatalf("タイムアウトしたXLockは%d件であるべきだが%d件だった", n-1, timeout)
		}
	})
}

const (
	shortTimeout = 50 * time.Millisecond
)
