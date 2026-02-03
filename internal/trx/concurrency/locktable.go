package concurrency

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keingoon/simpledb/internal/file"
)

const (
	maxtime  = 10000
	shardNum = 128
	// W=bit31, U=bit30, Readers=bits[0..29]
	wLockedMask     = int32(^(1<<31 - 1))
	upgradeMask     = int32(1 << 30)
	readerCountMask = int32((1 << 30) - 1)
	maxSpinCount    = 30
)

type LockMode int8

const (
	readMode LockMode = iota
	writeMode
)

type waiter struct {
	ch         chan struct{}
	mode       LockMode
	upgrade    bool
	upgraded   bool
	prev, next *waiter
}

func newWaiter(mode LockMode, upgrade bool, upgraded bool) *waiter {
	return &waiter{ch: make(chan struct{}), mode: mode, upgrade: upgrade, upgraded: upgraded, prev: nil, next: nil}
}

type lockShard struct {
	state       int32 // first 1bit is write/read flag, second 1bit is upgrade flag, last 31bits are reader count
	rHead       *waiter
	rTail       *waiter
	wHead       *waiter
	wTail       *waiter
	upgradeHead *waiter
	upgradeTail *waiter
	mu          sync.Mutex
}

func newLockShard() *lockShard {
	return &lockShard{state: 0, rHead: nil, rTail: nil, wHead: nil, wTail: nil, upgradeHead: nil, upgradeTail: nil}
}

func (shard *lockShard) enqueueRead(w *waiter) {
	if shard.rTail == nil {
		shard.rHead = w
		shard.rTail = w
	} else {
		w.prev = shard.rTail
		shard.rTail.next = w
		shard.rTail = w
	}
}

func (shard *lockShard) enqueueWrite(w *waiter) {
	if shard.wTail == nil {
		shard.wHead = w
		shard.wTail = w
	} else {
		w.prev = shard.wTail
		shard.wTail.next = w
		shard.wTail = w
	}
}

func (shard *lockShard) enqueueUpgrade(w *waiter) {
	if shard.upgradeTail == nil {
		shard.upgradeHead = w
		shard.upgradeTail = w
	} else {
		w.prev = shard.upgradeTail
		shard.upgradeTail.next = w
		shard.upgradeTail = w
	}
}

func (shard *lockShard) enqueueUpgradeFront(w *waiter) {
	// ensure clean links
	w.prev, w.next = nil, nil
	if shard.upgradeHead == nil {
		shard.upgradeHead = w
		shard.upgradeTail = w
		return
	}
	w.next = shard.upgradeHead
	shard.upgradeHead.prev = w
	shard.upgradeHead = w
}

func (shard *lockShard) dequeueRead(w *waiter) {
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		shard.rHead = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		shard.rTail = w.prev
	}
	w.prev, w.next = nil, nil
}

func (shard *lockShard) dequeueWrite(w *waiter) {
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		shard.wHead = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		shard.wTail = w.prev
	}
	w.prev, w.next = nil, nil
}

func (shard *lockShard) dequeueUpgrade(w *waiter) {
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		shard.upgradeHead = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		shard.upgradeTail = w.prev
	}
	w.prev, w.next = nil, nil
}

func (shard *lockShard) wakeAllReadWaiters() []*waiter {
	rWaiters := make([]*waiter, 0)
	for rw := shard.rHead; rw != nil; {
		next := rw.next
		close(rw.ch)
		shard.dequeueRead(rw)
		rWaiters = append(rWaiters, rw)
		rw = next
	}
	return rWaiters
}

func (shard *lockShard) wakeWriteWaiter() *waiter {
	writeWaiter := shard.wHead
	if writeWaiter == nil {
		return nil
	}
	close(writeWaiter.ch)
	shard.dequeueWrite(writeWaiter)
	return writeWaiter
}

func (shard *lockShard) wakeUpgradeWaiter() *waiter {
	upgradeWaiter := shard.upgradeHead
	if upgradeWaiter == nil {
		return nil
	}
	close(upgradeWaiter.ch)
	shard.dequeueUpgrade(upgradeWaiter)
	return upgradeWaiter
}

type LockTable struct {
	shards       []*lockShard
	shardNum     int32
	maxtime      int32
	maxSpinCount int32
}

func NewLockTable() *LockTable {
	shards := make([]*lockShard, shardNum)
	for i := 0; i < shardNum; i++ {
		shards[i] = newLockShard()
	}
	return &LockTable{shards, shardNum, maxtime, maxSpinCount}
}

func (locktbl *LockTable) getShard(blk *file.BlockId) *lockShard {
	h := fnvHash(blk.ToString())
	shardIndex := int32(h % uint32(locktbl.shardNum))
	shard := locktbl.shards[shardIndex]
	return shard
}

func fnvHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (locktbl *LockTable) SLock(ctx context.Context, blk *file.BlockId, rwaiter *waiter) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(locktbl.maxtime)*time.Millisecond)
	defer cancel()

	lockShard := locktbl.getShard(blk)

	for {
		// fast path
		for i := 0; i < int(locktbl.maxSpinCount); i++ {
			// タイムアウトしたらエラーを返す
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			old := atomic.LoadInt32(&lockShard.state)
			// block SLock when W lock is held or an upgrade is reserved
			if (old&wLockedMask) != 0 || (old&upgradeMask) != 0 {
				continue
			}
			new := old + 1
			if atomic.CompareAndSwapInt32(&lockShard.state, old, new) {
				return nil
			}
		}

		// slow path
		if err := locktbl.slowSLock(ctx, blk, rwaiter); err != nil {
			return err
		}
	}
}

func (locktbl *LockTable) slowSLock(ctx context.Context, blk *file.BlockId, rwaiter *waiter) error {
	lockShard := locktbl.getShard(blk)

	lockShard.mu.Lock()
	lockShard.enqueueRead(rwaiter)
	lockShard.mu.Unlock()
	select {
	case <-ctx.Done():
		lockShard.mu.Lock()
		lockShard.dequeueRead(rwaiter)
		lockShard.mu.Unlock()
		return ctx.Err()
	case <-rwaiter.ch:
		return nil
	}
}

func (locktbl *LockTable) XLock(ctx context.Context, blk *file.BlockId, wwaiter *waiter) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(locktbl.maxtime)*time.Millisecond)
	defer cancel()

	lockShard := locktbl.getShard(blk)

	for {
		// fast path
		for i := 0; i < int(locktbl.maxSpinCount); i++ {
			// タイムアウトしたらエラーを返す
			select {
			case <-ctx.Done():
				// upgrade待ちがある場合は、CASでupgradeビットをクリアする
				if wwaiter.upgrade && wwaiter.upgraded {
					for {
						cur := atomic.LoadInt32(&lockShard.state)
						if (cur & upgradeMask) == 0 {
							break
						}
						next := cur &^ upgradeMask
						if atomic.CompareAndSwapInt32(&lockShard.state, cur, next) {
							break
						}
					}
					wwaiter.upgraded = false
				}
				return ctx.Err()
			default:
			}
			old := atomic.LoadInt32(&lockShard.state)
			locked := old & wLockedMask
			upgrade := old & upgradeMask
			readers := old & readerCountMask

			// upgradeの場合
			if wwaiter.upgrade && !wwaiter.upgraded {
				if upgrade != 0 {
					continue
				}
				new := old | upgradeMask
				if !atomic.CompareAndSwapInt32(&lockShard.state, old, new) {
					continue
				}
				wwaiter.upgraded = true
			}

			if locked != 0 {
				continue
			}

			new := old | wLockedMask
			if wwaiter.upgrade {
				if readers != 1 {
					continue
				}
				if wwaiter.upgraded {
					new -= 1
					new &= ^upgradeMask
				}
			} else {
				if readers > 0 {
					continue
				}
			}
			if atomic.CompareAndSwapInt32(&lockShard.state, old, new) {
				return nil
			}
		}

		// slow path
		if err := locktbl.slowXLock(ctx, blk, wwaiter); err != nil {
			return err
		}
	}
}

func (locktbl *LockTable) slowXLock(ctx context.Context, blk *file.BlockId, wwaiter *waiter) error {
	lockShard := locktbl.getShard(blk)

	lockShard.mu.Lock()
	if wwaiter.upgrade {
		if wwaiter.upgraded {
			// 予約済みアップグレードはupgrade列の先頭に配置
			lockShard.enqueueUpgradeFront(wwaiter)
		} else {
			// 予約未取得は通常のupgrade列末尾へ
			lockShard.enqueueUpgrade(wwaiter)
		}
	} else {
		// 通常のwrite待機
		lockShard.enqueueWrite(wwaiter)
	}
	lockShard.mu.Unlock()

	select {
	case <-ctx.Done():
		lockShard.mu.Lock()
		if wwaiter.upgrade {
			lockShard.dequeueUpgrade(wwaiter)
		} else {
			lockShard.dequeueWrite(wwaiter)
		}
		// upgrade待ちがある場合は、CASでupgradeビットをクリアする
		if wwaiter.upgrade && wwaiter.upgraded {
			for {
				cur := atomic.LoadInt32(&lockShard.state)
				if (cur & upgradeMask) == 0 {
					break
				}
				next := cur &^ upgradeMask
				if atomic.CompareAndSwapInt32(&lockShard.state, cur, next) {
					break
				}
			}
			wwaiter.upgraded = false
		}
		lockShard.mu.Unlock()
		return ctx.Err()
	case <-wwaiter.ch:
		return nil
	}
}

func (locktbl *LockTable) Unlock(ctx context.Context, blk *file.BlockId, owner *waiter) error {
	lockShard := locktbl.getShard(blk)
	old := atomic.LoadInt32(&lockShard.state)
	prevLocked := old & wLockedMask

	var updated int32
	if prevLocked != 0 {
		new := (old &^ wLockedMask)
		// write中は他がstateを変更しない前提のため、in-place更新
		atomic.StoreInt32(&lockShard.state, new)
		updated = new
	} else {
		// readしてる時はread数だけatomic減算の戻り値を利用
		updated = atomic.AddInt32(&lockShard.state, -1)
	}

	lockShard.mu.Lock()
	// 譲渡ポリシー:
	// - writeビットが落ちているときのみ誰かを起こす
	// - readers==1 かつ upgrade待機がある場合は upgrade を優先的に付与
	// - readers==0 の場合は write → read の順
	// - readers>=1 の場合は read を起こす（スループット重視）
	if updated&wLockedMask == 0 {
		// Safety: 自分の予約（owner.upgraded）だけをクリア
		if owner != nil && owner.upgrade && owner.upgraded {
			for {
				cur := atomic.LoadInt32(&lockShard.state)
				if (cur & upgradeMask) == 0 {
					break
				}
				next := cur &^ upgradeMask
				if atomic.CompareAndSwapInt32(&lockShard.state, cur, next) {
					break
				}
			}
			owner.upgraded = false
		}
		// 最優先: 直前が単一読者で upgrade 待ちがいる
		readers := updated & readerCountMask
		if readers == 0 {
			if ww := lockShard.wakeWriteWaiter(); ww == nil {
				lockShard.wakeAllReadWaiters()
			}
		} else if readers == 1 && lockShard.upgradeHead != nil {
			lockShard.wakeUpgradeWaiter()
		} else {
			lockShard.wakeAllReadWaiters()
		}
	}
	lockShard.mu.Unlock()

	return nil
}
