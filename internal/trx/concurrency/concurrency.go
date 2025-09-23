package concurrency

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keingoon/simpledb/internal/file"
)

const (
	maxtime         = 10000
	shardNum        = 128
	wLockedMask     = int32(^(1<<31 - 1))
	readerCountMask = int32(1<<31 - 1) // last 31bits are reader count
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
	prev, next *waiter
}

func newWaiter(mode LockMode, upgrade bool) *waiter {
	return &waiter{ch: make(chan struct{}), mode: mode, upgrade: upgrade, prev: nil, next: nil}
}

type lockShard struct {
	state       int32 // first 1bit is write/read flag, last 31bits are reader count
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
			if (old & wLockedMask) != 0 {
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
				return ctx.Err()
			default:
			}
			old := atomic.LoadInt32(&lockShard.state)
			locked := old & wLockedMask
			readers := old & readerCountMask
			// forbid if (W locked) or (readers > 0 and not (upgrade && readers == 1))
			if (locked != 0) || (readers > 0 && !(wwaiter.upgrade && readers == 1)) {
				continue
			}
			new := old | wLockedMask
			if wwaiter.upgrade {
				new -= 1
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
		// Xlockを呼ばれたときにSlockを既に保持しているため、待機列から外す必要はない。
		lockShard.enqueueUpgrade(wwaiter)
	} else {
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
		lockShard.mu.Unlock()
		return ctx.Err()
	case <-wwaiter.ch:
		return nil
	}
}

func (locktbl *LockTable) Unlock(ctx context.Context, blk *file.BlockId) error {
	lockShard := locktbl.getShard(blk)
	old := atomic.LoadInt32(&lockShard.state)

	var updated int32
	locked := old & wLockedMask
	if locked != 0 {
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
	locked = updated & wLockedMask
	if locked == 0 {
		readers := updated & readerCountMask
		if readers == 1 && lockShard.upgradeHead != nil {
			lockShard.wakeUpgradeWaiter()
		} else if readers == 0 {
			if ww := lockShard.wakeWriteWaiter(); ww == nil {
				lockShard.wakeAllReadWaiters()
			}
		} else {
			lockShard.wakeAllReadWaiters()
		}
	}
	lockShard.mu.Unlock()

	return nil
}

var LockTbl = NewLockTable()

type concurrencyShard struct {
	locks map[*file.BlockId]*waiter
	mu    sync.RWMutex
}

func newConcurrencyShard() *concurrencyShard {
	return &concurrencyShard{
		locks: make(map[*file.BlockId]*waiter),
	}
}

type ConcurrencyMgr struct {
	shards   []*concurrencyShard
	shardNum int32
}

func NewConcurrencyMgr() *ConcurrencyMgr {
	shards := make([]*concurrencyShard, shardNum)
	for i := 0; i < shardNum; i++ {
		shards[i] = newConcurrencyShard()
	}
	return &ConcurrencyMgr{
		shards:   shards,
		shardNum: shardNum,
	}
}

func (conMgr *ConcurrencyMgr) getShard(blk *file.BlockId) *concurrencyShard {
	h := fnvHash(blk.ToString())
	shardIndex := int32(h % uint32(conMgr.shardNum))
	return conMgr.shards[shardIndex]
}

func (conMgr *ConcurrencyMgr) SLock(ctx context.Context, blk *file.BlockId) error {
	shard := conMgr.getShard(blk)

	// すでに保持している場合は何もしない
	shard.mu.Lock()
	if waiter, ok := shard.locks[blk]; ok && waiter != nil {
		shard.mu.Unlock()
		return nil
	}
	shard.mu.Unlock()

	rWaiter := newWaiter(readMode, false)
	if err := LockTbl.SLock(ctx, blk, rWaiter); err != nil {
		return fmt.Errorf("slock abort exception: %w", err)
	}
	shard.mu.Lock()
	shard.locks[blk] = rWaiter
	shard.mu.Unlock()
	return nil
}

func (conMgr *ConcurrencyMgr) XLock(ctx context.Context, blk *file.BlockId) error {
	shard := conMgr.getShard(blk)

	// XLockをすでに保持してる場合は何も処理しない
	if conMgr.hasXLock(blk) {
		return nil
	}

	// upgrade 判定のため現在の状態を参照
	upgrade := conMgr.hasSLock(blk)

	wwaiter := newWaiter(writeMode, upgrade)
	if err := LockTbl.XLock(ctx, blk, wwaiter); err != nil {
		return fmt.Errorf("xlock abort exception: %w", err)
	}

	shard.mu.Lock()
	shard.locks[blk] = wwaiter
	shard.mu.Unlock()

	return nil
}

func (conMgr *ConcurrencyMgr) Release(ctx context.Context) error {
	// 全shardを順次処理
	for _, shard := range conMgr.shards {
		shard.mu.Lock()
		for blk := range shard.locks {
			shard.mu.Unlock()
			if err := LockTbl.Unlock(ctx, blk); err != nil {
				return fmt.Errorf("release abort exception: %w", err)
			}
			shard.mu.Lock()
			delete(shard.locks, blk)
		}
		shard.mu.Unlock()
	}
	return nil
}

func (conMgr *ConcurrencyMgr) hasSLock(blk *file.BlockId) bool {
	shard := conMgr.getShard(blk)
	shard.mu.RLock()
	waiter := shard.locks[blk]
	shard.mu.RUnlock()
	return waiter != nil && waiter.mode == readMode
}

func (conMgr *ConcurrencyMgr) hasXLock(blk *file.BlockId) bool {
	shard := conMgr.getShard(blk)
	shard.mu.RLock()
	waiter := shard.locks[blk]
	shard.mu.RUnlock()
	return waiter != nil && waiter.mode == writeMode
}
