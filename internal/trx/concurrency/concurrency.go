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
	maxtime          = 10000
	shardNum         = 128
	wLockedMask      = 1 // 1: write locked, 0: not locked
	readerCountMask  = 1<<31 - 1
	readerCountShift = 1
	maxSpinCount     = 30
)

type LockMode int8

const (
	ReadMode LockMode = iota
	WriteMode
)

type Waiter struct {
	ch         chan struct{}
	mode       LockMode
	upgrade    bool
	prev, next *Waiter
}

func newWaiter(mode LockMode, upgrade bool) *Waiter {
	return &Waiter{ch: make(chan struct{}), mode: mode, upgrade: upgrade, prev: nil, next: nil}
}

type lockShard struct {
	state       int32
	rHead       *Waiter
	rTail       *Waiter
	wHead       *Waiter
	wTail       *Waiter
	upgradeHead *Waiter
	upgradeTail *Waiter
	mu          sync.Mutex
}

func newLockShard() *lockShard {
	return &lockShard{state: 0, rHead: nil, rTail: nil, wHead: nil, wTail: nil, upgradeHead: nil, upgradeTail: nil}
}

func (shard *lockShard) pushBackRead(w *Waiter) {
	if shard.rTail == nil {
		shard.rHead = w
		shard.rTail = w
	} else {
		w.prev = shard.rTail
		shard.rTail.next = w
		shard.rTail = w
	}
}

func (shard *lockShard) pushBackWrite(w *Waiter) {
	if shard.wTail == nil {
		shard.wHead = w
		shard.wTail = w
	} else {
		w.prev = shard.wTail
		shard.wTail.next = w
		shard.wTail = w
	}
}

func (shard *lockShard) pushBackUpgrade(w *Waiter) {
	if shard.upgradeTail == nil {
		shard.upgradeHead = w
		shard.upgradeTail = w
	} else {
		w.prev = shard.upgradeTail
		shard.upgradeTail.next = w
		shard.upgradeTail = w
	}
}

func (shard *lockShard) removeReadWaiter(w *Waiter) {
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

func (shard *lockShard) removeWriteWaiter(w *Waiter) {
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

func (shard *lockShard) removeUpgradeWaiter(w *Waiter) {
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

func (shard *lockShard) wakeAllReadWaiters() []*Waiter {
	rWaiters := make([]*Waiter, 0)
	for rw := shard.rHead; rw != nil; {
		next := rw.next
		close(rw.ch)
		shard.removeReadWaiter(rw)
		rWaiters = append(rWaiters, rw)
		rw = next
	}
	return rWaiters
}

func (shard *lockShard) wakeWriteWaiter() *Waiter {
	writeWaiter := shard.wHead
	if writeWaiter == nil {
		return nil
	}
	close(writeWaiter.ch)
	shard.removeWriteWaiter(writeWaiter)
	return writeWaiter
}

func (shard *lockShard) wakeUpgradeWaiter() *Waiter {
	upgradeWaiter := shard.upgradeHead
	if upgradeWaiter == nil {
		return nil
	}
	close(upgradeWaiter.ch)
	shard.removeUpgradeWaiter(upgradeWaiter)
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
	shardIndex := int32(h) % locktbl.shardNum
	shard := locktbl.shards[shardIndex]
	return shard
}

func fnvHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (locktbl *LockTable) SLock(ctx context.Context, blk *file.BlockId, rwaiter *Waiter) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(locktbl.maxtime))
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
			if old&wLockedMask == 1 {
				continue
			}
			new := old + (1 << readerCountShift)
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

func (locktbl *LockTable) slowSLock(ctx context.Context, blk *file.BlockId, rwaiter *Waiter) error {
	lockShard := locktbl.getShard(blk)

	lockShard.mu.Lock()
	lockShard.pushBackRead(rwaiter)
	lockShard.mu.Unlock()
	select {
	case <-ctx.Done():
		lockShard.mu.Lock()
		lockShard.removeReadWaiter(rwaiter)
		lockShard.mu.Unlock()
		return ctx.Err()
	case <-rwaiter.ch:
		return nil
	}
}

func (locktbl *LockTable) XLock(ctx context.Context, blk *file.BlockId, wwaiter *Waiter) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(locktbl.maxtime))
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
			readers := old & readerCountMask
			// forbid if (W locked) or (readers > 0 and not (upgrade && readers == 1))
			if (old&wLockedMask == 1) || (readers > 0 && !(wwaiter.upgrade && readers == 1)) {
				continue
			}
			new := old | wLockedMask
			if wwaiter.upgrade {
				new -= (1 << readerCountShift)
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

func (locktbl *LockTable) slowXLock(ctx context.Context, blk *file.BlockId, wwaiter *Waiter) error {
	lockShard := locktbl.getShard(blk)

	lockShard.mu.Lock()
	if wwaiter.upgrade {
		// 読みは既に保持済み。待機列から外す必要はない。
		lockShard.pushBackUpgrade(wwaiter)
	} else {
		lockShard.pushBackWrite(wwaiter)
	}
	lockShard.mu.Unlock()

	select {
	case <-ctx.Done():
		lockShard.mu.Lock()
		if wwaiter.upgrade {
			lockShard.removeUpgradeWaiter(wwaiter)
		} else {
			lockShard.removeWriteWaiter(wwaiter)
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
	if old&wLockedMask == 1 {
		new := (old &^ wLockedMask)
		// write中は他がstateを変更しない前提のため、in-place更新
		atomic.StoreInt32(&lockShard.state, new)
		updated = new
	} else {
		// readしてる時はread数だけatomic減算の戻り値を利用
		updated = atomic.AddInt32(&lockShard.state, -(1 << readerCountShift))
	}

	lockShard.mu.Lock()
	// 譲渡ポリシー:
	// - writeビットが落ちているときのみ誰かを起こす
	// - readers==1 かつ upgrade待機がある場合は upgrade を優先的に付与
	// - readers==0 の場合は upgrade → write → read の順
	// - readers>=1 でも writer/upgrade がいなければ read は起こしてよい（スループット重視）
	if updated&wLockedMask == 0 {
		readers := updated & readerCountMask
		if readers == 1 && lockShard.upgradeHead != nil {
			lockShard.wakeUpgradeWaiter()
		} else if readers == 0 {
			if uw := lockShard.wakeUpgradeWaiter(); uw == nil {
				if ww := lockShard.wakeWriteWaiter(); ww == nil {
					lockShard.wakeAllReadWaiters()
				}
			}
		} else if lockShard.wHead == nil && lockShard.upgradeHead == nil {
			// writer/upgrade が不在なら、readを増やしてよい
			lockShard.wakeAllReadWaiters()
		}
	}
	lockShard.mu.Unlock()

	return nil
}

var LockTbl = NewLockTable()

type concurrencyShard struct {
	locks map[*file.BlockId]*Waiter
	mu    sync.RWMutex
}

func newConcurrencyShard() *concurrencyShard {
	return &concurrencyShard{
		locks: make(map[*file.BlockId]*Waiter),
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
	shardIndex := int32(h) % conMgr.shardNum
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

	rWaiter := newWaiter(ReadMode, false)
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

	wwaiter := newWaiter(WriteMode, upgrade)
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
	return waiter != nil && waiter.mode == ReadMode
}

func (conMgr *ConcurrencyMgr) hasXLock(blk *file.BlockId) bool {
	shard := conMgr.getShard(blk)
	shard.mu.RLock()
	waiter := shard.locks[blk]
	shard.mu.RUnlock()
	return waiter != nil && waiter.mode == WriteMode
}
