package concurrency

import (
	"context"
	"fmt"
	"sync"

	"github.com/keingoon/simpledb/internal/file"
)

type concurrencyShard struct {
	locks map[file.BlockId]*waiter
	mu    sync.RWMutex
}

func newConcurrencyShard() *concurrencyShard {
	return &concurrencyShard{
		locks: make(map[file.BlockId]*waiter),
	}
}

type ConcurrencyMgr struct {
	locktbl  *LockTable
	shards   []*concurrencyShard
	shardNum int32
}

func NewConcurrencyMgr(locktbl *LockTable) *ConcurrencyMgr {
	shards := make([]*concurrencyShard, shardNum)
	for i := 0; i < shardNum; i++ {
		shards[i] = newConcurrencyShard()
	}
	return &ConcurrencyMgr{
		locktbl:  locktbl,
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
	if conMgr.hasLock(blk) {
		return nil
	}

	rWaiter := newWaiter(readMode, false, false)
	if err := conMgr.locktbl.SLock(ctx, blk, rWaiter); err != nil {
		return fmt.Errorf("slock abort exception: %w", err)
	}
	shard.mu.Lock()
	shard.locks[*blk] = rWaiter
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

	wwaiter := newWaiter(writeMode, upgrade, false)
	if err := conMgr.locktbl.XLock(ctx, blk, wwaiter); err != nil {
		return fmt.Errorf("xlock abort exception: %w", err)
	}

	shard.mu.Lock()
	shard.locks[*blk] = wwaiter
	shard.mu.Unlock()

	return nil
}

func (conMgr *ConcurrencyMgr) Unlock(ctx context.Context, blk *file.BlockId) error {
	shard := conMgr.getShard(blk)
	shard.mu.Lock()
	owner := shard.locks[*blk]
	delete(shard.locks, *blk)
	shard.mu.Unlock()
	return conMgr.locktbl.Unlock(ctx, blk, owner)
}

func (conMgr *ConcurrencyMgr) Release(ctx context.Context) error {
	// 全shardを順次処理
	for _, shard := range conMgr.shards {
		shard.mu.Lock()
		for blk := range shard.locks {
			shard.mu.Unlock()
			waiter := shard.locks[blk]
			if err := conMgr.locktbl.Unlock(ctx, &blk, waiter); err != nil {
				return fmt.Errorf("release abort exception: %w", err)
			}
			shard.mu.Lock()
			delete(shard.locks, blk)
		}
		shard.mu.Unlock()
	}
	return nil
}

func (conMgr *ConcurrencyMgr) hasLock(blk *file.BlockId) bool {
	shard := conMgr.getShard(blk)
	shard.mu.RLock()
	waiter := shard.locks[*blk]
	shard.mu.RUnlock()
	return waiter != nil
}

func (conMgr *ConcurrencyMgr) hasSLock(blk *file.BlockId) bool {
	shard := conMgr.getShard(blk)
	shard.mu.RLock()
	waiter := shard.locks[*blk]
	shard.mu.RUnlock()
	return waiter != nil && waiter.mode == readMode
}

func (conMgr *ConcurrencyMgr) hasXLock(blk *file.BlockId) bool {
	shard := conMgr.getShard(blk)
	shard.mu.RLock()
	waiter := shard.locks[*blk]
	shard.mu.RUnlock()
	return waiter != nil && waiter.mode == writeMode
}
