package simpledb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
	"github.com/keingoon/simpledb/internal/trx/recovery"
	"github.com/keingoon/simpledb/internal/trx/tx"
)

type simpleDBState int

const (
	created simpleDBState = iota
	starting
	started
	closed
)

type recoveryRunner interface {
	Recover(ctx context.Context) error
	Checkpoint(ctx context.Context) (int32, error)
}

type backgroundTicker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct {
	ticker *time.Ticker
}

func (t *realTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *realTicker) Stop() {
	t.ticker.Stop()
}

type simpleDBDeps struct {
	newRecoveryRunner func() (recoveryRunner, error)
	flushDirtyPages   func(ctx context.Context, limit int32) (int32, error)
	newTicker         func(d time.Duration) backgroundTicker
}

type SimpleDB struct {
	mu    sync.Mutex
	state simpleDBState

	fm      *file.FileMgr
	lm      *log.LogMgr
	bm      *buffer.BufferMgr
	locktbl *concurrency.LockTable
	atTbl   *recovery.ActiveTrxTable
	dptTbl  *buffer.DirtyPageTable

	ckpTicker  backgroundTicker
	pgclTicker backgroundTicker
	cancel     context.CancelFunc
	wg         *sync.WaitGroup

	bgErr   error
	errOnce sync.Once

	closeOnce sync.Once

	config SimpleDBConfig
	deps   simpleDBDeps
}

type SimpleDBConfig struct {
	blocksize            int32
	numbuffs             int32
	numwaits             int32
	ckpInterval          time.Duration
	pgclInterval         time.Duration
	pageCleanerBatchSize int32
}

func NewSimpleDB(dbDirectoryPath string, blocksize int32) (*SimpleDB, error) {
	fm, err := file.NewFileMgr(dbDirectoryPath, blocksize)
	if err != nil {
		return nil, err
	}
	lm, err := log.NewLogMgr(fm, defaultLogfileName)
	if err != nil {
		return nil, err
	}
	dptTbl := buffer.NewDirtyPageTable()
	bm := buffer.NewBufferMgr(fm, lm, defaultNumbuffs, defaultNumwaits, dptTbl)

	locktbl := concurrency.NewLockTable()
	atTbl := recovery.NewActiveTrxTable()
	config := SimpleDBConfig{
		blocksize:            blocksize,
		numbuffs:             defaultNumbuffs,
		numwaits:             defaultNumwaits,
		ckpInterval:          defaultCkpInterval,
		pgclInterval:         defaultPgclInterval,
		pageCleanerBatchSize: defaultPageCleanerBatchSize,
	}
	deps := simpleDBDeps{
		newRecoveryRunner: func() (recoveryRunner, error) {
			return tx.NewRecoveryTransactionMgr(locktbl, fm, lm, bm, atTbl, dptTbl)
		},
		flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
			return bm.FlushDirtyPages(ctx, limit)
		},
		newTicker: func(d time.Duration) backgroundTicker {
			return &realTicker{ticker: time.NewTicker(d)}
		},
	}

	return newSimpleDBWithDeps(fm, lm, bm, locktbl, atTbl, dptTbl, config, deps), nil
}

func newSimpleDBWithDeps(
	fm *file.FileMgr,
	lm *log.LogMgr,
	bm *buffer.BufferMgr,
	locktbl *concurrency.LockTable,
	atTbl *recovery.ActiveTrxTable,
	dptTbl *buffer.DirtyPageTable,
	config SimpleDBConfig,
	deps simpleDBDeps,
) *SimpleDB {
	return &SimpleDB{
		mu:        sync.Mutex{},
		state:     created,
		fm:        fm,
		lm:        lm,
		bm:        bm,
		locktbl:   locktbl,
		atTbl:     atTbl,
		dptTbl:    dptTbl,
		errOnce:   sync.Once{},
		closeOnce: sync.Once{},
		config:    config,
		deps:      deps,
	}
}

func (db *SimpleDB) Start() error {
	db.mu.Lock()
	state := db.state
	if state != created {
		db.mu.Unlock()
		return fmt.Errorf("start is not allowed in state %v", state)
	}
	db.state = starting
	db.mu.Unlock()

	if err := db.doStart(); err != nil {
		db.mu.Lock()
		db.state = created
		db.mu.Unlock()
		return err
	}

	return nil
}

func (db *SimpleDB) doStart() error {
	recovtxmgr, err := db.deps.newRecoveryRunner()
	if err != nil {
		return err
	}

	if !db.fm.IsNew() {
		recoveryCtx := context.Background()
		if err := recovtxmgr.Recover(recoveryCtx); err != nil {
			return err
		}
	}

	tickCtx, tickCancel := context.WithCancel(context.Background())
	ckpTicker := db.deps.newTicker(db.config.ckpInterval)
	pgclTicker := db.deps.newTicker(db.config.pgclInterval)
	wg := &sync.WaitGroup{}
	wg.Add(2)

	db.mu.Lock()
	db.wg = wg
	db.cancel = tickCancel
	db.ckpTicker = ckpTicker
	db.pgclTicker = pgclTicker
	db.state = started
	db.mu.Unlock()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ckpTicker.C():
				// transaction.Checkpoint() の処理を呼ぶ
				if _, err := recovtxmgr.Checkpoint(context.Background()); err != nil {
					db.failBackground(fmt.Errorf("checkpoint loop: %w", err))
					return
				}
			case <-tickCtx.Done():
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-pgclTicker.C():
				// transaction.PageCleaner() の処理を呼ぶ
				if _, err := db.deps.flushDirtyPages(context.Background(), db.config.pageCleanerBatchSize); err != nil {
					db.failBackground(fmt.Errorf("page cleaner loop: %w", err))
					return
				}
			case <-tickCtx.Done():
				return
			}
		}
	}()

	return nil
}

func (db *SimpleDB) Close() error {
	db.mu.Lock()
	state := db.state
	db.mu.Unlock()

	if state == created {
		return fmt.Errorf("close is not allowed in state %v", state)
	}
	if state == starting {
		return fmt.Errorf("close is not allowed in state %v", state)
	}
	if state == closed {
		return nil
	}

	var (
		ran bool
		err error
	)

	db.closeOnce.Do(func() {
		ran = true
		err = db.doClose()
	})

	if !ran {
		return nil
	}
	return err
}

func (db *SimpleDB) doClose() error {
	db.mu.Lock()
	cancel := db.cancel
	ckpTicker := db.ckpTicker
	pgclTicker := db.pgclTicker
	wg := db.wg
	db.mu.Unlock()

	if ckpTicker != nil {
		ckpTicker.Stop()
	}
	if pgclTicker != nil {
		pgclTicker.Stop()
	}
	if cancel != nil {
		cancel()
	}
	if wg != nil {
		wg.Wait()
	}

	db.mu.Lock()
	db.state = closed
	bgErr := db.bgErr
	db.mu.Unlock()

	if bgErr != nil {
		return fmt.Errorf("ticker loop error: %w", bgErr)
	}
	return nil
}

func (db *SimpleDB) failBackground(err error) {
	db.errOnce.Do(func() {
		db.mu.Lock()
		db.bgErr = err
		cancel := db.cancel
		db.mu.Unlock()

		if cancel != nil {
			cancel()
		}
	})
}
