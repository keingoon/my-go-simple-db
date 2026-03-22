package simpledb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
	"github.com/keingoon/simpledb/internal/trx/recovery"
)

type fakeRecoveryRunner struct {
	recoverErr    error
	checkpointErr error
}

func (r *fakeRecoveryRunner) Recover(ctx context.Context) error {
	return r.recoverErr
}

func (r *fakeRecoveryRunner) Checkpoint(ctx context.Context) (int32, error) {
	return 0, r.checkpointErr
}

type manualTicker struct {
	ch chan time.Time
}

func (t *manualTicker) C() <-chan time.Time {
	return t.ch
}

func (t *manualTicker) Stop() {}

func (t *manualTicker) Tick() {
	t.ch <- time.Now()
}

func newTickerFactory(ckpTicker backgroundTicker, pgclTicker backgroundTicker) func(d time.Duration) backgroundTicker {
	var mu sync.Mutex
	callCount := 0

	return func(d time.Duration) backgroundTicker {
		mu.Lock()
		defer mu.Unlock()

		if callCount == 0 {
			callCount++
			return ckpTicker
		}
		callCount++
		return pgclTicker
	}
}

func newSimpleDBForTest(t *testing.T, dbDirectoryPath string, deps simpleDBDeps) *SimpleDB {
	t.Helper()

	const blocksize = int32(400)

	fm, err := file.NewFileMgr(dbDirectoryPath, blocksize)
	if err != nil {
		t.Fatal(err)
	}
	lm, err := log.NewLogMgr(fm, defaultLogfileName)
	if err != nil {
		t.Fatal(err)
	}
	dptTbl := buffer.NewDirtyPageTable()
	bm := buffer.NewBufferMgr(fm, lm, defaultNumbuffs, defaultNumwaits, dptTbl)
	locktbl := concurrency.NewLockTable()
	atTbl := recovery.NewActiveTrxTable()
	config := SimpleDBConfig{
		blocksize:            blocksize,
		numbuffs:             defaultNumbuffs,
		numwaits:             defaultNumwaits,
		ckpInterval:          time.Second,
		pgclInterval:         time.Second * 2,
		pageCleanerBatchSize: defaultPageCleanerBatchSize,
	}

	return newSimpleDBWithDeps(fm, lm, bm, locktbl, atTbl, dptTbl, config, deps)
}

func TestSimpleDB(t *testing.T) {
	t.Run("Startは1回成功しstartedになる", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)

		if err := db.Start(); err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != started {
			t.Errorf("stateはstartedであるべきだが%vだった", got)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("cleanupのCloseは成功するべきだが%vだった", err)
		}
	})

	t.Run("Closeは1回成功しclosedになる", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)
		if err := db.Start(); err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("Closeは成功するべきだが%vだった", err)
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != closed {
			t.Errorf("stateはclosedであるべきだが%vだった", got)
		}
	})

	t.Run("StartはnewRecoveryRunner失敗でcreatedに戻る", func(t *testing.T) {
		expectedErr := errors.New("new recovery runner failed")
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return nil, expectedErr
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: func(d time.Duration) backgroundTicker {
				return &manualTicker{ch: make(chan time.Time, 1)}
			},
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)

		err := db.Start()
		if !errors.Is(err, expectedErr) {
			t.Fatalf("Startは%vを返すべきだが%vだった", expectedErr, err)
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != created {
			t.Errorf("stateはcreatedに戻るべきだが%vだった", got)
		}
	})

	t.Run("StartはRecover失敗でcreatedに戻る", func(t *testing.T) {
		dbDirectoryPath := t.TempDir()
		existingFilePath := filepath.Join(dbDirectoryPath, "existing")
		if err := os.WriteFile(existingFilePath, []byte("already exists"), 0644); err != nil {
			t.Fatal(err)
		}

		expectedErr := errors.New("recover failed")
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{recoverErr: expectedErr}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: func(d time.Duration) backgroundTicker {
				return &manualTicker{ch: make(chan time.Time, 1)}
			},
		}
		db := newSimpleDBForTest(t, dbDirectoryPath, deps)

		err := db.Start()
		if !errors.Is(err, expectedErr) {
			t.Fatalf("Startは%vを返すべきだが%vだった", expectedErr, err)
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != created {
			t.Errorf("stateはcreatedに戻るべきだが%vだった", got)
		}
	})

	t.Run("Closeはcreatedでエラー", func(t *testing.T) {
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: func(d time.Duration) backgroundTicker {
				return &manualTicker{ch: make(chan time.Time, 1)}
			},
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)

		err := db.Close()
		if err == nil {
			t.Fatalf("created状態のCloseはエラーを返すべき")
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != created {
			t.Errorf("stateはcreatedのままであるべきだが%vだった", got)
		}
	})

	t.Run("Closeは直列2回実行で2回目にnilを返す", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)
		if err := db.Start(); err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("1回目のCloseは成功するべきだが%vだった", err)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("2回目のCloseはnilを返すべきだが%vだった", err)
		}
	})

	t.Run("Startは並列2回実行で成功1件error1件になる", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		recoveryRunnerStarted := make(chan struct{}, 1)
		releaseRecoveryRunner := make(chan struct{})
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				select {
				case recoveryRunnerStarted <- struct{}{}:
				default:
				}
				<-releaseRecoveryRunner
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)

		startSignal := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-startSignal
			errs <- db.Start()
		}()
		go func() {
			defer wg.Done()
			<-startSignal
			errs <- db.Start()
		}()

		close(startSignal)
		<-recoveryRunnerStarted
		close(releaseRecoveryRunner)
		wg.Wait()
		close(errs)

		successCount := 0
		errorCount := 0
		for err := range errs {
			if err == nil {
				successCount++
				continue
			}
			errorCount++
		}

		if successCount != 1 {
			t.Errorf("成功は1件であるべきだが%d件だった", successCount)
		}
		if errorCount != 1 {
			t.Errorf("エラーは1件であるべきだが%d件だった", errorCount)
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != started {
			t.Errorf("stateはstartedであるべきだが%vだった", got)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("cleanupのCloseは成功するべきだが%vだった", err)
		}
	})

	t.Run("Startはstarting状態でエラーを返す", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		recoveryRunnerStarted := make(chan struct{}, 1)
		releaseRecoveryRunner := make(chan struct{})
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				select {
				case recoveryRunnerStarted <- struct{}{}:
				default:
				}
				<-releaseRecoveryRunner
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)

		startErrCh := make(chan error, 1)
		go func() {
			startErrCh <- db.Start()
		}()

		<-recoveryRunnerStarted

		err := db.Start()
		if err == nil {
			t.Fatal("starting状態のStartはエラーを返すべき")
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != starting {
			t.Errorf("stateはstartingであるべきだが%vだった", got)
		}

		close(releaseRecoveryRunner)

		if err := <-startErrCh; err != nil {
			t.Fatalf("先に開始したStartは成功するべきだが%vだった", err)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("cleanupのCloseは成功するべきだが%vだった", err)
		}
	})

	t.Run("starting状態でのCloseはエラーを返す", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		recoveryRunnerStarted := make(chan struct{}, 1)
		releaseRecoveryRunner := make(chan struct{})
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				select {
				case recoveryRunnerStarted <- struct{}{}:
				default:
				}
				<-releaseRecoveryRunner
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)

		startErrCh := make(chan error, 1)
		go func() {
			startErrCh <- db.Start()
		}()

		<-recoveryRunnerStarted

		err := db.Close()
		if err == nil {
			t.Fatal("starting状態のCloseはエラーを返すべき")
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != starting {
			t.Errorf("stateはstartingであるべきだが%vだった", got)
		}

		close(releaseRecoveryRunner)

		if err := <-startErrCh; err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("cleanupのCloseは成功するべきだが%vだった", err)
		}
	})

	t.Run("Closeは並列2回実行でpanicせず完了する", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)
		if err := db.Start(); err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}

		startSignal := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-startSignal
			errs <- db.Close()
		}()
		go func() {
			defer wg.Done()
			<-startSignal
			errs <- db.Close()
		}()

		close(startSignal)
		wg.Wait()
		close(errs)

		for err := range errs {
			if err != nil {
				t.Fatalf("並列Closeはnilで完了するべきだが%vだった", err)
			}
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != closed {
			t.Errorf("stateはclosedであるべきだが%vだった", got)
		}
	})

	t.Run("closed状態でのCloseはnilを返す", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)
		if err := db.Start(); err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("1回目のCloseは成功するべきだが%vだった", err)
		}

		err := db.Close()
		if err != nil {
			t.Fatalf("closed状態のCloseはnilを返すべきだが%vだった", err)
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != closed {
			t.Errorf("stateはclosedのままであるべきだが%vだった", got)
		}
	})

	t.Run("closed後のStartはエラーを返す", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)
		if err := db.Start(); err != nil {
			t.Fatalf("1回目のStartは成功するべきだが%vだった", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Closeは成功するべきだが%vだった", err)
		}

		err := db.Start()
		if err == nil {
			t.Fatal("closed後のStartはエラーを返すべき")
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != closed {
			t.Errorf("stateはclosedのままであるべきだが%vだった", got)
		}
	})

	t.Run("checkpoint loop失敗はCloseでerrorになる", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		expectedErr := errors.New("checkpoint failed")
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{checkpointErr: expectedErr}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)
		if err := db.Start(); err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}

		ckpTicker.Tick()
		deadline := time.Now().Add(1 * time.Second)
		for {
			db.mu.Lock()
			bgErr := db.bgErr
			db.mu.Unlock()
			if bgErr != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("checkpoint loop failureがbgErrに反映されなかった")
			}
			time.Sleep(1 * time.Millisecond)
		}

		err := db.Close()
		if err == nil {
			t.Fatal("checkpoint loop failure時のCloseはerrorを返すべき")
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != closed {
			t.Errorf("stateはclosedであるべきだが%vだった", got)
		}
	})

	t.Run("background errorが複数回起きてもCloseは最初のerrorだけ返す", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, nil
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)
		if err := db.Start(); err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}

		firstErr := errors.New("first background error")
		secondErr := errors.New("second background error")
		db.failBackground(firstErr)
		db.failBackground(secondErr)

		err := db.Close()
		if !errors.Is(err, firstErr) {
			t.Fatalf("Closeは最初のerror %v を返すべきだが%vだった", firstErr, err)
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != closed {
			t.Errorf("stateはclosedであるべきだが%vだった", got)
		}
	})

	t.Run("page cleaner loop失敗はCloseでerrorになる", func(t *testing.T) {
		ckpTicker := &manualTicker{ch: make(chan time.Time, 1)}
		pgclTicker := &manualTicker{ch: make(chan time.Time, 1)}
		expectedErr := errors.New("page cleaner failed")
		deps := simpleDBDeps{
			newRecoveryRunner: func() (recoveryRunner, error) {
				return &fakeRecoveryRunner{}, nil
			},
			flushDirtyPages: func(ctx context.Context, limit int32) (int32, error) {
				return 0, expectedErr
			},
			newTicker: newTickerFactory(ckpTicker, pgclTicker),
		}
		db := newSimpleDBForTest(t, t.TempDir(), deps)
		if err := db.Start(); err != nil {
			t.Fatalf("Startは成功するべきだが%vだった", err)
		}

		pgclTicker.Tick()
		deadline := time.Now().Add(1 * time.Second)
		for {
			db.mu.Lock()
			bgErr := db.bgErr
			db.mu.Unlock()
			if bgErr != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("page cleaner loop failureがbgErrに反映されなかった")
			}
			time.Sleep(1 * time.Millisecond)
		}

		err := db.Close()
		if err == nil {
			t.Fatal("page cleaner loop failure時のCloseはerrorを返すべき")
		}

		db.mu.Lock()
		got := db.state
		db.mu.Unlock()
		if got != closed {
			t.Errorf("stateはclosedであるべきだが%vだった", got)
		}
	})
}
