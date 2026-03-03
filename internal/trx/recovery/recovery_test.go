package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/keingoon/simpledb/internal/buffer"
	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/log"
	"github.com/keingoon/simpledb/internal/trx/access"
	"github.com/keingoon/simpledb/internal/trx/bufferlist"
	"github.com/keingoon/simpledb/internal/trx/concurrency"
	"github.com/keingoon/simpledb/internal/trx/recovery/logrecord"
)

const (
	blocksize = int32(256)
	logfile   = "logfile"
	filename  = "testfile"
	numbuffs  = 10
	numwaits  = 10
	// LogMgr reserves block#0 for header. The first data block is block#1 and its first record starts at offset 4.
	firstLSN = blocksize + int32(4)
)

type recoveryTestBase struct {
	locktbl *concurrency.LockTable
	fm      *file.FileMgr
	lm      *log.LogMgr
	bm      *buffer.BufferMgr
}

func initRecoveryBase(t *testing.T) *recoveryTestBase {
	t.Helper()
	locktbl := concurrency.NewLockTable()
	dpt := buffer.NewDirtyPageTable()
	fm, err := file.NewFileMgr(t.TempDir(), blocksize)
	if err != nil {
		t.Fatalf("failed to create FileMgr: %v", err)
	}
	lm, err := log.NewLogMgr(fm, logfile)
	if err != nil {
		t.Fatalf("failed to create LogMgr: %v", err)
	}
	// For tests, make sure Recovery's scan start LSN never points to the log header block.
	if err := lm.WriteMasterLSN(firstLSN); err != nil {
		t.Fatalf("failed to write master LSN: %v", err)
	}
	bm := buffer.NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

	return &recoveryTestBase{
		locktbl: locktbl,
		fm:      fm,
		lm:      lm,
		bm:      bm,
	}
}

func initRecoveryEnvOnBase(t *testing.T, base *recoveryTestBase, txnum int32) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *access.Transaction, *RecoveryMgr, *ActiveTrxTable, *buffer.DirtyPageTable) {
	t.Helper()
	locktbl, fm, lm, bm := base.locktbl, base.fm, base.lm, base.bm
	mybuffers := bufferlist.NewBufferList(bm)
	tx := access.NewTransaction(locktbl, fm, lm, bm, txnum, mybuffers)
	atTbl := newActiveTrxTable()
	dptTbl := buffer.NewDirtyPageTable()
	rm, err := NewRecoveryMgr(locktbl, fm, lm, bm, tx, txnum, atTbl, dptTbl)
	if err != nil {
		t.Fatalf("failed to create RecoveryMgr: %v", err)
	}

	// NewRecoveryMgr writes START but it may still be in-memory; flush it so iterators can see it.
	startLSN, err := atTbl.getLastLSN(txnum)
	if err != nil {
		t.Fatalf("failed to get START lsn: %v", err)
	}
	lm.Flush(startLSN)
	return fm, lm, bm, tx, rm, atTbl, dptTbl
}

func initRecoveryEnv(t *testing.T, txnum int32) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *access.Transaction, *RecoveryMgr, *ActiveTrxTable, *buffer.DirtyPageTable) {
	t.Helper()
	base := initRecoveryBase(t)
	return initRecoveryEnvOnBase(t, base, txnum)
}

func initRecoveryEnvForRecoverOnBase(t *testing.T, base *recoveryTestBase) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *access.Transaction, *RecoveryMgr, *ActiveTrxTable, *buffer.DirtyPageTable) {
	t.Helper()
	locktbl, fm, lm, bm := base.locktbl, base.fm, base.lm, base.bm
	atTbl := NewActiveTrxTable()
	dptTbl := buffer.NewDirtyPageTable()
	rm := NewRecoveryMgrForRecover(locktbl, fm, lm, bm, atTbl, dptTbl)

	return fm, lm, bm, rm.txAccess, rm, rm.atTbl, rm.dptTbl
}

func initRecoveryEnvForRecover(t *testing.T) (*file.FileMgr, *log.LogMgr, *buffer.BufferMgr, *access.Transaction, *RecoveryMgr, *ActiveTrxTable, *buffer.DirtyPageTable) {
	t.Helper()
	base := initRecoveryBase(t)
	return initRecoveryEnvForRecoverOnBase(t, base)
}

type logRecordResult[T logrecord.LogRecord] struct {
	LSN int32
	Rec T
}

func collectLogRecords[T logrecord.LogRecord](
	lm *log.LogMgr,
	startLSN int32,
	txnum *int32,
) []logRecordResult[T] {
	iter, err := lm.Iterater(startLSN)
	if err != nil {
		return nil
	}
	var records []logRecordResult[T]
	for iter.HasNext() {
		lsn, bytes := iter.Next()
		rec := logrecord.CreateLogRecord(bytes, lsn)
		if rec == nil {
			continue
		}
		if txnum != nil && rec.TxNumber() != *txnum {
			continue
		}
		r, ok := rec.(T)
		if ok {
			records = append(records, logRecordResult[T]{LSN: lsn, Rec: r})
		}
	}
	return records
}

type setStep struct {
	log   func(*buffer.Buffer) (int32, error)
	apply func(*buffer.Buffer) error
}

type setResult struct {
	LSN  int32
	Buff *buffer.Buffer
}

func runSetCase(
	t *testing.T,
	ctx context.Context,
	fm *file.FileMgr,
	bm *buffer.BufferMgr,
	setSteps ...setStep,
) *setResult {
	t.Helper()

	blk, err := fm.Append(filename)
	if err != nil {
		t.Fatalf("append block failed: %v", err)
	}
	buff, err := bm.Pin(ctx, blk)
	if err != nil {
		t.Fatalf("pin failed: %v", err)
	}

	var latestLSN int32
	for _, s := range setSteps {
		if latestLSN, err = s.log(buff); err != nil {
			t.Fatalf("recoveryMgr.set failed: %v", err)
		}
		if err := s.apply(buff); err != nil {
			t.Fatalf("buff.set newval failed: %v", err)
		}
	}
	return &setResult{
		LSN:  latestLSN,
		Buff: buff,
	}
}

type rollbackResult struct {
	EndLSN int32
	Buff   *buffer.Buffer
}

func runRollbackCase(
	t *testing.T,
	ctx context.Context,
	fm *file.FileMgr,
	bm *buffer.BufferMgr,
	rm *RecoveryMgr,
	initVal func(*buffer.Buffer) error,
	setSteps ...setStep,
) *rollbackResult {
	t.Helper()

	blk, err := fm.Append(filename)
	if err != nil {
		t.Fatalf("append block failed: %v", err)
	}
	buff, err := bm.Pin(ctx, blk)
	if err != nil {
		t.Fatalf("pin failed: %v", err)
	}

	if err := initVal(buff); err != nil {
		t.Fatalf("buff.set oldval failed: %v", err)
	}

	for _, s := range setSteps {
		if _, err := s.log(buff); err != nil {
			t.Fatalf("recoveryMgr.set failed: %v", err)
		}
		if err := s.apply(buff); err != nil {
			t.Fatalf("buff.set newval failed: %v", err)
		}
	}

	endLSN, err := rm.Rollback(ctx)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	return &rollbackResult{
		EndLSN: endLSN,
		Buff:   buff,
	}
}

type multiBlkStep struct {
	page  int
	log   func(*buffer.Buffer) (int32, error)
	apply func(*buffer.Buffer) error
}

type setMultiBlkResult struct {
	LastLSN int32
	Items   []setBlkResult
}

type setBlkResult struct {
	LastLSN int32
	RecLSN  int32
	Buff    *buffer.Buffer
	Blk     *file.BlockId
}

func runSetMultiBlkCase(
	t *testing.T,
	ctx context.Context,
	fm *file.FileMgr,
	bm *buffer.BufferMgr,
	numPages int,
	initVals []func(*buffer.Buffer) error, // pageごとの初期化
	steps ...multiBlkStep,
) *setMultiBlkResult {
	t.Helper()

	items := make([]setBlkResult, numPages)

	for i := 0; i < numPages; i++ {
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
		buff, err := bm.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("pin failed: %v", err)
		}
		items[i].Blk, items[i].Buff, items[i].RecLSN = blk, buff, -1

		if i < len(initVals) && initVals[i] != nil {
			if err := initVals[i](buff); err != nil {
				t.Fatalf("init failed: %v", err)
			}
		}
	}

	var lastLSN int32
	for _, s := range steps {
		buff := items[s.page].Buff
		lsn, err := s.log(buff)
		if err != nil {
			t.Fatalf("set log failed: %v", err)
		}
		lastLSN = lsn
		items[s.page].LastLSN = lsn
		if items[s.page].RecLSN == -1 {
			items[s.page].RecLSN = lsn
		}

		if err := s.apply(buff); err != nil {
			t.Fatalf("set apply failed: %v", err)
		}
	}

	return &setMultiBlkResult{LastLSN: lastLSN, Items: items}
}

type rollbackMultiBlkResult struct {
	EndLSN int32
	Buffs  []*buffer.Buffer
	Blks   []*file.BlockId
}

func runRollbackMultiBlkCase(
	t *testing.T,
	ctx context.Context,
	fm *file.FileMgr,
	bm *buffer.BufferMgr,
	rm *RecoveryMgr,
	numPages int,
	initVals []func(*buffer.Buffer) error, // pageごとの初期化
	steps ...multiBlkStep,
) *rollbackMultiBlkResult {
	t.Helper()

	buffs := make([]*buffer.Buffer, numPages)
	blks := make([]*file.BlockId, numPages)

	for i := 0; i < numPages; i++ {
		blk, err := fm.Append(filename)
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
		buff, err := bm.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("pin failed: %v", err)
		}
		blks[i], buffs[i] = blk, buff

		if i < len(initVals) && initVals[i] != nil {
			if err := initVals[i](buff); err != nil {
				t.Fatalf("init failed: %v", err)
			}
		}
	}

	for _, s := range steps {
		buff := buffs[s.page]
		if _, err := s.log(buff); err != nil {
			t.Fatalf("set log failed: %v", err)
		}
		if err := s.apply(buff); err != nil {
			t.Fatalf("set apply failed: %v", err)
		}
	}

	endLSN, err := rm.Rollback(ctx)
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	return &rollbackMultiBlkResult{EndLSN: endLSN, Buffs: buffs, Blks: blks}
}

func TestRecoveryMgr(t *testing.T) {
	t.Run("NewRecoveryMgr", func(t *testing.T) {
		const (
			txnum = 1
		)
		_, lm, _, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)
		if rm == nil {
			t.Fatal("RecoveryMgr が nil であるべきではない")
		}

		t.Run("txnum の StartRecord が1回ログに書き込まれている", func(t *testing.T) {
			txnum := int32(txnum)
			records := collectLogRecords[*logrecord.StartRecord](lm, firstLSN, &txnum)
			if len(records) != 1 {
				t.Errorf("tx=%d のStartRecordが見つからない", txnum)
			}
		})

		t.Run("ATT に running である txnum が追加されている", func(t *testing.T) {
			entry, ok := atTbl.getTrx(txnum)
			t.Run("ATT に txnum が存在する", func(t *testing.T) {
				if !ok {
					t.Errorf("tx=%d が ATT に存在しない", txnum)
				}
			})

			t.Run("ATT に 存在している txnum は running である", func(t *testing.T) {
				if entry.getStatus() != running {
					t.Errorf("tx=%d が running であるべきだが %d である", txnum, atTbl.getTable()[txnum].getStatus())
				}
			})
		})
	})

	t.Run("NewRecoveryMgrForRecover", func(t *testing.T) {
		locktbl := concurrency.NewLockTable()
		dpt := buffer.NewDirtyPageTable()
		fm, err := file.NewFileMgr(t.TempDir(), blocksize)
		if err != nil {
			t.Fatalf("failed to create FileMgr: %v", err)
		}
		lm, err := log.NewLogMgr(fm, logfile)
		if err != nil {
			t.Fatalf("failed to create LogMgr: %v", err)
		}
		if err := lm.WriteMasterLSN(firstLSN); err != nil {
			t.Fatalf("failed to write master LSN: %v", err)
		}
		bm := buffer.NewBufferMgr(fm, lm, numbuffs, numwaits, dpt)

		atTbl := NewActiveTrxTable()
		rm := NewRecoveryMgrForRecover(locktbl, fm, lm, bm, atTbl, dpt)
		if rm == nil {
			t.Fatal("RecoveryMgr が nil であるべきではない")
		}

		t.Run("txnum は RecoveryTxNum である", func(t *testing.T) {
			if rm.txnum != access.RecoveryTxNum {
				t.Errorf("txnum は %d であるべきだが %d だった", access.RecoveryTxNum, rm.txnum)
			}
		})

		t.Run("RecoveryTxNum の StartRecord がログに書き込まれていない", func(t *testing.T) {
			txnum := int32(access.RecoveryTxNum)
			records := collectLogRecords[*logrecord.StartRecord](lm, firstLSN, &txnum)
			if len(records) != 0 {
				t.Errorf("tx=%d のStartRecordが書き込まれているべきではない", txnum)
			}
		})

		t.Run("ATT は空である", func(t *testing.T) {
			if len(rm.atTbl.getTable()) != 0 {
				t.Errorf("ATT が空であるべきだが %d 件ある", len(rm.atTbl.getTable()))
			}
		})
	})

	t.Run("Commit", func(t *testing.T) {
		t.Run("Commit noop", func(t *testing.T) {
			const (
				txnum = 2
			)
			ctx := context.Background()
			_, lm, _, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

			endLSN, err := rm.Commit(ctx)
			if err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			t.Run("CommitRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.CommitRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のCommitRecordが見つからない", txnum)
				}
			})

			t.Run("ATT の txnum が削除されている", func(t *testing.T) {
				if _, ok := atTbl.getTrx(txnum); ok {
					t.Errorf("tx=%d が ATT に存在するべきではない", txnum)
				}
			})

			lm.Flush(endLSN)

			t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のEndRecordが見つからない", txnum)
				}
			})
		})

		t.Run("Commit SetBool Record", func(t *testing.T) {
			const (
				txnum  = 2
				offset = 0
				newVal = true
			)
			ctx := context.Background()
			fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offset, newVal)
					},
				},
			)
			defer bm.Unpin(ctx, result.Buff)

			endLSN, err := rm.Commit(ctx)
			if err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			t.Run("CommitRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.CommitRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のCommitRecordが見つからない", txnum)
				}
			})

			t.Run("ATT の txnum が削除されている", func(t *testing.T) {
				if _, ok := atTbl.getTrx(txnum); ok {
					t.Errorf("tx=%d が ATT に存在するべきではない", txnum)
				}
			})

			t.Run("ページの offset 番目の値が newVal であること", func(t *testing.T) {
				if result.Buff.Contents().GetBool(offset) != newVal {
					t.Errorf("%vであるべきだが%vだった", newVal, result.Buff.Contents().GetBool(offset))
				}
			})

			lm.Flush(endLSN)

			t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のEndRecordが見つからない", txnum)
				}
			})
		})
	})

	t.Run("Rollback", func(t *testing.T) {
		t.Run("Rollback noop", func(t *testing.T) {
			const (
				txnum = 3
			)
			ctx := context.Background()
			fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

			result := runRollbackCase(t, ctx, fm, bm, rm,
				func(b *buffer.Buffer) error {
					return nil
				},
			)
			defer bm.Unpin(ctx, result.Buff)

			t.Run("txnum の AbortRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
				}
			})

			t.Run("ATT の txnum が削除されている", func(t *testing.T) {
				if _, ok := atTbl.getTrx(txnum); ok {
					t.Errorf("tx=%d が ATT に存在するべきではない", txnum)
				}
			})

			lm.Flush(result.EndLSN)

			t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のEndRecordが見つからない", txnum)
				}
			})
		})

		t.Run("Rollback single Record", func(t *testing.T) {
			t.Run("SetBool Record", func(t *testing.T) {
				const (
					txnum  = 3
					offset = 0
					oldVal = true
					newVal = false
				)
				ctx := context.Background()
				fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetBool(b, offset, newVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetBool(offset, newVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ATT の txnum が削除されている", func(t *testing.T) {
					if _, ok := atTbl.getTrx(txnum); ok {
						t.Errorf("tx=%d が ATT に存在するべきではない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetBool(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetBool(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetBoolRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のCompensationSetBoolRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の SetBoolRecord の blk と CompensationSetBoolRecord の blk が同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setBoolRecords := collectLogRecords[*logrecord.SetBoolRecord](lm, firstLSN, &txnum)
					if len(setBoolRecords) != 1 {
						t.Fatalf("tx=%d のSetBoolRecordが見つからない", txnum)
					}
					setBoolRecord := setBoolRecords[0]

					compensationSetBoolRecords := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &txnum)
					if len(compensationSetBoolRecords) != 1 {
						t.Fatalf("tx=%d のCompensationSetBoolRecordが見つからない", txnum)
					}
					compensationSetBoolRecord := compensationSetBoolRecords[0]

					t.Run("txnum の SetBoolRecord の Blk と CompensationSetBoolRecord の Blk が同じである", func(t *testing.T) {
						if !setBoolRecord.Rec.Blk().Equals(compensationSetBoolRecord.Rec.Blk()) {
							t.Fatalf("Blkが異なる値: (SetBoolRecord.Blk, CompensationSetBoolRecord.Blk) = (%s, %s)", setBoolRecord.Rec.Blk().ToString(), compensationSetBoolRecord.Rec.Blk().ToString())
						}
					})
				})
			})

			t.Run("SetDate Record", func(t *testing.T) {
				const (
					txnum  = 3
					offset = 0
				)
				var (
					oldVal = time.Unix(1_690_000_000, 0).UTC()
					newVal = time.Unix(0, 0).UTC()
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetDate(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetDate(b, offset, newVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetDate(offset, newVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ATT の txnum が削除されている", func(t *testing.T) {
					if _, ok := atTbl.getTrx(txnum); ok {
						t.Errorf("tx=%d が ATT に存在するべきではない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetDate(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetDate(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetDateRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetDateRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のCompensationSetDateRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の SetDateRecord の blk と CompensationSetDateRecord の blk が同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setDateRecords := collectLogRecords[*logrecord.SetDateRecord](lm, firstLSN, &txnum)
					if len(setDateRecords) != 1 {
						t.Fatalf("tx=%d のSetDateRecordが見つからない", txnum)
					}
					setDateRecord := setDateRecords[0]

					compensationSetDateRecords := collectLogRecords[*logrecord.CompensationSetDateRecord](lm, firstLSN, &txnum)
					if len(compensationSetDateRecords) != 1 {
						t.Fatalf("tx=%d のCompensationSetDateRecordが見つからない", txnum)
					}
					compensationSetDateRecord := compensationSetDateRecords[0]

					t.Run("txnum の SetDateRecord の Blk と CompensationSetDateRecord の Blk が同じである", func(t *testing.T) {
						if !setDateRecord.Rec.Blk().Equals(compensationSetDateRecord.Rec.Blk()) {
							t.Fatalf("Blkが異なる値: (SetDateRecord.Blk, CompensationSetDateRecord.Blk) = (%s, %s)", setDateRecord.Rec.Blk().ToString(), compensationSetDateRecord.Rec.Blk().ToString())
						}
					})
				})
			})

			t.Run("SetInt16 Record", func(t *testing.T) {
				const (
					txnum  = 3
					offset = 0
				)
				var (
					oldVal = int16(123)
					newVal = int16(999)
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetInt16(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetInt16(b, offset, newVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetInt16(offset, newVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ATT の txnum が削除されている", func(t *testing.T) {
					if _, ok := atTbl.getTrx(txnum); ok {
						t.Errorf("tx=%d が ATT に存在するべきではない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetInt16(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetInt16(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetInt16Record が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetInt16Record](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のCompensationSetInt16Recordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の SetInt16Record の blk と CompensationSetInt16Record の blk が同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setInt16Records := collectLogRecords[*logrecord.SetInt16Record](lm, firstLSN, &txnum)
					if len(setInt16Records) != 1 {
						t.Fatalf("tx=%d のSetInt16Recordが見つからない", txnum)
					}
					setInt16Record := setInt16Records[0]

					compensationSetInt16Records := collectLogRecords[*logrecord.CompensationSetInt16Record](lm, firstLSN, &txnum)
					if len(compensationSetInt16Records) != 1 {
						t.Fatalf("tx=%d のCompensationSetInt16Recordが見つからない", txnum)
					}
					compensationSetInt16Record := compensationSetInt16Records[0]

					t.Run("txnum の SetInt16Record の Blk と CompensationSetInt16Record の Blk が同じである", func(t *testing.T) {
						if !setInt16Record.Rec.Blk().Equals(compensationSetInt16Record.Rec.Blk()) {
							t.Fatalf("Blkが異なる値: (SetInt16Record.Blk, CompensationSetInt16Record.Blk) = (%s, %s)", setInt16Record.Rec.Blk().ToString(), compensationSetInt16Record.Rec.Blk().ToString())
						}
					})
				})
			})

			t.Run("SetInt32 Record", func(t *testing.T) {
				const (
					txnum  = 3
					offset = 0
				)
				var (
					oldVal = int32(123)
					newVal = int32(999)
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetInt32(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetInt32(b, offset, newVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetInt32(offset, newVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ATT の txnum が削除されている", func(t *testing.T) {
					if _, ok := atTbl.getTrx(txnum); ok {
						t.Errorf("tx=%d が ATT に存在するべきではない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetInt32(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetInt32(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetInt32Record が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Fatalf("tx=%d のCompensationSetInt32Recordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Fatalf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の SetInt32Record の blk と CompensationSetInt32Record の blk が同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setInt32Records := collectLogRecords[*logrecord.SetInt32Record](lm, firstLSN, &txnum)
					if len(setInt32Records) != 1 {
						t.Fatalf("tx=%d のSetInt32Recordが見つからない", txnum)
					}
					setInt32Record := setInt32Records[0]

					compensationSetInt32Records := collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, &txnum)
					if len(compensationSetInt32Records) != 1 {
						t.Fatalf("tx=%d のCompensationSetInt32Recordが見つからない", txnum)
					}
					compensationSetInt32Record := compensationSetInt32Records[0]

					t.Run("txnum の SetInt32Record の Blk と CompensationSetInt32Record の Blk が同じである", func(t *testing.T) {
						if !setInt32Record.Rec.Blk().Equals(compensationSetInt32Record.Rec.Blk()) {
							t.Fatalf("Blkが異なる値: (SetInt32Record.Blk, CompensationSetInt32Record.Blk) = (%s, %s)", setInt32Record.Rec.Blk().ToString(), compensationSetInt32Record.Rec.Blk().ToString())
						}
					})
				})
			})

			t.Run("SetStr Record", func(t *testing.T) {
				const (
					txnum  = 3
					offset = 0
					oldVal = "original"
					newVal = "after"
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetStr(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetStr(b, offset, newVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetStr(offset, newVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ATT の txnum が削除されている", func(t *testing.T) {
					if _, ok := atTbl.getTrx(txnum); ok {
						t.Errorf("tx=%d が ATT に存在するべきではない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetStr(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetStr(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetStrRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetStrRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のCompensationSetStrRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の SetStrRecord の blk と CompensationSetStrRecord の blk が同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setStrRecords := collectLogRecords[*logrecord.SetStrRecord](lm, firstLSN, &txnum)
					if len(setStrRecords) != 1 {
						t.Fatalf("tx=%d のSetStrRecordが見つからない", txnum)
					}
					setStrRecord := setStrRecords[0]

					compensationSetStrRecords := collectLogRecords[*logrecord.CompensationSetStrRecord](lm, firstLSN, &txnum)
					if len(compensationSetStrRecords) != 1 {
						t.Fatalf("tx=%d のCompensationSetStrRecordが見つからない", txnum)
					}
					compensationSetStrRecord := compensationSetStrRecords[0]

					t.Run("txnum の SetStrRecord の Blk と CompensationSetStrRecord の Blk が同じである", func(t *testing.T) {
						if !setStrRecord.Rec.Blk().Equals(compensationSetStrRecord.Rec.Blk()) {
							t.Errorf("Blkが異なる値: (SetStrRecord.Blk, CompensationSetStrRecord.Blk) = (%s, %s)", setStrRecord.Rec.Blk().ToString(), compensationSetStrRecord.Rec.Blk().ToString())
						}
					})
				})
			})
		})

		t.Run("Rollback Multiple Record", func(t *testing.T) {
			t.Run("Same Blk SetBool Record", func(t *testing.T) {
				const (
					txnum     = 3
					offset    = 0
					oldVal    = false
					firstVal  = true
					secondVal = true
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetBool(b, offset, firstVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetBool(offset, firstVal)
						},
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetBool(b, offset, secondVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetBool(offset, secondVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetBool(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetBool(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetBoolRecord がログに2回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &txnum)
					if len(records) != 2 {
						t.Errorf("tx=%d のCompensationSetBoolRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("対となる txnum の SetBoolRecord の blk と CompensationSetBoolRecord の blk が全て同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setBoolRecords := collectLogRecords[*logrecord.SetBoolRecord](lm, firstLSN, &txnum)
					if len(setBoolRecords) != 2 {
						t.Fatalf("tx=%d のSetBoolRecordが2つあるべきだがそうでない", txnum)
					}

					compensationSetBoolRecords := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &txnum)
					if len(compensationSetBoolRecords) != 2 {
						t.Fatalf("tx=%d のCompensationSetBoolRecordが2つあるべきだがそうでない", txnum)
					}

					for i := 0; i < len(setBoolRecords); i++ {
						setBoolRecord := setBoolRecords[i]
						clrIdx := len(compensationSetBoolRecords) - 1 - i
						compensationSetBoolRecord := compensationSetBoolRecords[clrIdx]

						t.Run("txnum の SetBoolRecord の Blk と CompensationSetBoolRecord の Blk が同じである", func(t *testing.T) {
							if !setBoolRecord.Rec.Blk().Equals(compensationSetBoolRecord.Rec.Blk()) {
								t.Errorf("(SetBoolRecord, CompensationSetBoolRecord) = (%v, %v) のBlkが異なる値: (SetBoolRecord.Blk, CompensationSetBoolRecord.Blk) = (%s, %s)", setBoolRecord, compensationSetBoolRecord, setBoolRecord.Rec.Blk().ToString(), compensationSetBoolRecord.Rec.Blk().ToString())
							}
						})
					}
				})
			})

			t.Run("Same Blk SetDate Record", func(t *testing.T) {
				const (
					txnum  = 3
					offset = 0
				)
				var (
					oldVal    = time.Unix(1_690_000_000, 0).UTC()
					firstVal  = time.Unix(0, 0).UTC()
					secondVal = time.Unix(1_700_000_000, 0).UTC()
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetDate(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetDate(b, offset, firstVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetDate(offset, firstVal)
						},
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetDate(b, offset, secondVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetDate(offset, secondVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetDate(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetDate(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetDateRecord がログに2回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetDateRecord](lm, firstLSN, &txnum)
					if len(records) != 2 {
						t.Errorf("tx=%d のCompensationSetDateRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("対となる txnum の SetDateRecord の blk と CompensationSetDateRecord の blk が全て同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setDateRecords := collectLogRecords[*logrecord.SetDateRecord](lm, firstLSN, &txnum)
					if len(setDateRecords) != 2 {
						t.Fatalf("tx=%d のSetDateRecordが2つあるべきだがそうでない", txnum)
					}

					compensationSetDateRecords := collectLogRecords[*logrecord.CompensationSetDateRecord](lm, firstLSN, &txnum)
					if len(compensationSetDateRecords) != 2 {
						t.Fatalf("tx=%d のCompensationSetDateRecordが2つあるべきだがそうでない", txnum)
					}

					for i := 0; i < len(setDateRecords); i++ {
						setDateRecord := setDateRecords[i]
						clrIdx := len(compensationSetDateRecords) - 1 - i
						compensationSetDateRecord := compensationSetDateRecords[clrIdx]

						t.Run("txnum の SetDateRecord の Blk と CompensationSetDateRecord の Blk が同じである", func(t *testing.T) {
							if !setDateRecord.Rec.Blk().Equals(compensationSetDateRecord.Rec.Blk()) {
								t.Errorf("(SetDateRecord, CompensationSetDateRecord) = (%v, %v) のBlkが異なる値: (SetDateRecord.Blk, CompensationSetDateRecord.Blk) = (%s, %s)", setDateRecord, compensationSetDateRecord, setDateRecord.Rec.Blk().ToString(), compensationSetDateRecord.Rec.Blk().ToString())
							}
						})
					}
				})
			})

			t.Run("Same Blk SetInt16 Record", func(t *testing.T) {
				const (
					txnum  = 3
					offset = 0
				)
				var (
					oldVal    = int16(123)
					firstVal  = int16(999)
					secondVal = int16(1000)
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetInt16(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetInt16(b, offset, firstVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetInt16(offset, firstVal)
						},
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetInt16(b, offset, secondVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetInt16(offset, secondVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetInt16(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetInt16(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetInt16Record がログに2回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetInt16Record](lm, firstLSN, &txnum)
					if len(records) != 2 {
						t.Errorf("tx=%d のCompensationSetInt16Recordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("対となる txnum の SetInt16Record の blk と CompensationSetInt16Record の blk が全て同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setInt16Records := collectLogRecords[*logrecord.SetInt16Record](lm, firstLSN, &txnum)
					if len(setInt16Records) != 2 {
						t.Fatalf("tx=%d のSetInt16Recordが2つあるべきだがそうでない", txnum)
					}

					compensationSetInt16Records := collectLogRecords[*logrecord.CompensationSetInt16Record](lm, firstLSN, &txnum)
					if len(compensationSetInt16Records) != 2 {
						t.Fatalf("tx=%d のCompensationSetInt16Recordが2つあるべきだがそうでない", txnum)
					}

					for i := 0; i < len(setInt16Records); i++ {
						setInt16Record := setInt16Records[i]
						clrIdx := len(compensationSetInt16Records) - 1 - i
						compensationSetInt16Record := compensationSetInt16Records[clrIdx]

						t.Run("txnum の SetInt16Record の Blk と CompensationSetInt16Record の Blk が同じである", func(t *testing.T) {
							if !setInt16Record.Rec.Blk().Equals(compensationSetInt16Record.Rec.Blk()) {
								t.Errorf("(SetInt16Record, CompensationSetInt16Record) = (%v, %v) のBlkが異なる値: (SetInt16Record.Blk, CompensationSetInt16Record.Blk) = (%s, %s)", setInt16Record, compensationSetInt16Record, setInt16Record.Rec.Blk().ToString(), compensationSetInt16Record.Rec.Blk().ToString())
							}
						})
					}
				})
			})

			t.Run("Same Blk SetInt32 Record", func(t *testing.T) {
				const (
					txnum  = 3
					offset = 0
				)
				var (
					oldVal    = int32(123)
					firstVal  = int32(999)
					secondVal = int32(1000)
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetInt32(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetInt32(b, offset, firstVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetInt32(offset, firstVal)
						},
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetInt32(b, offset, secondVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetInt32(offset, secondVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetInt32(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetInt32(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetInt32Record がログに2回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, &txnum)
					if len(records) != 2 {
						t.Errorf("tx=%d のCompensationSetInt32Recordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("対となる txnum の SetInt32Record の blk と CompensationSetInt32Record の blk が全て同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setInt32Records := collectLogRecords[*logrecord.SetInt32Record](lm, firstLSN, &txnum)
					if len(setInt32Records) != 2 {
						t.Fatalf("tx=%d のSetInt32Recordが2つあるべきだがそうでない", txnum)
					}

					compensationSetInt32Records := collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, &txnum)
					if len(compensationSetInt32Records) != 2 {
						t.Fatalf("tx=%d のCompensationSetInt32Recordが2つあるべきだがそうでない", txnum)
					}

					for i := 0; i < len(setInt32Records); i++ {
						setInt32Record := setInt32Records[i]
						clrIdx := len(compensationSetInt32Records) - 1 - i
						compensationSetInt32Record := compensationSetInt32Records[clrIdx]

						t.Run("txnum の SetInt32Record の Blk と CompensationSetInt32Record の Blk が同じである", func(t *testing.T) {
							if !setInt32Record.Rec.Blk().Equals(compensationSetInt32Record.Rec.Blk()) {
								t.Errorf("(SetInt32Record, CompensationSetInt32Record) = (%v, %v) のBlkが異なる値: (SetInt32Record.Blk, CompensationSetInt32Record.Blk) = (%s, %s)", setInt32Record, compensationSetInt32Record, setInt32Record.Rec.Blk().ToString(), compensationSetInt32Record.Rec.Blk().ToString())
							}
						})
					}
				})
			})

			t.Run("Same Blk SetStr Record", func(t *testing.T) {
				const (
					txnum     = 3
					offset    = 0
					oldVal    = "original"
					firstVal  = "after"
					secondVal = "after2"
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

				result := runRollbackCase(t, ctx, fm, bm, rm,
					func(b *buffer.Buffer) error {
						return b.Contents().SetStr(offset, oldVal)
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetStr(b, offset, firstVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetStr(offset, firstVal)
						},
					},
					setStep{
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetStr(b, offset, secondVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetStr(offset, secondVal)
						},
					},
				)
				defer bm.Unpin(ctx, result.Buff)

				t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					if result.Buff.Contents().GetStr(offset) != oldVal {
						t.Errorf("%vであるべきだが%vだった", oldVal, result.Buff.Contents().GetStr(offset))
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetStrRecord がログに2回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetStrRecord](lm, firstLSN, &txnum)
					if len(records) != 2 {
						t.Errorf("tx=%d のCompensationSetStrRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("対となる txnum の SetStrRecord の blk と CompensationSetStrRecord の blk が全て同じである", func(t *testing.T) {
					txnum := int32(txnum)

					setStrRecords := collectLogRecords[*logrecord.SetStrRecord](lm, firstLSN, &txnum)
					if len(setStrRecords) != 2 {
						t.Fatalf("tx=%d のSetStrRecordが2つあるべきだがそうでない", txnum)
					}

					compensationSetStrRecords := collectLogRecords[*logrecord.CompensationSetStrRecord](lm, firstLSN, &txnum)
					if len(compensationSetStrRecords) != 2 {
						t.Fatalf("tx=%d のCompensationSetStrRecordが2つあるべきだがそうでない", txnum)
					}

					for i := 0; i < len(setStrRecords); i++ {
						setStrRecord := setStrRecords[i]
						clrIdx := len(compensationSetStrRecords) - 1 - i
						compensationSetStrRecord := compensationSetStrRecords[clrIdx]

						t.Run("txnum の SetStrRecord の Blk と CompensationSetStrRecord の Blk が同じである", func(t *testing.T) {
							if !setStrRecord.Rec.Blk().Equals(compensationSetStrRecord.Rec.Blk()) {
								t.Errorf("(SetStrRecord, CompensationSetStrRecord) = (%v, %v) のBlkが異なる値: (SetStrRecord.Blk, CompensationSetStrRecord.Blk) = (%s, %s)", setStrRecord, compensationSetStrRecord, setStrRecord.Rec.Blk().ToString(), compensationSetStrRecord.Rec.Blk().ToString())
							}
						})
					}
				})
			})

			t.Run("Different Blk SetBool Record", func(t *testing.T) {
				const (
					txnum     = 3
					offset    = 0
					oldVal    = false
					firstVal  = true
					secondVal = true
				)

				ctx := context.Background()
				fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

				result := runRollbackMultiBlkCase(t, ctx, fm, bm, rm, 2,
					[]func(*buffer.Buffer) error{
						func(b *buffer.Buffer) error {
							return b.Contents().SetBool(offset, oldVal)
						},
						func(b *buffer.Buffer) error {
							return b.Contents().SetBool(offset, oldVal)
						},
					},
					multiBlkStep{
						page: 0,
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetBool(b, offset, firstVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetBool(offset, firstVal)
						},
					},
					multiBlkStep{
						page: 1,
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetBool(b, offset, secondVal)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetBool(offset, secondVal)
						},
					},
				)

				for _, buff := range result.Buffs {
					defer bm.Unpin(ctx, buff)
				}

				t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
					}
				})

				t.Run("各ページの offset 番目の値が oldVal であること", func(t *testing.T) {
					for _, buff := range result.Buffs {
						t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
							if buff.Contents().GetBool(offset) != oldVal {
								t.Errorf("%vであるべきだが%vだった", oldVal, buff.Contents().GetBool(offset))
							}
						})
					}
				})

				lm.Flush(result.EndLSN)

				t.Run("txnum の CompensationSetBoolRecord がログに2回書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &txnum)
					if len(records) != 2 {
						t.Errorf("tx=%d のCompensationSetBoolRecordが見つからない", txnum)
					}
				})

				t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
					txnum := int32(txnum)
					records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
					if len(records) != 1 {
						t.Errorf("tx=%d のEndRecordが見つからない", txnum)
					}
				})

				t.Run("CompensationSetBoolRecord の PrevLSN と UndoNextLSN が適切であること", func(t *testing.T) {
					txnum := int32(txnum)

					starts := collectLogRecords[*logrecord.StartRecord](lm, firstLSN, &txnum)
					if len(starts) != 1 {
						t.Fatalf("tx=%d のStartRecordが1つあるべきだがそうでない", txnum)
					}

					sets := collectLogRecords[*logrecord.SetBoolRecord](lm, firstLSN, &txnum)
					if len(sets) != 2 {
						t.Fatalf("tx=%d のSetBoolRecordが2つあるべきだがそうでない", txnum)
					}

					aborts := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
					if len(aborts) != 1 {
						t.Fatalf("tx=%d のAbortRecordが1つあるべきだがそうでない", txnum)
					}

					clrs := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &txnum)
					if len(clrs) != 2 {
						t.Fatalf("tx=%d のCompensationSetBoolRecordが2つあるべきだがそうでない", txnum)
					}

					t.Run("1個目の CompensationSetBoolRecord の PrevLSN が 同一txnum の AbortRercord の LSN である", func(t *testing.T) {
						if clrs[0].Rec.PrevLSN() != aborts[0].LSN {
							t.Errorf("1個目の CompensationSetBoolRecord の PrevLSN が %d であるべきだが %d だった", aborts[0].LSN, clrs[0].Rec.PrevLSN())
						}
					})

					t.Run("2個目の CompensationSetBoolRecord の PrevLSN が 同一txnum の1個目の CompensationSetBoolRecord の LSN である", func(t *testing.T) {
						if clrs[1].Rec.PrevLSN() != clrs[0].LSN {
							t.Errorf("2個目の CompensationSetBoolRecord の PrevLSN が %d であるべきだが %d だった", clrs[0].LSN, clrs[1].Rec.PrevLSN())
						}
					})

					t.Run("1個目の CompensationSetBoolRecord の UndoNextLSN が 同一txnum の StartRecord の LSN である", func(t *testing.T) {
						if clrs[0].Rec.UndoNextLSN() != sets[0].LSN {
							t.Errorf("1個目の CompensationSetBoolRecord の UndoNextLSN が %d であるべきだが %d だった", sets[0].LSN, clrs[0].Rec.UndoNextLSN())
						}
					})

					t.Run("2個目の CompensationSetBoolRecord の UndoNextLSN が 同一txnum の 1個目の SetBoolRecord の LSN である", func(t *testing.T) {
						if clrs[1].Rec.UndoNextLSN() != starts[0].LSN {
							t.Errorf("2個目の CompensationSetBoolRecord の UndoNextLSN が %d であるべきだが %d だった", starts[0].LSN, clrs[1].Rec.UndoNextLSN())
						}
					})
				})

				t.Run("Different Blk SetDate Record", func(t *testing.T) {
					const (
						txnum  = 3
						offset = 0
					)
					var (
						oldVal    = time.Unix(1_690_000_000, 0).UTC()
						firstVal  = time.Unix(1_700_000_000, 0).UTC()
						secondVal = time.Unix(1_710_000_000, 0).UTC()
					)

					ctx := context.Background()
					fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

					result := runRollbackMultiBlkCase(t, ctx, fm, bm, rm, 2,
						[]func(*buffer.Buffer) error{
							func(b *buffer.Buffer) error {
								return b.Contents().SetDate(offset, oldVal)
							},
							func(b *buffer.Buffer) error {
								return b.Contents().SetDate(offset, oldVal)
							},
						},
						multiBlkStep{
							page: 0,
							log: func(b *buffer.Buffer) (int32, error) {
								return rm.SetDate(b, offset, firstVal)
							},
							apply: func(b *buffer.Buffer) error {
								return b.Contents().SetDate(offset, firstVal)
							},
						},
						multiBlkStep{
							page: 1,
							log: func(b *buffer.Buffer) (int32, error) {
								return rm.SetDate(b, offset, secondVal)
							},
							apply: func(b *buffer.Buffer) error {
								return b.Contents().SetDate(offset, secondVal)
							},
						},
					)

					for _, buff := range result.Buffs {
						defer bm.Unpin(ctx, buff)
					}

					t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
						if len(records) != 1 {
							t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
						}
					})

					t.Run("各ページの offset 番目の値が oldVal であること", func(t *testing.T) {
						for _, buff := range result.Buffs {
							t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
								if buff.Contents().GetDate(offset) != oldVal {
									t.Errorf("%vであるべきだが%vだった", oldVal, buff.Contents().GetDate(offset))
								}
							})
						}
					})

					lm.Flush(result.EndLSN)

					t.Run("txnum の CompensationSetDateRecord がログに2回書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.CompensationSetDateRecord](lm, firstLSN, &txnum)
						if len(records) != 2 {
							t.Errorf("tx=%d のCompensationSetDateRecordが見つからない", txnum)
						}
					})

					t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
						if len(records) != 1 {
							t.Errorf("tx=%d のEndRecordが見つからない", txnum)
						}
					})

					t.Run("CompensationSetDateRecord の PrevLSN と UndoNextLSN が適切であること", func(t *testing.T) {
						txnum := int32(txnum)

						starts := collectLogRecords[*logrecord.StartRecord](lm, firstLSN, &txnum)
						if len(starts) != 1 {
							t.Fatalf("tx=%d のStartRecordが1つあるべきだがそうでない", txnum)
						}

						sets := collectLogRecords[*logrecord.SetDateRecord](lm, firstLSN, &txnum)
						if len(sets) != 2 {
							t.Fatalf("tx=%d のSetDateRecordが2つあるべきだがそうでない", txnum)
						}

						aborts := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
						if len(aborts) != 1 {
							t.Fatalf("tx=%d のAbortRecordが1つあるべきだがそうでない", txnum)
						}

						clrs := collectLogRecords[*logrecord.CompensationSetDateRecord](lm, firstLSN, &txnum)
						if len(clrs) != 2 {
							t.Fatalf("tx=%d のCompensationSetDateRecordが2つあるべきだがそうでない", txnum)
						}

						t.Run("1個目の CompensationSetDateRecord の PrevLSN が 同一txnum の AbortRercord の LSN である", func(t *testing.T) {
							if clrs[0].Rec.PrevLSN() != aborts[0].LSN {
								t.Errorf("1個目の CompensationSetDateRecord の PrevLSN が %d であるべきだが %d だった", aborts[0].LSN, clrs[0].Rec.PrevLSN())
							}
						})

						t.Run("2個目の CompensationSetDateRecord の PrevLSN が 同一txnum の1個目の CompensationSetDateRecord の LSN である", func(t *testing.T) {
							if clrs[1].Rec.PrevLSN() != clrs[0].LSN {
								t.Errorf("2個目の CompensationSetDateRecord の PrevLSN が %d であるべきだが %d だった", clrs[0].LSN, clrs[1].Rec.PrevLSN())
							}
						})

						t.Run("1個目の CompensationSetDateRecord の UndoNextLSN が 同一txnum の StartRecord の LSN である", func(t *testing.T) {
							if clrs[0].Rec.UndoNextLSN() != sets[0].LSN {
								t.Errorf("1個目の CompensationSetDateRecord の UndoNextLSN が %d であるべきだが %d だった", sets[0].LSN, clrs[0].Rec.UndoNextLSN())
							}
						})

						t.Run("2個目の CompensationSetDateRecord の UndoNextLSN が 同一txnum の 1個目の SetDateRecord の LSN である", func(t *testing.T) {
							if clrs[1].Rec.UndoNextLSN() != starts[0].LSN {
								t.Errorf("2個目の CompensationSetDateRecord の UndoNextLSN が %d であるべきだが %d だった", starts[0].LSN, clrs[1].Rec.UndoNextLSN())
							}
						})
					})
				})

				t.Run("Different Blk SetInt16 Record", func(t *testing.T) {
					const (
						txnum  = 3
						offset = 0
					)
					var (
						oldVal    = int16(100)
						firstVal  = int16(200)
						secondVal = int16(300)
					)

					ctx := context.Background()
					fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

					result := runRollbackMultiBlkCase(t, ctx, fm, bm, rm, 2,
						[]func(*buffer.Buffer) error{
							func(b *buffer.Buffer) error {
								return b.Contents().SetInt16(offset, oldVal)
							},
							func(b *buffer.Buffer) error {
								return b.Contents().SetInt16(offset, oldVal)
							},
						},
						multiBlkStep{
							page: 0,
							log: func(b *buffer.Buffer) (int32, error) {
								return rm.SetInt16(b, offset, firstVal)
							},
							apply: func(b *buffer.Buffer) error {
								return b.Contents().SetInt16(offset, firstVal)
							},
						},
						multiBlkStep{
							page: 1,
							log: func(b *buffer.Buffer) (int32, error) {
								return rm.SetInt16(b, offset, secondVal)
							},
							apply: func(b *buffer.Buffer) error {
								return b.Contents().SetInt16(offset, secondVal)
							},
						},
					)

					for _, buff := range result.Buffs {
						defer bm.Unpin(ctx, buff)
					}

					t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
						if len(records) != 1 {
							t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
						}
					})

					t.Run("各ページの offset 番目の値が oldVal であること", func(t *testing.T) {
						for _, buff := range result.Buffs {
							t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
								if buff.Contents().GetInt16(offset) != oldVal {
									t.Errorf("%vであるべきだが%vだった", oldVal, buff.Contents().GetInt16(offset))
								}
							})
						}
					})

					lm.Flush(result.EndLSN)

					t.Run("txnum の CompensationSetInt16Record がログに2回書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.CompensationSetInt16Record](lm, firstLSN, &txnum)
						if len(records) != 2 {
							t.Errorf("tx=%d のCompensationSetInt16Recordが見つからない", txnum)
						}
					})

					t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
						if len(records) != 1 {
							t.Errorf("tx=%d のEndRecordが見つからない", txnum)
						}
					})

					t.Run("CompensationSetInt16Record の PrevLSN と UndoNextLSN が適切であること", func(t *testing.T) {
						txnum := int32(txnum)

						starts := collectLogRecords[*logrecord.StartRecord](lm, firstLSN, &txnum)
						if len(starts) != 1 {
							t.Fatalf("tx=%d のStartRecordが1つあるべきだがそうでない", txnum)
						}

						sets := collectLogRecords[*logrecord.SetInt16Record](lm, firstLSN, &txnum)
						if len(sets) != 2 {
							t.Fatalf("tx=%d のSetInt16Recordが2つあるべきだがそうでない", txnum)
						}

						aborts := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
						if len(aborts) != 1 {
							t.Fatalf("tx=%d のAbortRecordが1つあるべきだがそうでない", txnum)
						}

						clrs := collectLogRecords[*logrecord.CompensationSetInt16Record](lm, firstLSN, &txnum)
						if len(clrs) != 2 {
							t.Fatalf("tx=%d のCompensationSetInt16Recordが2つあるべきだがそうでない", txnum)
						}

						t.Run("1個目の CompensationSetInt16Record の PrevLSN が 同一txnum の AbortRercord の LSN である", func(t *testing.T) {
							if clrs[0].Rec.PrevLSN() != aborts[0].LSN {
								t.Errorf("1個目の CompensationSetInt16Record の PrevLSN が %d であるべきだが %d だった", aborts[0].LSN, clrs[0].Rec.PrevLSN())
							}
						})

						t.Run("2個目の CompensationSetInt16Record の PrevLSN が 同一txnum の1個目の CompensationSetInt16Record の LSN である", func(t *testing.T) {
							if clrs[1].Rec.PrevLSN() != clrs[0].LSN {
								t.Errorf("2個目の CompensationSetInt16Record の PrevLSN が %d であるべきだが %d だった", clrs[0].LSN, clrs[1].Rec.PrevLSN())
							}
						})

						t.Run("1個目の CompensationSetInt16Record の UndoNextLSN が 同一txnum の StartRecord の LSN である", func(t *testing.T) {
							if clrs[0].Rec.UndoNextLSN() != sets[0].LSN {
								t.Errorf("1個目の CompensationSetInt16Record の UndoNextLSN が %d であるべきだが %d だった", sets[0].LSN, clrs[0].Rec.UndoNextLSN())
							}
						})

						t.Run("2個目の CompensationSetInt16Record の UndoNextLSN が 同一txnum の 1個目の SetInt16Record の LSN である", func(t *testing.T) {
							if clrs[1].Rec.UndoNextLSN() != starts[0].LSN {
								t.Errorf("2個目の CompensationSetInt16Record の UndoNextLSN が %d であるべきだが %d だった", starts[0].LSN, clrs[1].Rec.UndoNextLSN())
							}
						})
					})
				})

				t.Run("Different Blk SetInt32 Record", func(t *testing.T) {
					const (
						txnum  = 3
						offset = 0
					)
					var (
						oldVal    = int32(100)
						firstVal  = int32(200)
						secondVal = int32(300)
					)

					ctx := context.Background()
					fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

					result := runRollbackMultiBlkCase(t, ctx, fm, bm, rm, 2,
						[]func(*buffer.Buffer) error{
							func(b *buffer.Buffer) error {
								return b.Contents().SetInt32(offset, oldVal)
							},
							func(b *buffer.Buffer) error {
								return b.Contents().SetInt32(offset, oldVal)
							},
						},
						multiBlkStep{
							page: 0,
							log: func(b *buffer.Buffer) (int32, error) {
								return rm.SetInt32(b, offset, firstVal)
							},
							apply: func(b *buffer.Buffer) error {
								return b.Contents().SetInt32(offset, firstVal)
							},
						},
						multiBlkStep{
							page: 1,
							log: func(b *buffer.Buffer) (int32, error) {
								return rm.SetInt32(b, offset, secondVal)
							},
							apply: func(b *buffer.Buffer) error {
								return b.Contents().SetInt32(offset, secondVal)
							},
						},
					)

					for _, buff := range result.Buffs {
						defer bm.Unpin(ctx, buff)
					}

					t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
						if len(records) != 1 {
							t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
						}
					})

					t.Run("各ページの offset 番目の値が oldVal であること", func(t *testing.T) {
						for _, buff := range result.Buffs {
							t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
								if buff.Contents().GetInt32(offset) != oldVal {
									t.Errorf("%vであるべきだが%vだった", oldVal, buff.Contents().GetInt32(offset))
								}
							})
						}
					})

					lm.Flush(result.EndLSN)

					t.Run("txnum の CompensationSetInt32Record がログに2回書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, &txnum)
						if len(records) != 2 {
							t.Errorf("tx=%d のCompensationSetInt32Recordが見つからない", txnum)
						}
					})

					t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
						if len(records) != 1 {
							t.Errorf("tx=%d のEndRecordが見つからない", txnum)
						}
					})

					t.Run("CompensationSetInt32Record の PrevLSN と UndoNextLSN が適切であること", func(t *testing.T) {
						txnum := int32(txnum)

						starts := collectLogRecords[*logrecord.StartRecord](lm, firstLSN, &txnum)
						if len(starts) != 1 {
							t.Fatalf("tx=%d のStartRecordが1つあるべきだがそうでない", txnum)
						}

						sets := collectLogRecords[*logrecord.SetInt32Record](lm, firstLSN, &txnum)
						if len(sets) != 2 {
							t.Fatalf("tx=%d のSetInt32Recordが2つあるべきだがそうでない", txnum)
						}

						aborts := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
						if len(aborts) != 1 {
							t.Fatalf("tx=%d のAbortRecordが1つあるべきだがそうでない", txnum)
						}

						clrs := collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, &txnum)
						if len(clrs) != 2 {
							t.Fatalf("tx=%d のCompensationSetInt32Recordが2つあるべきだがそうでない", txnum)
						}

						t.Run("1個目の CompensationSetInt32Record の PrevLSN が 同一txnum の AbortRercord の LSN である", func(t *testing.T) {
							if clrs[0].Rec.PrevLSN() != aborts[0].LSN {
								t.Errorf("1個目の CompensationSetInt32Record の PrevLSN が %d であるべきだが %d だった", aborts[0].LSN, clrs[0].Rec.PrevLSN())
							}
						})

						t.Run("2個目の CompensationSetInt32Record の PrevLSN が 同一txnum の1個目の CompensationSetInt32Record の LSN である", func(t *testing.T) {
							if clrs[1].Rec.PrevLSN() != clrs[0].LSN {
								t.Errorf("2個目の CompensationSetInt32Record の PrevLSN が %d であるべきだが %d だった", clrs[0].LSN, clrs[1].Rec.PrevLSN())
							}
						})

						t.Run("1個目の CompensationSetInt32Record の UndoNextLSN が 同一txnum の StartRecord の LSN である", func(t *testing.T) {
							if clrs[0].Rec.UndoNextLSN() != sets[0].LSN {
								t.Errorf("1個目の CompensationSetInt32Record の UndoNextLSN が %d であるべきだが %d だった", sets[0].LSN, clrs[0].Rec.UndoNextLSN())
							}
						})

						t.Run("2個目の CompensationSetInt32Record の UndoNextLSN が 同一txnum の 1個目の SetInt32Record の LSN である", func(t *testing.T) {
							if clrs[1].Rec.UndoNextLSN() != starts[0].LSN {
								t.Errorf("2個目の CompensationSetInt32Record の UndoNextLSN が %d であるべきだが %d だった", starts[0].LSN, clrs[1].Rec.UndoNextLSN())
							}
						})
					})
				})

				t.Run("Different Blk SetStr Record", func(t *testing.T) {
					const (
						txnum     = 3
						offset    = 0
						oldVal    = "origin"
						firstVal  = "first"
						secondVal = "second"
					)

					ctx := context.Background()
					fm, lm, bm, _, rm, _, _ := initRecoveryEnv(t, txnum)

					result := runRollbackMultiBlkCase(t, ctx, fm, bm, rm, 2,
						[]func(*buffer.Buffer) error{
							func(b *buffer.Buffer) error {
								return b.Contents().SetStr(offset, oldVal)
							},
							func(b *buffer.Buffer) error {
								return b.Contents().SetStr(offset, oldVal)
							},
						},
						multiBlkStep{
							page: 0,
							log: func(b *buffer.Buffer) (int32, error) {
								return rm.SetStr(b, offset, firstVal)
							},
							apply: func(b *buffer.Buffer) error {
								return b.Contents().SetStr(offset, firstVal)
							},
						},
						multiBlkStep{
							page: 1,
							log: func(b *buffer.Buffer) (int32, error) {
								return rm.SetStr(b, offset, secondVal)
							},
							apply: func(b *buffer.Buffer) error {
								return b.Contents().SetStr(offset, secondVal)
							},
						},
					)

					for _, buff := range result.Buffs {
						defer bm.Unpin(ctx, buff)
					}

					t.Run("txnum の AbortRecord がログに1回書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
						if len(records) != 1 {
							t.Errorf("tx=%d のAbortRecordが見つからない", txnum)
						}
					})

					t.Run("各ページの offset 番目の値が oldVal であること", func(t *testing.T) {
						for _, buff := range result.Buffs {
							t.Run("ページの offset 番目の値が oldVal であること", func(t *testing.T) {
								if buff.Contents().GetStr(offset) != oldVal {
									t.Errorf("%vであるべきだが%vだった", oldVal, buff.Contents().GetStr(offset))
								}
							})
						}
					})

					lm.Flush(result.EndLSN)

					t.Run("txnum の CompensationSetStrRecord がログに2回書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.CompensationSetStrRecord](lm, firstLSN, &txnum)
						if len(records) != 2 {
							t.Errorf("tx=%d のCompensationSetStrRecordが見つからない", txnum)
						}
					})

					t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
						txnum := int32(txnum)
						records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
						if len(records) != 1 {
							t.Errorf("tx=%d のEndRecordが見つからない", txnum)
						}
					})

					t.Run("CompensationSetStrRecord の PrevLSN と UndoNextLSN が適切であること", func(t *testing.T) {
						txnum := int32(txnum)

						starts := collectLogRecords[*logrecord.StartRecord](lm, firstLSN, &txnum)
						if len(starts) != 1 {
							t.Fatalf("tx=%d のStartRecordが1つあるべきだがそうでない", txnum)
						}

						sets := collectLogRecords[*logrecord.SetStrRecord](lm, firstLSN, &txnum)
						if len(sets) != 2 {
							t.Fatalf("tx=%d のSetStrRecordが2つあるべきだがそうでない", txnum)
						}

						aborts := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
						if len(aborts) != 1 {
							t.Fatalf("tx=%d のAbortRecordが1つあるべきだがそうでない", txnum)
						}

						clrs := collectLogRecords[*logrecord.CompensationSetStrRecord](lm, firstLSN, &txnum)
						if len(clrs) != 2 {
							t.Fatalf("tx=%d のCompensationSetStrRecordが2つあるべきだがそうでない", txnum)
						}

						t.Run("1個目の CompensationSetStrRecord の PrevLSN が 同一txnum の AbortRercord の LSN である", func(t *testing.T) {
							if clrs[0].Rec.PrevLSN() != aborts[0].LSN {
								t.Errorf("1個目の CompensationSetStrRecord の PrevLSN が %d であるべきだが %d だった", aborts[0].LSN, clrs[0].Rec.PrevLSN())
							}
						})

						t.Run("2個目の CompensationSetStrRecord の PrevLSN が 同一txnum の1個目の CompensationSetStrRecord の LSN である", func(t *testing.T) {
							if clrs[1].Rec.PrevLSN() != clrs[0].LSN {
								t.Errorf("2個目の CompensationSetStrRecord の PrevLSN が %d であるべきだが %d だった", clrs[0].LSN, clrs[1].Rec.PrevLSN())
							}
						})

						t.Run("1個目の CompensationSetStrRecord の UndoNextLSN が 同一txnum の StartRecord の LSN である", func(t *testing.T) {
							if clrs[0].Rec.UndoNextLSN() != sets[0].LSN {
								t.Errorf("1個目の CompensationSetStrRecord の UndoNextLSN が %d であるべきだが %d だった", sets[0].LSN, clrs[0].Rec.UndoNextLSN())
							}
						})

						t.Run("2個目の CompensationSetStrRecord の UndoNextLSN が 同一txnum の 1個目の SetStrRecord の LSN である", func(t *testing.T) {
							if clrs[1].Rec.UndoNextLSN() != starts[0].LSN {
								t.Errorf("2個目の CompensationSetStrRecord の UndoNextLSN が %d であるべきだが %d だった", starts[0].LSN, clrs[1].Rec.UndoNextLSN())
							}
						})
					})
				})
			})
		})
	})

	t.Run("Checkpoint", func(t *testing.T) {
		const (
			txnum = access.RecoveryTxNum
		)
		ctx := context.Background()
		_, lm, _, _, rm, _, _ := initRecoveryEnvForRecover(t)
		if _, err := rm.Checkpoint(ctx); err != nil {
			t.Fatalf("Checkpoint failed: %v", err)
		}

		t.Run("CheckpointBeginRecord が1回ログに書き込まれている", func(t *testing.T) {
			txnum := int32(txnum)
			records := collectLogRecords[*logrecord.CheckpointBeginRecord](lm, firstLSN, nil)
			if len(records) != 1 {
				t.Errorf("tx=%d のCheckpointBeginRecordが見つからない", txnum)
			}
		})

		t.Run("CheckpointEndRecord が1回ログに書き込まれている", func(t *testing.T) {
			txnum := int32(txnum)
			records := collectLogRecords[*logrecord.CheckpointEndRecord](lm, firstLSN, nil)
			if len(records) != 1 {
				t.Errorf("tx=%d のCheckpointEndRecordが見つからない", txnum)
			}
		})

		t.Run("ReadMasterLSN で CheckpointBeginRecord の LSN が取得できる", func(t *testing.T) {
			endRecords := collectLogRecords[*logrecord.CheckpointEndRecord](lm, firstLSN, nil)
			if len(endRecords) != 1 {
				t.Fatalf("CheckpointEndRecord expected 1, got %d", len(endRecords))
			}
			checkpointBeginLSN := endRecords[0].Rec.BeginLSN()

			masterLSN, err := lm.ReadMasterLSN()
			if err != nil {
				t.Fatalf("ReadMasterLSN failed: %v", err)
			}
			if masterLSN != checkpointBeginLSN {
				t.Errorf("CheckpointBeginRecord の LSN が取得できない: expected %d, got %d", checkpointBeginLSN, masterLSN)
			}
		})
	})

	t.Run("Recover", func(t *testing.T) {
		const (
			runningTxnum         = 3
			committedNotEndTxnum = 4
			committedTxnum       = 5
			abortedNotEndTxnum   = 6
			abortedTxnum         = 7
			offsetBool           = 0
			offsetInt32          = 8
			offsetStr            = 16
			offsetDate           = 48
			oldBool              = false
			newBool              = true
			oldInt32             = int32(10)
			newInt32             = int32(999)
			oldStr               = "origin"
			newStr               = "updated"
		)
		oldDate := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
		newDate := time.Date(2024, time.February, 2, 0, 0, 0, 0, time.UTC)
		base := initRecoveryBase(t)
		mixedInitVals := []func(*buffer.Buffer) error{
			func(b *buffer.Buffer) error {
				if err := b.Contents().SetBool(offsetBool, oldBool); err != nil {
					return err
				}
				if err := b.Contents().SetStr(offsetStr, oldStr); err != nil {
					return err
				}
				return nil
			},
			func(b *buffer.Buffer) error {
				if err := b.Contents().SetInt32(offsetInt32, oldInt32); err != nil {
					return err
				}
				if err := b.Contents().SetDate(offsetDate, oldDate); err != nil {
					return err
				}
				return nil
			},
		}
		buildMixedSteps := func(rm *RecoveryMgr) []multiBlkStep {
			return []multiBlkStep{
				{
					page: 0,
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetBool(b, offsetBool, newBool)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offsetBool, newBool)
					},
				},
				{
					page: 1,
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetInt32(b, offsetInt32, newInt32)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetInt32(offsetInt32, newInt32)
					},
				},
				{
					page: 0,
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetStr(b, offsetStr, newStr)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetStr(offsetStr, newStr)
					},
				},
				{
					page: 1,
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetDate(b, offsetDate, newDate)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetDate(offsetDate, newDate)
					},
				},
			}
		}

		// 未コミットのトランザクション
		{
			ctx := context.Background()
			fm, _, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, runningTxnum)

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)

			for _, item := range result.Items {
				bm.Unpin(ctx, item.Buff)
			}
		}

		// コミット済みのトランザクション(end未出力)
		{
			ctx := context.Background()
			fm, lm, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, committedNotEndTxnum)

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)

			commitLSN, err := logrecord.WriteCommitToLog(lm, result.LastLSN, committedNotEndTxnum)
			if err != nil {
				t.Fatalf("WriteCommitToLog failed: %v", err)
			}
			rm.lm.Flush(commitLSN)

			for _, item := range result.Items {
				bm.Unpin(ctx, item.Buff)
			}
		}

		// コミット済みのトランザクション
		var committedItems []setBlkResult
		{
			ctx := context.Background()
			fm, _, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, committedTxnum)

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)
			committedItems = result.Items

			if _, err := rm.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			for _, item := range result.Items {
				bm.Unpin(ctx, item.Buff)
			}
		}

		// aborted済みのトランザクション(end未出力)
		{
			ctx := context.Background()
			fm, lm, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, abortedNotEndTxnum)

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)

			abortLSN, err := logrecord.WriteAbortToLog(lm, result.LastLSN, abortedNotEndTxnum)
			if err != nil {
				t.Fatalf("WriteAbortToLog failed: %v", err)
			}
			lm.Flush(abortLSN)

			for _, item := range result.Items {
				bm.Unpin(ctx, item.Buff)
			}
		}

		// aborted済みのトランザクション
		{
			ctx := context.Background()
			fm, _, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, abortedTxnum)

			result := runRollbackMultiBlkCase(t, ctx, fm, bm, rm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)

			for _, buff := range result.Buffs {
				bm.Unpin(ctx, buff)
			}
		}

		ctx := context.Background()
		_, lm, _, _, rm, _, _ := initRecoveryEnvForRecoverOnBase(t, base)
		if err := rm.Recover(ctx); err != nil {
			t.Fatalf("Recover failed: %v", err)
		}

		t.Run("コミット済みのトランザクション(end未出力) の EndRecord がログに書き込まれている", func(t *testing.T) {
			txnum := int32(committedNotEndTxnum)
			records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
			if len(records) != 1 {
				t.Errorf("tx=%d のEndRecordが見つからない", txnum)
			}
		})

		t.Run("abort済みのトランザクション(end未出力) の EndRecord がログに書き込まれている", func(t *testing.T) {
			txnum := int32(abortedNotEndTxnum)
			records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
			if len(records) != 1 {
				t.Errorf("tx=%d のEndRecordが見つからない", txnum)
			}
		})

		t.Run("Recover 複数回実行による冪等性が担保される", func(t *testing.T) {
			beforeCounts := map[string]int{
				"StartRecord":                len(collectLogRecords[*logrecord.StartRecord](lm, firstLSN, nil)),
				"CommitRecord":               len(collectLogRecords[*logrecord.CommitRecord](lm, firstLSN, nil)),
				"AbortRecord":                len(collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, nil)),
				"EndRecord":                  len(collectLogRecords[*logrecord.EndRecord](lm, firstLSN, nil)),
				"SetBoolRecord":              len(collectLogRecords[*logrecord.SetBoolRecord](lm, firstLSN, nil)),
				"SetInt16Record":             len(collectLogRecords[*logrecord.SetInt16Record](lm, firstLSN, nil)),
				"SetInt32Record":             len(collectLogRecords[*logrecord.SetInt32Record](lm, firstLSN, nil)),
				"SetStrRecord":               len(collectLogRecords[*logrecord.SetStrRecord](lm, firstLSN, nil)),
				"SetDateRecord":              len(collectLogRecords[*logrecord.SetDateRecord](lm, firstLSN, nil)),
				"CompensationSetBoolRecord":  len(collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, nil)),
				"CompensationSetInt16Record": len(collectLogRecords[*logrecord.CompensationSetInt16Record](lm, firstLSN, nil)),
				"CompensationSetInt32Record": len(collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, nil)),
				"CompensationSetStrRecord":   len(collectLogRecords[*logrecord.CompensationSetStrRecord](lm, firstLSN, nil)),
				"CompensationSetDateRecord":  len(collectLogRecords[*logrecord.CompensationSetDateRecord](lm, firstLSN, nil)),
				"CheckpointBeginRecord":      len(collectLogRecords[*logrecord.CheckpointBeginRecord](lm, firstLSN, nil)),
				"CheckpointEndRecord":        len(collectLogRecords[*logrecord.CheckpointEndRecord](lm, firstLSN, nil)),
			}

			if err := rm.Recover(ctx); err != nil {
				t.Fatalf("Recover failed: %v", err)
			}

			afterCounts := map[string]int{
				"StartRecord":                len(collectLogRecords[*logrecord.StartRecord](lm, firstLSN, nil)),
				"CommitRecord":               len(collectLogRecords[*logrecord.CommitRecord](lm, firstLSN, nil)),
				"AbortRecord":                len(collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, nil)),
				"EndRecord":                  len(collectLogRecords[*logrecord.EndRecord](lm, firstLSN, nil)),
				"SetBoolRecord":              len(collectLogRecords[*logrecord.SetBoolRecord](lm, firstLSN, nil)),
				"SetInt16Record":             len(collectLogRecords[*logrecord.SetInt16Record](lm, firstLSN, nil)),
				"SetInt32Record":             len(collectLogRecords[*logrecord.SetInt32Record](lm, firstLSN, nil)),
				"SetStrRecord":               len(collectLogRecords[*logrecord.SetStrRecord](lm, firstLSN, nil)),
				"SetDateRecord":              len(collectLogRecords[*logrecord.SetDateRecord](lm, firstLSN, nil)),
				"CompensationSetBoolRecord":  len(collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, nil)),
				"CompensationSetInt16Record": len(collectLogRecords[*logrecord.CompensationSetInt16Record](lm, firstLSN, nil)),
				"CompensationSetInt32Record": len(collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, nil)),
				"CompensationSetStrRecord":   len(collectLogRecords[*logrecord.CompensationSetStrRecord](lm, firstLSN, nil)),
				"CompensationSetDateRecord":  len(collectLogRecords[*logrecord.CompensationSetDateRecord](lm, firstLSN, nil)),
				"CheckpointBeginRecord":      len(collectLogRecords[*logrecord.CheckpointBeginRecord](lm, firstLSN, nil)),
				"CheckpointEndRecord":        len(collectLogRecords[*logrecord.CheckpointEndRecord](lm, firstLSN, nil)),
			}

			t.Run("2回目 Recover 前後でログ件数が不変であること", func(t *testing.T) {
				for name, before := range beforeCounts {
					after := afterCounts[name]
					if before != after {
						t.Errorf("%s の件数が変化している: before=%d after=%d", name, before, after)
					}
				}
			})

			t.Run("各ページの状態が適切であること", func(t *testing.T) {
				t.Run("1個目のページの offsetBool の値が newBool であること", func(t *testing.T) {
					buff, err := base.bm.Pin(ctx, committedItems[0].Blk)
					if err != nil {
						t.Fatalf("pin failed: %v", err)
					}
					defer base.bm.Unpin(ctx, buff)

					got := buff.Contents().GetBool(offsetBool)
					if got != newBool {
						t.Errorf("1個目のページのoffset %d 番目の値 : expected %v, got %v", offsetBool, newBool, got)
					}
				})

				t.Run("1個目のページの offsetStr の値が newStr であること", func(t *testing.T) {
					buff, err := base.bm.Pin(ctx, committedItems[0].Blk)
					if err != nil {
						t.Fatalf("pin failed: %v", err)
					}
					defer base.bm.Unpin(ctx, buff)

					got := buff.Contents().GetStr(offsetStr)
					if got != newStr {
						t.Errorf("1個目のページのoffset %d 番目の値 : expected %s, got %s", offsetStr, newStr, got)
					}
				})

				t.Run("2個目のページの offsetInt32 の値が newInt32 であること", func(t *testing.T) {
					buff, err := base.bm.Pin(ctx, committedItems[1].Blk)
					if err != nil {
						t.Fatalf("pin failed: %v", err)
					}
					defer base.bm.Unpin(ctx, buff)

					got := buff.Contents().GetInt32(offsetInt32)
					if got != newInt32 {
						t.Errorf("2個目のページのoffset %d 番目の値 : expected %v, got %v", offsetInt32, newInt32, got)
					}
				})

				t.Run("2個目のページの offsetDate の値が newDate であること", func(t *testing.T) {
					buff, err := base.bm.Pin(ctx, committedItems[1].Blk)
					if err != nil {
						t.Fatalf("pin failed: %v", err)
					}
					defer base.bm.Unpin(ctx, buff)

					got := buff.Contents().GetDate(offsetDate)
					if got != newDate {
						t.Errorf("2個目のページのoffset %d 番目の値 : expected %v, got %v", offsetDate, newDate, got)
					}
				})
			})
		})
	})

	t.Run("doAnalyzePhase", func(t *testing.T) {
		const (
			runningTxnum         = 3
			committedNotEndTxnum = 4
			committedTxnum       = 5
			abortedNotEndTxnum   = 6
			abortedTxnum         = 7
			offsetBool           = 0
			offsetInt32          = 8
			offsetStr            = 16
			offsetDate           = 48
			oldBool              = false
			newBool              = true
			oldInt32             = int32(10)
			newInt32             = int32(999)
			oldStr               = "origin"
			newStr               = "updated"
		)
		oldDate := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
		newDate := time.Date(2024, time.February, 2, 0, 0, 0, 0, time.UTC)
		base := initRecoveryBase(t)
		mixedInitVals := []func(*buffer.Buffer) error{
			func(b *buffer.Buffer) error {
				if err := b.Contents().SetBool(offsetBool, oldBool); err != nil {
					return err
				}
				if err := b.Contents().SetStr(offsetStr, oldStr); err != nil {
					return err
				}
				return nil
			},
			func(b *buffer.Buffer) error {
				if err := b.Contents().SetInt32(offsetInt32, oldInt32); err != nil {
					return err
				}
				if err := b.Contents().SetDate(offsetDate, oldDate); err != nil {
					return err
				}
				return nil
			},
		}
		buildMixedSteps := func(rm *RecoveryMgr) []multiBlkStep {
			return []multiBlkStep{
				{
					page: 0,
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetBool(b, offsetBool, newBool)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offsetBool, newBool)
					},
				},
				{
					page: 1,
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetInt32(b, offsetInt32, newInt32)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetInt32(offsetInt32, newInt32)
					},
				},
				{
					page: 0,
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetStr(b, offsetStr, newStr)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetStr(offsetStr, newStr)
					},
				},
				{
					page: 1,
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetDate(b, offsetDate, newDate)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetDate(offsetDate, newDate)
					},
				},
			}
		}

		// 未コミットのトランザクション
		var firstDirtyResult *setMultiBlkResult
		{
			ctx := context.Background()
			fm, _, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, runningTxnum)

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)
			firstDirtyResult = result

			for _, item := range result.Items {
				bm.Unpin(ctx, item.Buff)
			}
		}

		// コミット済みのトランザクション(end未出力)
		{
			ctx := context.Background()
			fm, lm, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, committedNotEndTxnum)

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)

			commitLSN, err := logrecord.WriteCommitToLog(lm, result.LastLSN, committedNotEndTxnum)
			if err != nil {
				t.Fatalf("WriteCommitToLog failed: %v", err)
			}
			rm.lm.Flush(commitLSN)

			for _, item := range result.Items {
				bm.Unpin(ctx, item.Buff)
			}
		}

		// コミット済みのトランザクション(end Flush済み)
		{
			ctx := context.Background()
			fm, lm, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, committedTxnum)

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)

			endLSN, err := rm.Commit(ctx)
			if err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			for _, item := range result.Items {
				bm.Unpin(ctx, item.Buff)
			}

			lm.Flush(endLSN)
		}

		// aborted済みのトランザクション(end未出力)
		{
			ctx := context.Background()
			fm, lm, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, abortedNotEndTxnum)

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)

			abortLSN, err := logrecord.WriteAbortToLog(lm, result.LastLSN, abortedNotEndTxnum)
			if err != nil {
				t.Fatalf("WriteAbortToLog failed: %v", err)
			}
			lm.Flush(abortLSN)

			for _, item := range result.Items {
				bm.Unpin(ctx, item.Buff)
			}
		}

		// aborted済みのトランザクション(end Flush済み)
		{
			ctx := context.Background()
			fm, lm, bm, _, rm, _, _ := initRecoveryEnvOnBase(t, base, abortedTxnum)

			result := runRollbackMultiBlkCase(t, ctx, fm, bm, rm, 2,
				mixedInitVals,
				buildMixedSteps(rm)...,
			)

			for _, buff := range result.Buffs {
				bm.Unpin(ctx, buff)
			}

			lm.Flush(result.EndLSN)
		}

		_, _, _, _, rm, _, _ := initRecoveryEnvForRecoverOnBase(t, base)

		atTbl := newActiveTrxTable()
		dptTbl := buffer.NewDirtyPageTable()
		if err := rm.doAnalyzePhase(atTbl, dptTbl); err != nil {
			t.Fatalf("doAnalyzePhase failed: %v", err)
		}

		t.Run("未コミットのトランザクションが activeTable に running で含まれている", func(t *testing.T) {
			ety, ok := atTbl.getTrx(runningTxnum)
			if !ok {
				t.Fatalf("%d txnum not found at atTbl", runningTxnum)
			}

			if ety.getStatus() != running {
				t.Errorf("tx=%d の status が running でない: expected %d, got %d", runningTxnum, running, ety.getStatus())
			}
		})

		t.Run("コミット済みのトランザクションが activeTable に含まれていない", func(t *testing.T) {
			if _, ok := atTbl.getTrx(committedTxnum); ok {
				t.Errorf("tx=%d が activeTable に含まれている", committedTxnum)
			}
		})

		t.Run("aborted済み(end未出力)のトランザクションが activeTable に aborting で含まれている", func(t *testing.T) {
			ety, ok := atTbl.getTrx(abortedNotEndTxnum)
			if !ok {
				t.Fatalf("%d txnum not found at atTbl", abortedNotEndTxnum)
			}

			if ety.getStatus() != aborting {
				t.Errorf("tx=%d の status が aborting でない: expected %d, got %d", abortedNotEndTxnum, aborting, ety.getStatus())
			}
		})

		t.Run("aborted済み(End Flush済み)のトランザクションが activeTable に含まれていない", func(t *testing.T) {
			if _, ok := atTbl.getTrx(abortedTxnum); ok {
				t.Errorf("tx=%d が activeTable に含まれている", abortedTxnum)
			}
		})

		t.Run("変更がaborted済みページの 1個目のblk が DPT に 最初に変更した時のLSN で含まれている", func(t *testing.T) {
			ety, ok := dptTbl.GetPage(firstDirtyResult.Items[0].Blk)
			if !ok {
				t.Fatalf("blk=%s が DPT に含まれていない", firstDirtyResult.Items[0].Blk.ToString())
			}

			if ety.GetRecLSN() != firstDirtyResult.Items[0].RecLSN {
				t.Errorf("GetRecLSN=%d であるべきだが %d だった", firstDirtyResult.Items[0].RecLSN, ety.GetRecLSN())
			}
		})

		t.Run("変更がaborted済みページの 2個目のblk が DPT に 最初に変更した時のLSN で含まれている", func(t *testing.T) {
			ety, ok := dptTbl.GetPage(firstDirtyResult.Items[1].Blk)
			if !ok {
				t.Fatalf("blk=%s が DPT に含まれていない", firstDirtyResult.Items[1].Blk.ToString())
			}

			if ety.GetRecLSN() != firstDirtyResult.Items[1].RecLSN {
				t.Errorf("GetRecLSN=%d であるべきだが %d だった", firstDirtyResult.Items[1].RecLSN, ety.GetRecLSN())
			}
		})
	})

	t.Run("doRedoPhase", func(t *testing.T) {
		t.Run("DPT にないページはRedoされない", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				oldVal = false
				newVal = true
			)
			ctx := context.Background()
			base := initRecoveryBase(t)

			fm, lm, bm, _, rmWriter, _, _ := initRecoveryEnvOnBase(t, base, txnum)
			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rmWriter.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return nil
					},
				},
			)
			result.Buff.SetModified(txnum, result.LSN)
			defer bm.Unpin(ctx, result.Buff)
			lm.Flush(result.LSN)

			_, _, _, recovTx, rmRecov, _, _ := initRecoveryEnvForRecoverOnBase(t, base)
			atTbl := newActiveTrxTable()
			dptTbl := buffer.NewDirtyPageTable()
			if err := rmRecov.doRedoPhase(ctx, recovTx, atTbl, dptTbl); err != nil {
				t.Fatalf("doRedoPhase failed: %v", err)
			}

			t.Run("ページが変更されていない", func(t *testing.T) {
				if result.Buff.Contents().GetBool(offset) != oldVal {
					t.Errorf("ページが変更されていない: expected %t, got %t", oldVal, result.Buff.Contents().GetBool(offset))
				}
			})

			t.Run("txnum の EndRecord がログに出力されていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のEndRecordが出力されるべきではない", txnum)
				}
			})
		})

		t.Run("ディスク上のページのpageLSN が ログのLSN 以上の時に当該ページがRedoされない", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				oldVal = false
				newVal = true
			)
			ctx := context.Background()
			base := initRecoveryBase(t)

			fm, lm, bm, _, rmWriter, _, _ := initRecoveryEnvOnBase(t, base, txnum)
			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rmWriter.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return nil
					},
				},
			)
			result.Buff.SetModified(txnum, result.LSN)
			defer bm.Unpin(ctx, result.Buff)
			lm.Flush(result.LSN)
			bm.FlushAll(ctx, txnum)

			_, _, _, recovTx, rmRecov, _, _ := initRecoveryEnvForRecoverOnBase(t, base)
			atTbl := newActiveTrxTable()
			dptTbl := buffer.NewDirtyPageTable()
			dptTbl.MarkDirty(result.Buff.Block(), result.LSN)
			if err := rmRecov.doRedoPhase(ctx, recovTx, atTbl, dptTbl); err != nil {
				t.Fatalf("doRedoPhase failed: %v", err)
			}

			t.Run("ページが変更されていない", func(t *testing.T) {
				if result.Buff.Contents().GetBool(offset) != oldVal {
					t.Errorf("ページが変更されていない: expected %t, got %t", oldVal, result.Buff.Contents().GetBool(offset))
				}
			})

			t.Run("txnum の EndRecord がログに出力されていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のEndRecordが出力されるべきではない", txnum)
				}
			})
		})

		t.Run("DPT にあるページのRecLSN が ログのLSN 以上の時に当該ページがRedoされない", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				oldVal = false
				newVal = true
			)
			ctx := context.Background()
			base := initRecoveryBase(t)

			fm, lm, bm, _, rmWriter, _, _ := initRecoveryEnvOnBase(t, base, txnum)
			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rmWriter.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return nil
					},
				},
			)
			result.Buff.SetModified(txnum, result.LSN)
			defer bm.Unpin(ctx, result.Buff)
			lm.Flush(result.LSN)
			// bufferに書かれたpageLSNをディスクにflushするため
			bm.FlushAll(ctx, txnum)

			_, _, _, recovTx, rmRecov, _, _ := initRecoveryEnvForRecoverOnBase(t, base)
			atTbl := newActiveTrxTable()
			dptTbl := buffer.NewDirtyPageTable()
			// dpt に 未来LSN を設定してRedoされないことを確認
			dptTbl.MarkDirty(result.Buff.Block(), result.LSN+blocksize)
			if err := rmRecov.doRedoPhase(ctx, recovTx, atTbl, dptTbl); err != nil {
				t.Fatalf("doRedoPhase failed: %v", err)
			}

			t.Run("ページが変更されていない", func(t *testing.T) {
				if result.Buff.Contents().GetBool(offset) != oldVal {
					t.Errorf("ページが変更されていない: expected %t, got %t", oldVal, result.Buff.Contents().GetBool(offset))
				}
			})

			t.Run("txnum の EndRecord がログに出力されていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のEndRecordが出力されるべきではない", txnum)
				}
			})
		})

		t.Run("DPT対象かつ recLSN/ pageLSN 条件を満たすログはRedoされる", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				oldVal = false
				newVal = true
			)
			ctx := context.Background()
			base := initRecoveryBase(t)

			fm, lm, bm, _, rmWriter, _, _ := initRecoveryEnvOnBase(t, base, txnum)
			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rmWriter.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return nil
					},
				},
			)
			result.Buff.SetModified(txnum, result.LSN)
			defer bm.Unpin(ctx, result.Buff)
			lm.Flush(result.LSN)

			_, _, _, recovTx, rmRecov, _, _ := initRecoveryEnvForRecoverOnBase(t, base)
			atTbl := newActiveTrxTable()
			dptTbl := buffer.NewDirtyPageTable()
			// dpt に 未来LSN を設定してRedoされないことを確認
			dptTbl.MarkDirty(result.Buff.Block(), result.LSN)
			if err := rmRecov.doRedoPhase(ctx, recovTx, atTbl, dptTbl); err != nil {
				t.Fatalf("doRedoPhase failed: %v", err)
			}

			t.Run("ページが変更されている", func(t *testing.T) {
				if result.Buff.Contents().GetBool(offset) != newVal {
					t.Errorf("ページが変更されていない: expected %t, got %t", newVal, result.Buff.Contents().GetBool(offset))
				}
			})

			t.Run("txnum の EndRecord がログに出力されていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のEndRecordが出力されるべきではない", txnum)
				}
			})
		})

		t.Run("Commit Record がログに書き込まれているときに End Record が補完されてログに書き込まれている", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				oldVal = false
				newVal = true
			)
			ctx := context.Background()
			base := initRecoveryBase(t)

			fm, lm, bm, _, rmWriter, _, _ := initRecoveryEnvOnBase(t, base, txnum)
			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rmWriter.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offset, newVal)
					},
				},
			)
			result.Buff.SetModified(txnum, result.LSN)
			defer bm.Unpin(ctx, result.Buff)
			commitLSN, err := logrecord.WriteCommitToLog(lm, result.LSN, txnum)
			lm.Flush(commitLSN)
			if err != nil {
				t.Fatalf("WriteCommitToLog failed: %v", err)
			}

			_, _, _, recovTx, rmRecov, _, _ := initRecoveryEnvForRecoverOnBase(t, base)

			atTbl := newActiveTrxTable()
			// redo時にEnd Record を出力するため ATT に commit しているトランザクションを追加
			atTbl.setTrx(txnum, commited, commitLSN)
			dptTbl := buffer.NewDirtyPageTable()
			if err := rmRecov.doRedoPhase(ctx, recovTx, atTbl, dptTbl); err != nil {
				t.Fatalf("doRedoPhase failed: %v", err)
			}

			t.Run("txnum の EndRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.EndRecord](lm, commitLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のEndRecordが見つからない", txnum)
				}
			})
		})
	})

	t.Run("doUndoPhase", func(t *testing.T) {
		t.Run("ATT にないページはUndoされない", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				newVal = true
			)

			ctx := context.Background()
			base := initRecoveryBase(t)

			fm, lm, bm, _, rmWriter, _, _ := initRecoveryEnvOnBase(t, base, txnum)
			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rmWriter.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offset, newVal)
					},
				},
			)
			defer bm.Unpin(ctx, result.Buff)
			lm.Flush(result.LSN)

			_, _, _, recovTx, rmRecov, _, _ := initRecoveryEnvForRecoverOnBase(t, base)
			atTbl := newActiveTrxTable()
			if err := rmRecov.doUndoPhase(ctx, recovTx, atTbl); err != nil {
				t.Fatalf("doUndoPhase failed: %v", err)
			}

			tx := int32(txnum)
			aborts := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &tx)
			clrs := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &tx)
			ends := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &tx)

			t.Run("ページがUndoされていない", func(t *testing.T) {
				if result.Buff.Contents().GetBool(offset) != newVal {
					t.Errorf("ページがUndoされていない: expected %t, got %t", newVal, result.Buff.Contents().GetBool(offset))
				}
			})

			t.Run("txnum の AbortRecord がログに出力されていない", func(t *testing.T) {
				if len(aborts) != 0 {
					t.Errorf("tx=%d の AbortRecord が出力されるべきではない", txnum)
				}
			})

			t.Run("txnum の CompensationSetBoolRecord がログに出力されていない", func(t *testing.T) {
				if len(clrs) != 0 {
					t.Errorf("tx=%d の CompensationSetBoolRecord が出力されるべきではない", txnum)
				}
			})

			t.Run("txnum の EndRecord がログに出力されていない", func(t *testing.T) {
				if len(ends) != 0 {
					t.Errorf("tx=%d の EndRecord が出力されるべきではない", txnum)
				}
			})
		})

		t.Run("ATT に running のtrx がある場合は Abort→CLR→END がログに出力される", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				oldVal = false
				newVal = true
			)

			ctx := context.Background()
			base := initRecoveryBase(t)

			fm, lm, bm, _, rmWriter, _, _ := initRecoveryEnvOnBase(t, base, txnum)
			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rmWriter.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offset, newVal)
					},
				},
			)
			defer bm.Unpin(ctx, result.Buff)
			lm.Flush(result.LSN)

			_, _, _, recovTx, rmRecov, _, _ := initRecoveryEnvForRecoverOnBase(t, base)
			atTbl := newActiveTrxTable()
			// undo時にAbort → CLR → End Record を出力するため ATT に running のトランザクションを追加
			atTbl.setTrx(txnum, running, result.LSN)
			if err := rmRecov.doUndoPhase(ctx, recovTx, atTbl); err != nil {
				t.Fatalf("doUndoPhase failed: %v", err)
			}

			tx := int32(txnum)
			aborts := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &tx)
			clrs := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &tx)
			ends := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &tx)

			t.Run("txnum の AbortRecord がログに1回出力されている", func(t *testing.T) {
				if len(aborts) != 1 {
					t.Errorf("tx=%d の AbortRecord がログに正しい回数書き込まれていない", txnum)
				}
			})

			t.Run("txnum の AbortRecord の PrevLSN が SetBoolRecord の LSN である", func(t *testing.T) {
				if aborts[0].Rec.PrevLSN() != result.LSN {
					t.Errorf("AbortRecord の PrevLSN が %d であるべきだが %d だった", result.LSN, aborts[0].Rec.PrevLSN())
				}
			})

			t.Run("txnum の CompensationSetBoolRecord がログに1回出力されている", func(t *testing.T) {
				if len(clrs) != 1 {
					t.Errorf("tx=%d の CompensationSetBoolRecord がログに正しい回数書き込まれていない", txnum)
				}
			})

			t.Run("txnum の CompensationSetBoolRecord の PrevLSN が AbortRecord の LSN である", func(t *testing.T) {
				if clrs[0].Rec.PrevLSN() != aborts[0].LSN {
					t.Errorf("CompensationSetBoolRecord の PrevLSN が %d であるべきだが %d だった", aborts[0].LSN, clrs[0].Rec.PrevLSN())
				}
			})

			t.Run("txnum の EndRecord がログに1回出力されている", func(t *testing.T) {
				if len(ends) != 1 {
					t.Errorf("tx=%d の EndRecord がログに正しい回数書き込まれていない", txnum)
				}
			})

			t.Run("txnum の EndRecord の PrevLSN が CompensationSetBoolRecord の LSN である", func(t *testing.T) {
				if ends[0].Rec.PrevLSN() != clrs[0].LSN {
					t.Errorf("CompensationSetBoolRecord の PrevLSN が %d であるべきだが %d だった", clrs[0].LSN, ends[0].Rec.PrevLSN())
				}
			})
		})

		t.Run("Abort→CLR の途中までログに出力されている場合は その途中から再開してUndoが完了する", func(t *testing.T) {
			const (
				txnum       = 3
				offsetBool  = 0
				offsetInt32 = 8
				oldBool     = false
				newBool     = true
				oldInt32    = int32(10)
				newInt32    = int32(999)
			)

			ctx := context.Background()
			base := initRecoveryBase(t)

			fm, lm, bm, rmTx, rmWriter, _, _ := initRecoveryEnvOnBase(t, base, txnum)

			mixedInitVals := []func(*buffer.Buffer) error{
				func(b *buffer.Buffer) error {
					if err := b.Contents().SetBool(offsetBool, oldBool); err != nil {
						return err
					}
					return nil
				},
				func(b *buffer.Buffer) error {
					if err := b.Contents().SetInt32(offsetInt32, oldInt32); err != nil {
						return err
					}
					return nil
				},
			}
			buildMixedSteps := func(rm *RecoveryMgr) []multiBlkStep {
				return []multiBlkStep{
					{
						page: 0,
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetBool(b, offsetBool, newBool)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetBool(offsetBool, newBool)
						},
					},
					{
						page: 1,
						log: func(b *buffer.Buffer) (int32, error) {
							return rm.SetInt32(b, offsetInt32, newInt32)
						},
						apply: func(b *buffer.Buffer) error {
							return b.Contents().SetInt32(offsetInt32, newInt32)
						},
					},
				}
			}

			result := runSetMultiBlkCase(t, ctx, fm, bm, 2, mixedInitVals, buildMixedSteps(rmWriter)...)
			undoLSN := result.LastLSN

			abortLSN, err := logrecord.WriteAbortToLog(rmWriter.lm, undoLSN, rmWriter.txnum)
			if err != nil {
				t.Fatalf("WriteAbortToLog failed: %v", err)
			}
			lm.Flush(abortLSN)

			bytes, err := lm.ReadRecordAt(undoLSN)
			if err != nil {
				t.Fatalf("ReadRecordAt failed: %v", err)
			}

			logRec := logrecord.CreateLogRecord(bytes, undoLSN)

			int32Rec, ok := logRec.(*logrecord.SetInt32Record)
			if !ok {
				t.Fatalf("logrecord.SetInt32Record cast failed")
			}

			clrLSN, err := int32Rec.WriteCLR(ctx, rmTx, lm, abortLSN)
			if err != nil {
				t.Fatalf("WriteCLR failed %v", err)
			}
			lm.Flush(clrLSN)
			int32Rec.UndoPage(ctx, rmTx, clrLSN)

			atTbl := newActiveTrxTable()
			// undoを最後まで行うためにactive txのLastLSN追加
			atTbl.setTrx(txnum, aborting, clrLSN)
			if err := rmWriter.doUndoPhase(ctx, rmTx, atTbl); err != nil {
				t.Fatalf("doUndoPhase failed: %v", err)
			}

			t.Run("ページの 1個目のblk の値が oldBool である", func(t *testing.T) {
				if result.Items[0].Buff.Contents().GetBool(offsetBool) != oldBool {
					t.Errorf("%v であるべきだが %v だった", oldBool, result.Items[0].Buff.Contents().GetBool(offsetBool))
				}
			})

			t.Run("ページの 2個目のblk の値が oldInt32 である", func(t *testing.T) {
				if result.Items[1].Buff.Contents().GetInt32(offsetInt32) != oldInt32 {
					t.Errorf("%v であるべきだが %v だった", oldInt32, result.Items[1].Buff.Contents().GetInt32(offsetInt32))
				}
			})

			t.Run("残りの CLR→END が出力されている", func(t *testing.T) {
				txnum := int32(txnum)
				startRecs := collectLogRecords[*logrecord.StartRecord](lm, firstLSN, &txnum)
				clrBoolRecs := collectLogRecords[*logrecord.CompensationSetBoolRecord](lm, firstLSN, &txnum)
				clrInt32Recs := collectLogRecords[*logrecord.CompensationSetInt32Record](lm, firstLSN, &txnum)
				abortRecs := collectLogRecords[*logrecord.AbortRecord](lm, firstLSN, &txnum)
				endRecs := collectLogRecords[*logrecord.EndRecord](lm, firstLSN, &txnum)

				if len(startRecs) != 1 {
					t.Fatalf("StartRecordの書き込み回数が1回であるべきだがそうではない")
				}

				if len(endRecs) != 1 {
					t.Fatalf("EndRecordの書き込み回数が1回であるべきだがそうではない")
				}

				if len(abortRecs) != 1 {
					t.Fatalf("AbortRecordの書き込み回数が1回であるべきだがそうではない")
				}

				t.Run("CompensationSetInt32Record が1回のみ出力されている", func(t *testing.T) {
					if len(clrInt32Recs) != 1 {
						t.Errorf("tx=%d のCompensationSetInt32Recordが正しい回数見つからない: expected %d, got %d", txnum, 1, len(clrInt32Recs))
					}
				})

				t.Run("CompensationSetInt32RecordのPrevLSN が AbortRecordのLSN である", func(t *testing.T) {
					if clrInt32Recs[0].Rec.PrevLSN() != abortRecs[0].LSN {
						t.Errorf("CompensationSetInt32Record の PrevLSN が %d であるべきだが %d だった", abortRecs[0].LSN, clrInt32Recs[0].Rec.PrevLSN())
					}
				})

				t.Run("CompensationSetInt32RecordのUndoNextLSN が SetBoolRecordのLSN である", func(t *testing.T) {
					if clrInt32Recs[0].Rec.UndoNextLSN() != result.Items[0].LastLSN {
						t.Errorf("CompensationSetInt32Record の UndoNextLSN が %d であるべきだが %d だった", result.Items[0].LastLSN, clrInt32Recs[0].Rec.UndoNextLSN())
					}
				})

				t.Run("CompensationSetBoolRecord が1回のみ出力されている", func(t *testing.T) {
					if len(clrBoolRecs) != 1 {
						t.Errorf("tx=%d のCompensationSetBoolRecordが正しい回数見つからない: expected %d, got %d", txnum, 1, len(clrBoolRecs))
					}
				})

				t.Run("CompensationSetBoolRecordのPrevLSN が CompensationSetInt32RecordのLSN である", func(t *testing.T) {
					if clrBoolRecs[0].Rec.PrevLSN() != clrInt32Recs[0].LSN {
						t.Errorf("CompensationSetBoolRecord の PrevLSN が %d であるべきだが %d だった", clrInt32Recs[0].LSN, clrBoolRecs[0].Rec.PrevLSN())
					}
				})

				t.Run("CompensationSetBoolRecordのUndoNextLSN が StartRecordのLSN である", func(t *testing.T) {
					if clrBoolRecs[0].Rec.UndoNextLSN() != startRecs[0].LSN {
						t.Errorf("CompensationSetBoolRecord の UndoNextLSN が %d であるべきだが %d だった", startRecs[0].LSN, clrBoolRecs[0].Rec.UndoNextLSN())
					}
				})

				t.Run("EndRecordのPrevLSN が CompensationSetBoolRecordのLSN である", func(t *testing.T) {
					if endRecs[0].Rec.PrevLSN() != clrBoolRecs[0].LSN {
						t.Errorf("EndRecord の PrevLSN が %d であるべきだが %d だった", clrBoolRecs[0].LSN, endRecs[0].Rec.PrevLSN())
					}
				})
			})
		})
	})

	t.Run("SetXXX", func(t *testing.T) {
		t.Run("SetBool Record", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				newVal = false
			)
			ctx := context.Background()
			fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetBool(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetBool(offset, newVal)
					},
				},
			)
			defer bm.Unpin(ctx, result.Buff)

			t.Run("ATT の txnum のLSNが SetBoolのLSN である", func(t *testing.T) {
				lastLSN, err := atTbl.getLastLSN(txnum)
				if err != nil {
					t.Fatalf("getLastLSN failed: %v", err)
				}

				if lastLSN != result.LSN {
					t.Errorf("tx=%d のLSNが %d であるべきだが %d だった", txnum, result.LSN, lastLSN)
				}
			})

			t.Run("txnum の SetBoolRecord がログに書き込まれていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetBoolRecord](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のSetBoolRecordがログに書き込まれているべきではない", txnum)
				}
			})

			if _, err := rm.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			t.Run("txnum の SetBoolRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetBoolRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のSetBoolRecordが見つからない", txnum)
				}
			})
		})

		t.Run("SetDate Record", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
			)
			var (
				newVal = time.Unix(0, 0).UTC()
			)

			ctx := context.Background()
			fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetDate(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetDate(offset, newVal)
					},
				},
			)
			defer bm.Unpin(ctx, result.Buff)

			t.Run("ATT の txnum のLSNが SetBoolのLSN である", func(t *testing.T) {
				lastLSN, err := atTbl.getLastLSN(txnum)
				if err != nil {
					t.Fatalf("getLastLSN failed: %v", err)
				}

				if lastLSN != result.LSN {
					t.Errorf("tx=%d のLSNが %d であるべきだが %d だった", txnum, result.LSN, lastLSN)
				}
			})

			t.Run("txnum の SetDateRecord がログに書き込まれていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetDateRecord](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のSetDateRecordがログに書き込まれているべきではない", txnum)
				}
			})

			if _, err := rm.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			t.Run("txnum の SetDateRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetDateRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のSetDateRecordが見つからない", txnum)
				}
			})
		})

		t.Run("SetInt16 Record", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
			)
			var (
				newVal = int16(999)
			)

			ctx := context.Background()
			fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetInt16(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetInt16(offset, newVal)
					},
				},
			)
			defer bm.Unpin(ctx, result.Buff)

			t.Run("ATT の txnum のLSNが SetBoolのLSN である", func(t *testing.T) {
				lastLSN, err := atTbl.getLastLSN(txnum)
				if err != nil {
					t.Fatalf("getLastLSN failed: %v", err)
				}

				if lastLSN != result.LSN {
					t.Errorf("tx=%d のLSNが %d であるべきだが %d だった", txnum, result.LSN, lastLSN)
				}
			})

			t.Run("txnum の SetInt16Record がログに書き込まれていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetInt16Record](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のSetInt16Recordがログに書き込まれているべきではない", txnum)
				}
			})

			if _, err := rm.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			t.Run("txnum の SetInt16Record が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetInt16Record](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のSetInt16Recordが見つからない", txnum)
				}
			})
		})

		t.Run("SetInt32 Record", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
			)
			var (
				newVal = int32(999)
			)

			ctx := context.Background()
			fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetInt32(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetInt32(offset, newVal)
					},
				},
			)
			defer bm.Unpin(ctx, result.Buff)

			t.Run("ATT の txnum のLSNが SetBoolのLSN である", func(t *testing.T) {
				lastLSN, err := atTbl.getLastLSN(txnum)
				if err != nil {
					t.Fatalf("getLastLSN failed: %v", err)
				}

				if lastLSN != result.LSN {
					t.Errorf("tx=%d のLSNが %d であるべきだが %d だった", txnum, result.LSN, lastLSN)
				}
			})

			t.Run("txnum の SetInt32Record がログに書き込まれていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetInt32Record](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のSetInt32Recordがログに書き込まれているべきではない", txnum)
				}
			})

			if _, err := rm.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			t.Run("txnum の SetInt32Record が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetInt32Record](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のSetInt32Recordが見つからない", txnum)
				}
			})
		})

		t.Run("SetStr Record", func(t *testing.T) {
			const (
				txnum  = 3
				offset = 0
				newVal = "after"
			)

			ctx := context.Background()
			fm, lm, bm, _, rm, atTbl, _ := initRecoveryEnv(t, txnum)

			result := runSetCase(t, ctx, fm, bm,
				setStep{
					log: func(b *buffer.Buffer) (int32, error) {
						return rm.SetStr(b, offset, newVal)
					},
					apply: func(b *buffer.Buffer) error {
						return b.Contents().SetStr(offset, newVal)
					},
				},
			)
			defer bm.Unpin(ctx, result.Buff)

			t.Run("ATT の txnum のLSNが SetBoolのLSN である", func(t *testing.T) {
				lastLSN, err := atTbl.getLastLSN(txnum)
				if err != nil {
					t.Fatalf("getLastLSN failed: %v", err)
				}

				if lastLSN != result.LSN {
					t.Errorf("tx=%d のLSNが %d であるべきだが %d だった", txnum, result.LSN, lastLSN)
				}
			})

			t.Run("txnum の SetStrRecord がログに書き込まれていない", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetStrRecord](lm, firstLSN, &txnum)
				if len(records) != 0 {
					t.Errorf("tx=%d のSetStrRecordがログに書き込まれているべきではない", txnum)
				}
			})

			if _, err := rm.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			t.Run("txnum の SetStrRecord が1回ログに書き込まれている", func(t *testing.T) {
				txnum := int32(txnum)
				records := collectLogRecords[*logrecord.SetStrRecord](lm, firstLSN, &txnum)
				if len(records) != 1 {
					t.Errorf("tx=%d のSetStrRecordが見つからない", txnum)
				}
			})
		})
	})
}

func TestActiveTrxTable(t *testing.T) {
	t.Run("ReplaceSnapshot: snapshotの内容で全置換される", func(t *testing.T) {
		atTbl := newActiveTrxTable()
		atTbl.setTrx(1, running, 10)

		snapshot := map[int32]logrecord.CheckpointTxnEntry{
			2: {Status: commited, LastLSN: 20},
		}
		atTbl.ReplaceSnapshot(snapshot)

		if _, ok := atTbl.getTrx(1); ok {
			t.Fatalf("old tx should be removed by ReplaceSnapshot")
		}
		entry, ok := atTbl.getTrx(2)
		if !ok {
			t.Fatalf("new tx should exist after ReplaceSnapshot")
		}
		if got := entry.getStatus(); got != commited {
			t.Fatalf("expected status=%d, got %d", commited, got)
		}
		if got := entry.getLastLSN(); got != 20 {
			t.Fatalf("expected lastLSN=20, got %d", got)
		}
	})

	t.Run("ReplaceSnapshot: 引数snapshotを後で変更しても内部状態が変わらない", func(t *testing.T) {
		atTbl := newActiveTrxTable()
		snapshot := map[int32]logrecord.CheckpointTxnEntry{
			3: {Status: running, LastLSN: 30},
		}
		atTbl.ReplaceSnapshot(snapshot)

		delete(snapshot, 3)
		if _, ok := atTbl.getTrx(3); !ok {
			t.Fatalf("internal table should not be affected by snapshot mutation")
		}
	})
}
