// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockop

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	retryError               = moerr.NewTxnNeedRetryNoCtx()
	retryWithDefChangedError = moerr.NewTxnNeedRetryWithDefChangedNoCtx()
)

const opName = "lock_op"

func (lockOp *LockOp) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": lock-op(")
	n := len(lockOp.targets) - 1
	for idx, target := range lockOp.targets {
		buf.WriteString(fmt.Sprintf("%d-%d-%d",
			target.tableID,
			target.primaryColumnIndexInBatch,
			target.refreshTimestampIndexInBatch))
		if idx < n {
			buf.WriteString(",")
		}
	}
	buf.WriteString(")")
}

func (lockOp *LockOp) OpType() vm.OpType {
	return vm.LockOp
}

func (lockOp *LockOp) Prepare(proc *process.Process) error {
	if lockOp.OpAnalyzer == nil {
		lockOp.OpAnalyzer = process.NewAnalyzer(lockOp.GetIdx(), lockOp.IsFirst, lockOp.IsLast, "lock_op")
	} else {
		lockOp.OpAnalyzer.Reset()
	}

	if len(lockOp.ctr.fetchers) == 0 {
		lockOp.logger = getLogger(proc.GetService())
		lockOp.ctr.fetchers = make([]FetchLockRowsFunc, 0, len(lockOp.targets))
		for idx := range lockOp.targets {
			lockOp.ctr.fetchers = append(lockOp.ctr.fetchers,
				GetFetchRowsFunc(lockOp.targets[idx].primaryColumnType))
		}
	}
	if len(lockOp.ctr.relations) == 0 {
		lockOp.ctr.relations = make([]engine.Relation, len(lockOp.targets))
		for i, target := range lockOp.targets {
			if target.objRef != nil {
				rel, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, lockOp.engine, target.objRef)
				if err != nil {
					return err
				}
				lockOp.ctr.relations[i] = rel
			}
		}
	} else {
		for i, target := range lockOp.targets {
			if target.objRef != nil {
				err := lockOp.ctr.relations[i].Reset(proc.GetTxnOperator())
				if err != nil {
					return err
				}
			}
		}
	}
	lockOp.ctr.parker = types.NewPacker()
	return nil
}

// Call the lock op is used to add locks into lockservice of the Table operated by the
// current transaction under a pessimistic transaction.
//
// In RC's transaction mode, after successful locking, if an accessed data is found to be
// concurrently modified by other transactions, a Timestamp column will be put on the output
// vectors for querying the latest data, and subsequent op needs to check this column to check
// whether the latest data needs to be read.
func (lockOp *LockOp) Call(proc *process.Process) (vm.CallResult, error) {
	txnOp := proc.GetTxnOperator()
	if !txnOp.Txn().IsPessimistic() {
		return vm.Exec(lockOp.GetChildren(0), proc)
	}

	// for the case like `select for update`, need to lock whole batches before send it to next operator
	// currently, this is implemented by blocking a output operator below, instead of calling func callBlocking
	return callNonBlocking(proc, lockOp)
}

func callNonBlocking(
	proc *process.Process,
	lockOp *LockOp) (vm.CallResult, error) {
	analyzer := lockOp.OpAnalyzer

	result, err := vm.ChildrenCall(lockOp.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}

	if result.Batch == nil {
		if lockOp.ctr.retryError == nil {
			err := lockTalbeIfLockCountIsZero(proc, lockOp)
			if err != nil {
				return result, err
			}
		}
		return result, lockOp.ctr.retryError
	}
	if result.Batch.IsEmpty() {
		return result, err
	}

	lockOp.ctr.lockCount += int64(result.Batch.RowCount())
	if err = performLock(result.Batch, proc, lockOp, analyzer, -1); err != nil {
		return result, err
	}

	return result, nil
}

// if input vec is not allnull and has null, return a copy vector without null value
func getVec(proc *process.Process, vec *vector.Vector) (*vector.Vector, error) {
	if vec.HasNull() {
		nulls := vec.GetNulls()
		newVec := vector.NewVec(*vec.GetType())
		for i := 0; i < vec.Length(); i++ {
			if !nulls.Contains(uint64(i)) {
				if err := vector.AppendBytes(newVec, nil, true, proc.Mp()); err != nil {
					newVec.Free(proc.GetMPool())
					return nil, err
				}
				if err := newVec.Copy(vec, int64(newVec.Length()-1), int64(i), proc.GetMPool()); err != nil {
					newVec.Free(proc.GetMPool())
					return nil, err
				}
			}
		}
		return newVec, nil
	}
	return vec, nil
}

func performLock(
	bat *batch.Batch,
	proc *process.Process,
	lockOp *LockOp,
	analyzer process.Analyzer,
	targetIdx int,
) error {
	needRetry := false
	for idx, target := range lockOp.targets {
		if targetIdx != -1 && targetIdx != idx {
			continue
		}
		if proc.GetTxnOperator().LockSkipped(target.tableID, target.mode) {
			return nil
		}
		lockOp.logger.Debug("lock",
			zap.Uint64("table", target.tableID),
			zap.Bool("filter", target.filter != nil),
			zap.Int32("filter-col", target.filterColIndexInBatch),
			zap.Int32("primary-index", target.primaryColumnIndexInBatch))
		var filterCols []int32
		// For partitioned tables, filter is not nil
		// no function call AddLockTargetWithPartition to set target.filter, so next code is unused
		/* 		if target.filter != nil {
			srcVec := bat.GetVector(target.filterColIndexInBatch)
			vec, err := getVec(proc, srcVec)
			if err != nil {
				return err
			}
			filterCols = vector.MustFixedColWithTypeCheck[int32](vec)
			if srcVec != vec {
				vec.Free(proc.GetMPool())
			}
			for _, value := range filterCols {
				// has Illegal Partition index
				if value == -1 {
					return moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
				}
			}
		} */
		locked, defChanged, refreshTS, err := doLock(
			proc.Ctx,
			lockOp.engine,
			analyzer,
			lockOp.ctr.relations[idx],
			target.tableID,
			proc,
			bat,
			target.primaryColumnIndexInBatch,
			target.primaryColumnType,
			target.partitionColumnIndexInBatch,
			DefaultLockOptions(lockOp.ctr.parker).
				WithLockMode(lock.LockMode_Exclusive).
				WithFetchLockRowsFunc(lockOp.ctr.fetchers[idx]).
				WithMaxBytesPerLock(int(proc.GetLockService().GetConfig().MaxLockRowCount)).
				WithFilterRows(target.filter, filterCols).
				WithLockTable(target.lockTable, target.changeDef).
				WithHasNewVersionInRangeFunc(lockOp.ctr.hasNewVersionInRange),
		)
		if lockOp.logger.Enabled(zap.DebugLevel) {
			lockOp.logger.Debug("lock result",
				zap.Uint64("table", target.tableID),
				zap.Bool("locked", locked),
				zap.Int32("primary-index", target.primaryColumnIndexInBatch),
				zap.String("refresh-ts", refreshTS.DebugString()),
				zap.Error(err))
		}
		if err != nil {
			return err
		}
		if !locked {
			continue
		}

		// refreshTS is last commit ts + 1, because we need see the committed data.
		if proc.Base.TxnClient.RefreshExpressionEnabled() &&
			target.refreshTimestampIndexInBatch != -1 {
			priVec := bat.GetVector(target.primaryColumnIndexInBatch)
			vec := bat.GetVector(target.refreshTimestampIndexInBatch)
			ts := types.BuildTS(refreshTS.PhysicalTime, refreshTS.LogicalTime)
			n := priVec.Length()
			for i := 0; i < n; i++ {
				vector.AppendFixed(vec, ts, false, proc.Mp())
			}
			continue
		}

		// if need to retry, do not return the retry error immediately, first try to get all
		// the locks to avoid another conflict when retrying
		if !needRetry && !refreshTS.IsEmpty() {
			needRetry = true
		}
		if !lockOp.ctr.defChanged {
			lockOp.ctr.defChanged = defChanged
		}
	}
	// when a transaction needs to operate on many data, there may be multiple conflicts on the
	// data, and if you go to retry every time a conflict occurs, you will also encounter conflicts
	// when you retry. We need to return the conflict after all the locks have been added successfully,
	// so that the retry will definitely succeed because all the locks have been put.
	if needRetry && lockOp.ctr.retryError == nil {
		lockOp.ctr.retryError = retryError
	}
	if lockOp.ctr.defChanged {
		lockOp.ctr.retryError = retryWithDefChangedError
	}
	return nil
}

// LockTable lock table, all rows in the table will be locked, and wait current txn
// closed.
func LockTable(
	eng engine.Engine,
	proc *process.Process,
	tableID uint64,
	pkType types.Type,
	changeDef bool) error {
	txnOp := proc.GetTxnOperator()
	if !txnOp.Txn().IsPessimistic() {
		return nil
	}
	parker := types.NewPacker()
	defer parker.Close()

	stats := statistic.StatsInfoFromContext(proc.Ctx)
	analyzer := process.NewTempAnalyzer()
	defer func() {
		waitLockTime := analyzer.GetOpStats().GetMetricByKey(process.OpWaitLockTime)
		stats.AddPreRunOnceWaitLockDuration(waitLockTime)
		stats.AddScopePrepareS3Request(statistic.S3Request{
			List:      analyzer.GetOpStats().S3List,
			Head:      analyzer.GetOpStats().S3Head,
			Put:       analyzer.GetOpStats().S3Put,
			Get:       analyzer.GetOpStats().S3Get,
			Delete:    analyzer.GetOpStats().S3Delete,
			DeleteMul: analyzer.GetOpStats().S3DeleteMul,
		})
	}()

	opts := DefaultLockOptions(parker).
		WithLockTable(true, changeDef).
		WithFetchLockRowsFunc(GetFetchRowsFunc(pkType))
	_, defChanged, refreshTS, err := doLock(
		proc.Ctx,
		eng,
		analyzer,
		nil,
		tableID,
		proc,
		nil,
		0,
		pkType,
		-1,
		opts)
	if err != nil {
		return err
	}

	// If the returned timestamp is not empty, we should return a retry error,
	if !refreshTS.IsEmpty() {
		if !defChanged {
			return retryError
		}
		return retryWithDefChangedError
	}
	return nil
}

// LockRow lock rows in table, rows will be locked, and wait current txn closed.
func LockRows(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	tableID uint64,
	bat *batch.Batch,
	idx int32,
	pkType types.Type,
	lockMode lock.LockMode,
	sharding lock.Sharding,
	group uint32,
) error {
	txnOp := proc.GetTxnOperator()
	if !txnOp.Txn().IsPessimistic() {
		return nil
	}

	parker := types.NewPacker()
	defer parker.Close()

	stats := statistic.StatsInfoFromContext(proc.Ctx)
	analyzer := process.NewTempAnalyzer()
	defer func() {
		waitLockTime := analyzer.GetOpStats().GetMetricByKey(process.OpWaitLockTime)
		stats.AddPreRunOnceWaitLockDuration(waitLockTime)
		stats.AddScopePrepareS3Request(statistic.S3Request{
			List:      analyzer.GetOpStats().S3List,
			Head:      analyzer.GetOpStats().S3Head,
			Put:       analyzer.GetOpStats().S3Put,
			Get:       analyzer.GetOpStats().S3Get,
			Delete:    analyzer.GetOpStats().S3Delete,
			DeleteMul: analyzer.GetOpStats().S3DeleteMul,
		})
	}()

	opts := DefaultLockOptions(parker).
		WithLockTable(false, false).
		WithLockSharding(sharding).
		WithLockMode(lockMode).
		WithLockGroup(group).
		WithFetchLockRowsFunc(GetFetchRowsFunc(pkType))
	_, defChanged, refreshTS, err := doLock(
		proc.Ctx,
		eng,
		analyzer,
		rel,
		tableID,
		proc,
		bat,
		idx,
		pkType,
		-1,
		opts)
	if err != nil {
		return err
	}
	// If the returned timestamp is not empty, we should return a retry error,
	if !refreshTS.IsEmpty() {
		if !defChanged {
			return retryError
		}
		return retryWithDefChangedError
	}
	return nil
}

// doLock locks a set of data so that no other transaction can modify it.
// The data is described by the primary key. When the returned timestamp.IsEmpty
// is false, it means there is a conflict with other transactions and the data to
// be manipulated has been modified, you need to get the latest data at timestamp.
func doLock(
	ctx context.Context,
	eng engine.Engine,
	analyzer process.Analyzer,
	rel engine.Relation,
	tableID uint64,
	proc *process.Process,
	bat *batch.Batch,
	idx int32,
	pkType types.Type,
	partitionIdx int32,
	opts LockOptions,
) (bool, bool, timestamp.Timestamp, error) {
	txnOp := proc.GetTxnOperator()
	txnClient := proc.Base.TxnClient
	lockService := proc.GetLockService()

	if !txnOp.Txn().IsPessimistic() {
		return false, false, timestamp.Timestamp{}, nil
	}

	if runtime.InTesting(proc.GetService()) {
		tc := runtime.MustGetTestingContext(proc.GetService())
		tc.GetBeforeLockFunc()(txnOp.Txn().ID, tableID)
	}

	seq := txnOp.NextSequence()
	startAt := time.Now()
	trace.GetService(proc.GetService()).AddTxnDurationAction(
		txnOp,
		client.LockEvent,
		seq,
		tableID,
		0,
		nil)

	// in this case:
	// create table t1 (a int primary key, b int ,c int, unique key(b,c));
	// insert into t1 values (1,1,null);
	// update t1 set b = b+1 where a = 1;
	//    here MO will use 't1 left join hidden_tbl' to fetch the PK in hidden table to lock,
	//    but the result will be ConstNull vector
	var vec *vector.Vector
	if bat != nil {
		vec = bat.GetVector(idx)
	}

	var err error
	if vec != nil && vec.AllNull() {
		return false, false, timestamp.Timestamp{}, nil
	}
	if vec != nil {
		inputVec := vec
		if vec, err = getVec(proc, inputVec); err != nil {
			return false, false, timestamp.Timestamp{}, err
		}
		if vec != inputVec {
			defer vec.Free(proc.GetMPool())
		}
	}

	if opts.maxCountPerLock == 0 {
		opts.maxCountPerLock = int(lockService.GetConfig().MaxLockRowCount)
	}
	fetchFunc := opts.fetchFunc
	if fetchFunc == nil {
		fetchFunc = GetFetchRowsFunc(pkType)
	}

	has, rows, g := fetchFunc(
		vec,
		opts.parker,
		pkType,
		opts.maxCountPerLock,
		opts.lockTable,
		opts.filter,
		opts.filterCols)
	if !has {
		return false, false, timestamp.Timestamp{}, nil
	}

	txn := txnOp.Txn()
	options := lock.LockOptions{
		Granularity:     g,
		Policy:          proc.GetWaitPolicy(),
		Mode:            opts.mode,
		TableDefChanged: opts.changeDef,
		Sharding:        opts.sharding,
		Group:           opts.group,
		SnapShotTs:      txnOp.CreateTS(),
	}
	if txn.Mirror {
		options.ForwardTo = txn.LockService
		if options.ForwardTo == "" {
			panic("forward to empty lock service")
		}
	} else {
		// FIXME: in launch model, multi-cn will use same process level runtime. So lockservice will be wrong.
		if txn.LockService != lockService.GetServiceID() {
			lockService = lockservice.GetLockServiceByServiceID(txn.LockService)
		}
	}

	start := time.Now()
	key := txnOp.AddWaitLock(tableID, rows, options)
	defer txnOp.RemoveWaitLock(key)

	table := tableID
	if opts.sharding == lock.Sharding_ByRow {
		table = lockservice.ShardingByRow(rows[0])
	}

	result, err := lockWithRetry(
		ctx,
		lockService,
		table,
		rows,
		txn.ID,
		options,
		txnOp,
		fetchFunc,
		vec,
		opts,
		pkType,
	)
	if err != nil {
		return false, false, timestamp.Timestamp{}, err
	}
	// Record lock waiting time
	analyzeLockWaitTime(analyzer, start)

	if runtime.InTesting(proc.GetService()) {
		tc := runtime.MustGetTestingContext(proc.GetService())
		tc.GetAdjustLockResultFunc()(txn.ID, tableID, &result)
	}

	if len(result.ConflictKey) > 0 {
		trace.GetService(proc.GetService()).AddTxnActionInfo(
			txnOp,
			client.LockEvent,
			seq,
			tableID,
			func(writer trace.Writer) {
				writer.WriteHex(result.ConflictKey)
				writer.WriteString(":")
				writer.WriteHex(result.ConflictTxn)
				writer.WriteString("/")
				writer.WriteUint(uint64(result.Waiters))
				if len(result.PrevWaiter) > 0 {
					writer.WriteString("/")
					writer.WriteHex(result.PrevWaiter)
				}
			},
		)
	}

	trace.GetService(proc.GetService()).AddTxnDurationAction(
		txnOp,
		client.LockEvent,
		seq,
		tableID,
		time.Since(startAt),
		nil)

	// add bind locks
	if err = txnOp.AddLockTable(result.LockedOn); err != nil {
		return false, false, timestamp.Timestamp{}, err
	}

	snapshotTS := txnOp.Txn().SnapshotTS
	// if has no conflict, lockedTS means the latest commit ts of this table
	lockedTS := result.Timestamp

	// if no conflict, maybe data has been updated in [snapshotTS, lockedTS]. So wen need check here
	if result.NewLockAdd && // only check when new lock added, reentrant lock can skip check
		!result.HasConflict &&
		snapshotTS.LessEq(lockedTS) && // only retry when snapshotTS <= lockedTS, means lost some update in rc mode.
		txnOp.Txn().IsRCIsolation() {

		start = time.Now()
		// wait last committed logtail applied, (IO wait not related to FileService)
		newSnapshotTS, err := txnClient.WaitLogTailAppliedAt(ctx, lockedTS)
		if err != nil {
			return false, false, timestamp.Timestamp{}, err
		}
		// Record logtail waiting time
		analyzeLockWaitTime(analyzer, start)

		fn := opts.hasNewVersionInRangeFunc
		if fn == nil {
			fn = hasNewVersionInRange
		}

		// if [snapshotTS, newSnapshotTS] has been modified, need retry at new snapshot ts
		changed, err := fn(proc, rel, analyzer, tableID, eng, bat, idx, partitionIdx, snapshotTS, newSnapshotTS)
		if err != nil {
			return false, false, timestamp.Timestamp{}, err
		}

		if changed {
			trace.GetService(proc.GetService()).TxnNoConflictChanged(
				proc.GetTxnOperator(),
				tableID,
				lockedTS,
				newSnapshotTS)
			if err := txnOp.UpdateSnapshot(ctx, newSnapshotTS); err != nil {
				return false, false, timestamp.Timestamp{}, err
			}
			return true, false, newSnapshotTS, nil
		}
	}

	// no conflict or has conflict, but all prev txn all aborted
	// current txn can read and write normally
	if !result.HasConflict ||
		!result.HasPrevCommit {
		return true, false, timestamp.Timestamp{}, nil
	} else if lockedTS.Less(snapshotTS) {
		return true, false, timestamp.Timestamp{}, nil
	}

	// Arriving here means that at least one of the conflicting
	// transactions has committed.
	//
	// For the RC schema we need some retries between
	// [txn.snapshot ts, prev.commit ts] (de-duplication for insert, re-query for
	// update and delete).
	//
	// For the SI schema the current transaction needs to be abort (TODO: later
	// we can consider recording the ReadSet of the transaction and check if data
	// is modified between [snapshotTS,prev.commits] and raise the SnapshotTS of
	// the SI transaction to eliminate conflicts)
	if !txnOp.Txn().IsRCIsolation() {
		return false, false, timestamp.Timestamp{}, moerr.NewTxnWWConflict(ctx, tableID, "SI not support retry")
	}

	// forward rc's snapshot ts
	snapshotTS = result.Timestamp.Next()

	trace.GetService(proc.GetService()).TxnConflictChanged(
		proc.GetTxnOperator(),
		tableID,
		snapshotTS)
	if err := txnOp.UpdateSnapshot(ctx, snapshotTS); err != nil {
		return false, false, timestamp.Timestamp{}, err
	}
	return true, result.TableDefChanged, snapshotTS, nil
}

const defaultWaitTimeOnRetryLock = time.Second

func lockWithRetry(
	ctx context.Context,
	lockService lockservice.LockService,
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options lock.LockOptions,
	txnOp client.TxnOperator,
	fetchFunc FetchLockRowsFunc,
	vec *vector.Vector,
	opts LockOptions,
	pkType types.Type,
) (lock.Result, error) {
	var result lock.Result
	var err error

	result, err = LockWithMayUpgrade(ctx, lockService, tableID, rows, txnID, options, fetchFunc, vec, opts, pkType)
	if !canRetryLock(tableID, txnOp, err) {
		return result, err
	}

	for {
		result, err = lockService.Lock(ctx, tableID, rows, txnID, options)
		if !canRetryLock(tableID, txnOp, err) {
			break
		}
	}

	return result, err
}

func LockWithMayUpgrade(
	ctx context.Context,
	lockService lockservice.LockService,
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options lock.LockOptions,
	fetchFunc FetchLockRowsFunc,
	vec *vector.Vector,
	opts LockOptions,
	pkType types.Type,
) (lock.Result, error) {
	result, err := lockService.Lock(ctx, tableID, rows, txnID, options)
	if !moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade) {
		return result, err
	}

	// if return ErrLockNeedUpgrade, manually upgrade the lock and retry
	logutil.Infof("Trying to upgrade lock level due to too many row level locks for txn %s", txnID)
	opts = opts.WithLockTable(true, false)
	_, nrows, ng := fetchFunc(
		vec,
		opts.parker,
		pkType,
		opts.maxCountPerLock,
		opts.lockTable,
		opts.filter,
		opts.filterCols,
	)
	options.Granularity = ng
	return lockService.Lock(ctx, tableID, nrows, txnID, options)
}

func canRetryLock(table uint64, txn client.TxnOperator, err error) bool {
	if moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart) {
		time.Sleep(defaultWaitTimeOnRetryLock)
		return true
	}
	if txn.HasLockTable(table) {
		return false
	}
	if moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) ||
		moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound) {
		time.Sleep(defaultWaitTimeOnRetryLock)
		return true
	}
	if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) ||
		moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend) {
		time.Sleep(defaultWaitTimeOnRetryLock)
		return true
	}
	return false
}

// DefaultLockOptions create a default lock operation. The parker is used to
// encode primary key into lock row.
func DefaultLockOptions(parker *types.Packer) LockOptions {
	return LockOptions{
		mode:            lock.LockMode_Exclusive,
		lockTable:       false,
		maxCountPerLock: 0,
		parker:          parker,
	}
}

// WithLockSharding set lock sharding
func (opts LockOptions) WithLockSharding(sharding lock.Sharding) LockOptions {
	opts.sharding = sharding
	return opts
}

// WithLockGroup set lock group
func (opts LockOptions) WithLockGroup(group uint32) LockOptions {
	opts.group = group
	return opts
}

// WithLockMode set lock mode, Exclusive or Shared
func (opts LockOptions) WithLockMode(mode lock.LockMode) LockOptions {
	opts.mode = mode
	return opts
}

// WithLockTable set lock all table
func (opts LockOptions) WithLockTable(lockTable, changeDef bool) LockOptions {
	opts.lockTable = lockTable
	opts.changeDef = changeDef
	return opts
}

// WithMaxBytesPerLock every lock operation, will add some lock rows into
// lockservice. If very many rows of data are added at once, this can result
// in an excessive memory footprint. This value limits the amount of lock memory
// that can be allocated per lock operation, and if it is exceeded, it will be
// converted to a range lock.
func (opts LockOptions) WithMaxBytesPerLock(maxBytesPerLock int) LockOptions {
	opts.maxCountPerLock = maxBytesPerLock
	return opts
}

// WithFetchLockRowsFunc set the primary key into lock rows conversion function.
func (opts LockOptions) WithFetchLockRowsFunc(fetchFunc FetchLockRowsFunc) LockOptions {
	opts.fetchFunc = fetchFunc
	return opts
}

// WithFilterRows set filter rows, filterCols used to rowsFilter func
func (opts LockOptions) WithFilterRows(
	filter RowsFilter,
	filterCols []int32) LockOptions {
	opts.filter = filter
	opts.filterCols = filterCols
	return opts
}

// WithHasNewVersionInRangeFunc setup hasNewVersionInRange func
func (opts LockOptions) WithHasNewVersionInRangeFunc(fn hasNewVersionInRangeFunc) LockOptions {
	opts.hasNewVersionInRangeFunc = fn
	return opts
}

// NewArgument create new lock op argument.
func NewArgumentByEngine(engine engine.Engine) *LockOp {
	lock := reuse.Alloc[LockOp](nil)
	lock.engine = engine
	return lock
}

// AddLockTarget add lock targets
func (lockOp *LockOp) CopyToPipelineTarget() []*pipeline.LockTarget {
	targets := make([]*pipeline.LockTarget, len(lockOp.targets))
	for i, target := range lockOp.targets {
		targets[i] = &pipeline.LockTarget{
			TableId:            target.tableID,
			PrimaryColIdxInBat: target.primaryColumnIndexInBatch,
			PrimaryColTyp:      plan.MakePlan2Type(&target.primaryColumnType),
			RefreshTsIdxInBat:  target.refreshTimestampIndexInBatch,
			FilterColIdxInBat:  target.filterColIndexInBatch,
			LockTable:          target.lockTable,
			ChangeDef:          target.changeDef,
			Mode:               target.mode,
			LockRows:           plan.DeepCopyExpr(target.lockRows),
			LockTableAtTheEnd:  target.lockTableAtTheEnd,
			ObjRef:             plan.DeepCopyObjectRef(target.objRef),
		}
	}
	return targets
}

// AddLockTarget add lock target, LockMode_Exclusive will used
func (lockOp *LockOp) AddLockTarget(
	tableID uint64,
	objRef *plan.ObjectRef,
	primaryColumnIndexInBatch int32,
	primaryColumnType types.Type,
	partitionColIndexInBatch int32,
	refreshTimestampIndexInBatch int32,
	lockRows *plan.Expr,
	lockTableAtTheEnd bool) *LockOp {
	return lockOp.AddLockTargetWithMode(
		tableID,
		objRef,
		lock.LockMode_Exclusive,
		primaryColumnIndexInBatch,
		primaryColumnType,
		partitionColIndexInBatch,
		refreshTimestampIndexInBatch,
		lockRows,
		lockTableAtTheEnd)
}

// AddLockTargetWithMode add lock target with lock mode
func (lockOp *LockOp) AddLockTargetWithMode(
	tableID uint64,
	objRef *plan.ObjectRef,
	mode lock.LockMode,
	primaryColumnIndexInBatch int32,
	primaryColumnType types.Type,
	partitionColIndexInBatch int32,
	refreshTimestampIndexInBatch int32,
	lockRows *plan.Expr,
	lockTableAtTheEnd bool) *LockOp {
	lockOp.targets = append(lockOp.targets, lockTarget{
		tableID:                      tableID,
		objRef:                       objRef,
		primaryColumnIndexInBatch:    primaryColumnIndexInBatch,
		primaryColumnType:            primaryColumnType,
		refreshTimestampIndexInBatch: refreshTimestampIndexInBatch,
		mode:                         mode,
		lockRows:                     lockRows,
		lockTableAtTheEnd:            lockTableAtTheEnd,
		partitionColumnIndexInBatch:  partitionColIndexInBatch,
	})
	return lockOp
}

// LockTable lock all table, used for delete, truncate and drop table
func (lockOp *LockOp) LockTable(
	tableID uint64,
	changeDef bool) *LockOp {
	return lockOp.LockTableWithMode(
		tableID,
		lock.LockMode_Exclusive,
		changeDef)
}

// LockTableWithMode is similar to LockTable, but with specify
// lock mode
func (lockOp *LockOp) LockTableWithMode(
	tableID uint64,
	mode lock.LockMode,
	changeDef bool) *LockOp {
	for idx := range lockOp.targets {
		if lockOp.targets[idx].tableID == tableID {
			lockOp.targets[idx].lockTable = true
			lockOp.targets[idx].changeDef = changeDef
			lockOp.targets[idx].mode = mode
			break
		}
	}
	return lockOp
}

// AddLockTargetWithPartition add lock targets for partition tables. Our partitioned table implementation
// has each partition as a separate table. So when modifying data, these rows may belong to different
// partitions. For lock op does not care about the logic of data and partition mapping calculation, the
// caller needs to tell the lock op.
//
// tableIDs: the set of ids of the sub-tables of the partition to which the data of the current operation is
// attributed after calculation.
//
// partitionTableIDMappingInBatch: the ID index of the sub-table corresponding to the data. Index of tableIDs
func (lockOp *LockOp) AddLockTargetWithPartition(
	tableIDs []uint64,
	primaryColumnIndexInBatch int32,
	primaryColumnType types.Type,
	refreshTimestampIndexInBatch int32,
	lockRows *plan.Expr,
	lockTableAtTheEnd bool,
	partitionTableIDMappingInBatch int32) *LockOp {
	return lockOp.AddLockTargetWithPartitionAndMode(
		tableIDs,
		lock.LockMode_Exclusive,
		primaryColumnIndexInBatch,
		primaryColumnType,
		refreshTimestampIndexInBatch,
		lockRows,
		lockTableAtTheEnd,
		partitionTableIDMappingInBatch)
}

// AddLockTargetWithPartitionAndMode is similar to AddLockTargetWithPartition, but you can specify
// the lock mode
func (lockOp *LockOp) AddLockTargetWithPartitionAndMode(
	tableIDs []uint64,
	mode lock.LockMode,
	primaryColumnIndexInBatch int32,
	primaryColumnType types.Type,
	refreshTimestampIndexInBatch int32,
	lockRows *plan.Expr,
	lockTableAtTheEnd bool,
	partitionTableIDMappingInBatch int32) *LockOp {
	if len(tableIDs) == 0 {
		panic("invalid partition table ids")
	}

	// only one partition table, process as normal table
	if len(tableIDs) == 1 {
		return lockOp.AddLockTarget(tableIDs[0],
			nil,
			primaryColumnIndexInBatch,
			primaryColumnType,
			-1,
			refreshTimestampIndexInBatch,
			lockRows,
			lockTableAtTheEnd,
		)
	}

	for _, tableID := range tableIDs {
		lockOp.targets = append(lockOp.targets, lockTarget{
			tableID:                      tableID,
			primaryColumnIndexInBatch:    primaryColumnIndexInBatch,
			primaryColumnType:            primaryColumnType,
			refreshTimestampIndexInBatch: refreshTimestampIndexInBatch,
			filter:                       getRowsFilter(tableID, tableIDs),
			filterColIndexInBatch:        partitionTableIDMappingInBatch,
			mode:                         mode,
			lockRows:                     lockRows,
			lockTableAtTheEnd:            lockTableAtTheEnd,
		})
	}
	return lockOp
}

func (lockOp *LockOp) Reset(proc *process.Process, pipelineFailed bool, err error) {
	lockOp.resetParker()
	lockOp.ctr.retryError = nil
	lockOp.ctr.defChanged = false
}

// Free free mem
func (lockOp *LockOp) Free(proc *process.Process, pipelineFailed bool, err error) {
	lockOp.cleanParker()
	lockOp.ctr.relations = nil
}

func (lockOp *LockOp) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (lockOp *LockOp) resetParker() {
	if lockOp.ctr.parker != nil {
		lockOp.ctr.parker.Reset()
	}
}

func (lockOp *LockOp) cleanParker() {
	if lockOp.ctr.parker != nil {
		lockOp.ctr.parker.Close()
		lockOp.ctr.parker = nil
	}
}

func getRowsFilter(
	tableID uint64,
	partitionTables []uint64) RowsFilter {
	return func(
		row int,
		filterCols []int32) bool {
		return partitionTables[filterCols[row]] == tableID
	}
}

// [from, to].
// 1. if has a mvcc record <= from, return false, means no changed
// 2. otherwise return true, changed
func hasNewVersionInRange(
	proc *process.Process,
	rel engine.Relation,
	analyzer process.Analyzer,
	tableID uint64,
	eng engine.Engine,
	bat *batch.Batch,
	idx int32,
	partitionIdx int32,
	from, to timestamp.Timestamp,
) (bool, error) {
	if bat == nil {
		return false, nil
	}

	if rel == nil {
		var err error
		txnOp := proc.GetTxnOperator()
		_, _, rel, err = eng.GetRelationById(proc.Ctx, txnOp, tableID)
		if err != nil {
			if strings.Contains(err.Error(), "can not find table by id") {
				return false, nil
			}
			return false, err
		}
	}

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
	defer func() {
		if analyzer != nil {
			analyzer.AddS3RequestCount(crs)
			analyzer.AddFileServiceCacheInfo(crs)
			analyzer.AddDiskIO(crs)
		}
	}()

	fromTS := types.BuildTS(from.PhysicalTime, from.LogicalTime)
	toTS := types.BuildTS(to.PhysicalTime, to.LogicalTime)
	return rel.PrimaryKeysMayBeModified(newCtx, fromTS, toTS, bat, idx, partitionIdx)
}

func analyzeLockWaitTime(analyzer process.Analyzer, start time.Time) {
	if analyzer != nil {
		analyzer.WaitStop(start)
		analyzer.AddWaitLockTime(start)
	}
}

func lockTalbeIfLockCountIsZero(
	proc *process.Process,
	lockOp *LockOp,
) error {
	ctr := lockOp.ctr
	if ctr.lockCount != 0 {
		return nil
	}
	for idx := 0; idx < len(lockOp.targets); idx++ {
		target := lockOp.targets[idx]
		if target.lockRows != nil {
			vec, free, err := colexec.GetReadonlyResultFromNoColumnExpression(proc, target.lockRows)
			if err != nil {
				return err
			}
			defer func() {
				free()
			}()

			bat := batch.NewWithSize(int(target.primaryColumnIndexInBatch) + 1)
			bat.Vecs[target.primaryColumnIndexInBatch] = vec
			bat.SetRowCount(vec.Length())

			anal := lockOp.OpAnalyzer
			anal.Start()
			defer anal.Stop()
			err = performLock(bat, proc, lockOp, anal, idx)
			if err != nil {
				return err
			}
		} else {
			if !target.lockTableAtTheEnd {
				continue
			}
			err := LockTable(lockOp.engine, proc, target.tableID, target.primaryColumnType, false)
			if err != nil {
				return err
			}
		}
	}
	ctr.lockCount = 1

	return nil
}
