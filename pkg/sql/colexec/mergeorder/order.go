// Copyright 2021 Matrix Origin
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

package mergeorder

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_order"

func (ctr *container) mergeAndEvaluateOrderColumn(proc *process.Process, bat *batch.Batch) error {
	ctr.batchList = append(ctr.batchList, bat)
	ctr.orderCols = append(ctr.orderCols, nil)
	// if only one batch, no need to evaluate the order column.
	if len(ctr.batchList) == 1 {
		return nil
	}

	index := len(ctr.orderCols) - 1
	return ctr.evaluateOrderColumn(proc, index)
}

func (ctr *container) evaluateOrderColumn(proc *process.Process, index int) error {
	inputs := []*batch.Batch{ctr.batchList[index]}

	ctr.orderCols[index] = make([]*vector.Vector, len(ctr.executors))
	for i := 0; i < len(ctr.executors); i++ {
		vec, err := ctr.executors[i].EvalWithoutResultReusing(proc, inputs, nil)
		if err != nil {
			return err
		}
		ctr.orderCols[index][i] = vec
	}
	return nil
}

func (ctr *container) generateCompares(fs []*plan.OrderBySpec) {
	if len(ctr.compares) > 0 {
		return
	}
	var desc, nullsLast bool
	ctr.compares = make([]compare.Compare, len(fs))
	for i := range ctr.compares {
		desc = fs[i].Flag&plan2.OrderBySpec_DESC != 0
		if fs[i].Flag&plan2.OrderBySpec_NULLS_FIRST != 0 {
			nullsLast = false
		} else if fs[i].Flag&plan2.OrderBySpec_NULLS_LAST != 0 {
			nullsLast = true
		} else {
			nullsLast = desc
		}

		exprTyp := fs[i].Expr.Typ
		typ := types.New(types.T(exprTyp.Id), exprTyp.Width, exprTyp.Scale)
		ctr.compares[i] = compare.New(typ, desc, nullsLast)
	}
}

func (ctr *container) pickAndSend(proc *process.Process, result *vm.CallResult) (sendOver bool, err error) {
	mp := proc.Mp()
	if ctr.buf == nil {
		ctr.buf = batch.NewWithSize(ctr.batchList[0].VectorCount())
		for i := range ctr.buf.Vecs {
			ctr.buf.Vecs[i] = vector.NewVec(*ctr.batchList[0].Vecs[i].GetType())
		}
	} else {
		ctr.buf.CleanOnlyData()
	}

	wholeLength := 0
	for {
		choice := ctr.pickFirstRow()
		for j := range ctr.buf.Vecs {
			err = ctr.buf.Vecs[j].UnionOne(ctr.batchList[choice].Vecs[j], ctr.indexList[choice], mp)
			if err != nil {
				return false, err
			}
		}

		wholeLength++
		ctr.indexList[choice]++
		if ctr.indexList[choice] == int64(ctr.batchList[choice].RowCount()) {
			ctr.removeBatch(proc, choice)
		}

		if len(ctr.indexList) == 0 {
			sendOver = true
			break
		}
		if ctr.buf.Size() >= maxBatchSizeToSend {
			break
		}
	}
	ctr.buf.SetRowCount(wholeLength)
	result.Batch = ctr.buf
	return sendOver, nil
}

func (ctr *container) pickFirstRow() (batIndex int) {
	l := len(ctr.indexList)

	if l > 1 {
		i, j := 0, 1
		for j < l {
			for k := 0; k < len(ctr.compares); k++ {
				ctr.compares[k].Set(0, ctr.orderCols[i][k])
				ctr.compares[k].Set(1, ctr.orderCols[j][k])
				result := ctr.compares[k].Compare(0, 1, ctr.indexList[i], ctr.indexList[j])
				if result < 0 {
					break
				} else if result > 0 {
					i = j
					break
				} else if k == len(ctr.compares)-1 {
					break
				}
			}
			j++
		}
		return i
	}
	return 0
}

func (ctr *container) removeBatch(proc *process.Process, index int) {
	bat := ctr.batchList[index]
	cols := ctr.orderCols[index]

	alreadyPut := make(map[*vector.Vector]bool, len(bat.Vecs))
	for i := range bat.Vecs {
		alreadyPut[bat.Vecs[i]] = true
	}
	ctr.batchList = append(ctr.batchList[:index], ctr.batchList[index+1:]...)
	ctr.indexList = append(ctr.indexList[:index], ctr.indexList[index+1:]...)

	for i := range cols {
		if _, ok := alreadyPut[cols[i]]; ok {
			continue
		}
		cols[i].Free(proc.GetMPool())
	}
	for v := range alreadyPut {
		v.Free(proc.GetMPool())
	}
	ctr.orderCols = append(ctr.orderCols[:index], ctr.orderCols[index+1:]...)
}

func (mergeOrder *MergeOrder) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	ap := mergeOrder
	buf.WriteString(": mergeorder([")
	for i, f := range ap.OrderBySpecs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (mergeOrder *MergeOrder) OpType() vm.OpType {
	return vm.MergeOrder
}

func (mergeOrder *MergeOrder) Prepare(proc *process.Process) (err error) {
	if mergeOrder.OpAnalyzer == nil {
		mergeOrder.OpAnalyzer = process.NewAnalyzer(mergeOrder.GetIdx(), mergeOrder.IsFirst, mergeOrder.IsLast, "merge order")
	} else {
		mergeOrder.OpAnalyzer.Reset()
	}

	ctr := &mergeOrder.ctr
	if len(mergeOrder.ctr.executors) == 0 {
		ctr.batchList = make([]*batch.Batch, 0, defaultCacheBatchSize)
		ctr.orderCols = make([][]*vector.Vector, 0, defaultCacheBatchSize)

		mergeOrder.ctr.executors = make([]colexec.ExpressionExecutor, len(mergeOrder.OrderBySpecs))
		for i := range mergeOrder.ctr.executors {
			mergeOrder.ctr.executors[i], err = colexec.NewExpressionExecutor(proc, mergeOrder.OrderBySpecs[i].Expr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (mergeOrder *MergeOrder) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := mergeOrder.OpAnalyzer

	ctr := &mergeOrder.ctr
	for {
		switch ctr.status {
		case receiving:
			input, err := vm.ChildrenCall(mergeOrder.GetChildren(0), proc, analyzer)
			if err != nil {
				return vm.CancelResult, err
			}

			if input.Batch == nil {
				// if number of block is less than 2, no need to do merge sort.
				ctr.status = normalSending

				if len(ctr.batchList) > 1 {
					ctr.status = pickUpSending

					// evaluate the first batch's order column.
					if err = ctr.evaluateOrderColumn(proc, 0); err != nil {
						return input, err
					}
					ctr.generateCompares(mergeOrder.OrderBySpecs)
					ctr.indexList = make([]int64, len(ctr.batchList))
				}
				continue
			}

			if input.Batch.IsEmpty() {
				continue
			}

			bat, err := input.Batch.Dup(proc.GetMPool())
			if err != nil {
				return vm.CancelResult, err
			}
			analyzer.Alloc(int64(bat.Size()))
			if err = ctr.mergeAndEvaluateOrderColumn(proc, bat); err != nil {
				return vm.CancelResult, err
			}

		case normalSending:
			if len(ctr.batchList) == 0 {
				return vm.CancelResult, nil
			}

			// If only one batch, no need to sort. just send it.
			if len(ctr.batchList) == 1 {
				if ctr.buf != nil {
					ctr.buf.Clean(proc.Mp())
					ctr.buf = nil
				}
				ctr.buf = ctr.batchList[0]
				ctr.batchList[0] = nil
				result := vm.NewCallResult()
				result.Batch = ctr.buf
				return result, nil
			}
			return vm.CancelResult, nil

		case pickUpSending:
			result := vm.NewCallResult()
			sendOver, err := ctr.pickAndSend(proc, &result)
			if sendOver {
				ctr.status = finish
				return result, err
			}
			result.Status = vm.ExecHasMore
			return result, err

		case finish:
			return vm.CancelResult, nil
		}
	}
}
