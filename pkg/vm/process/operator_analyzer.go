// Copyright 2022 Matrix Origin
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

package process

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// Analyze analyzes information for operator
type Analyzer interface {
	Start()
	Stop()
	ChildrenCallStop(time time.Time)
	Alloc(int64)
	Input(batch *batch.Batch)
	Output(*batch.Batch)
	WaitStop(time.Time)
	Network(*batch.Batch)
	AddScanTime(t time.Time)
	AddInsertTime(t time.Time)
	ServiceInvokeTime(t time.Time)
	GetOpStats() *OperatorStats
	Reset()

	InputBlock()
	DiskIO(*batch.Batch)   // delete it, unused
	S3IOByte(*batch.Batch) // delete it, unused
	S3IOInputCount(int)    // delete it, unused
	S3IOOutputCount(int)   // delete it, unused
}

// Operator Resource operatorAnalyzer
type operatorAnalyzer struct {
	nodeIdx              int
	isFirst              bool
	isLast               bool
	start                time.Time
	wait                 time.Duration
	childrenCallDuration time.Duration
	opStats              *OperatorStats
}

var _ Analyzer = &operatorAnalyzer{}

func NewAnalyzer(idx int, isFirst bool, isLast bool, operatorName string) Analyzer {
	return &operatorAnalyzer{
		nodeIdx:              idx,
		isFirst:              isFirst,
		isLast:               isLast,
		wait:                 0,
		childrenCallDuration: 0,
		opStats:              NewOperatorStats(operatorName),
	}
}

func (opAlyzr *operatorAnalyzer) Reset() {
	opAlyzr.wait = 0
	opAlyzr.childrenCallDuration = 0
	opAlyzr.opStats.Reset()
}

func (opAlyzr *operatorAnalyzer) Start() {
	opAlyzr.start = time.Now()
	opAlyzr.wait = 0
	opAlyzr.childrenCallDuration = 0
}

func (opAlyzr *operatorAnalyzer) Stop() {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Stop: operatorAnalyzer.opStats is nil")
	}

	// Calculate waiting time and total time consumption
	waitDuration := opAlyzr.wait

	opDuration := time.Since(opAlyzr.start)
	totalDuration := opDuration - waitDuration - opAlyzr.childrenCallDuration

	// Check if the time consumption is legal
	if totalDuration < 0 {
		str := fmt.Sprintf("opName:%s, opDuration: %v, waitDuration:%v, childrenCallDuration:%v , start:%v, end:%v\n",
			opAlyzr.opStats.OperatorName,
			opDuration,
			waitDuration,
			opAlyzr.childrenCallDuration,
			opAlyzr.start,
			time.Now())
		panic("Time consumed by the operator cannot be less than 0, " + str)
		//fmt.Printf("Time consumed by the operator cannot be less than 0, %s \n", str)
		//duration := time.Since(start)
		//nonNegativeDuration := time.Duration(math.Max(0, float64(duration)))
	}

	// Update the statistical information of the operation analyzer
	opAlyzr.opStats.TotalWaitTimeConsumed += waitDuration.Nanoseconds()
	opAlyzr.opStats.TotalTimeConsumed += totalDuration.Nanoseconds()
	opAlyzr.opStats.CallNum++
}

func (opAlyzr *operatorAnalyzer) Alloc(size int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Alloc: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.TotalMemorySize += size
}

func (opAlyzr *operatorAnalyzer) InputBlock() {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.InputBlock: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.TotalInputBlocks += 1
}

func (opAlyzr *operatorAnalyzer) Input(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Input: operatorAnalyzer.opStats is nil")
	}

	if bat != nil && opAlyzr.isFirst {
		opAlyzr.opStats.TotalInputSize += int64(bat.Size())
		opAlyzr.opStats.TotalInputRows += int64(bat.RowCount())
	}
}

func (opAlyzr *operatorAnalyzer) Output(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Output: operatorAnalyzer.opStats is nil")
	}

	if bat != nil && opAlyzr.isLast {
		opAlyzr.opStats.TotalOutputSize += int64(bat.Size())
		opAlyzr.opStats.TotalOutputRows += int64(bat.RowCount())
	}
}

func (opAlyzr *operatorAnalyzer) WaitStop(start time.Time) {
	opAlyzr.wait += time.Since(start)
}

func (opAlyzr *operatorAnalyzer) ChildrenCallStop(start time.Time) {
	opAlyzr.childrenCallDuration += time.Since(start)
}

func (opAlyzr *operatorAnalyzer) DiskIO(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.DiskIO: operatorAnalyzer.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.TotalDiskIO += int64(bat.Size())
	}
}

func (opAlyzr *operatorAnalyzer) S3IOByte(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.S3IOByte: operatorAnalyzer.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.TotalS3IOByte += int64(bat.Size())
	}
}

func (opAlyzr *operatorAnalyzer) S3IOInputCount(count int) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.S3IOInputCount: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.TotalS3InputCount += int64(count)
}

func (opAlyzr *operatorAnalyzer) S3IOOutputCount(count int) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.S3IOOutputCount: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.TotalS3OutputCount += int64(count)
}

func (opAlyzr *operatorAnalyzer) Network(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Network: operatorAnalyzer.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.TotalNetworkIO += int64(bat.Size())
	}
}

func (opAlyzr *operatorAnalyzer) AddScanTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddScanTime: operatorAnalyzer.opStats is nil")
	}
	duration := time.Since(t)
	opAlyzr.opStats.TotalScanTime += duration.Nanoseconds()
}

func (opAlyzr *operatorAnalyzer) AddInsertTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddInsertTime: operatorAnalyzer.opStats is nil")
	}
	duration := time.Since(t)
	opAlyzr.opStats.TotalInsertTime += duration.Nanoseconds()
}

func (opAlyzr *operatorAnalyzer) ServiceInvokeTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.ServiceInvokeTime: operatorAnalyzer.opStats is nil")
	}
	duration := time.Since(t)
	opAlyzr.opStats.TotalServiceTime += duration.Nanoseconds()
}

func (opAlyzr *operatorAnalyzer) GetOpStats() *OperatorStats {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.GetOpStats(): operatorAnalyzer.opStats is nil")
	}
	return opAlyzr.opStats
}

type OperatorStats struct {
	OperatorName          string `json:"-"`
	CallNum               int    `json:"CallCount,omitempty"`
	TotalTimeConsumed     int64  `json:"TotalTimeConsumed,omitempty"`
	TotalWaitTimeConsumed int64  `json:"TotalWaitTimeConsumed,omitempty"`
	TotalMemorySize       int64  `json:"TotalMemorySize,omitempty"`
	TotalInputRows        int64  `json:"TotalInputRows,omitempty"`
	TotalInputSize        int64  `json:"TotalInputSize,omitempty"`
	TotalOutputRows       int64  `json:"TotalOutputRows,omitempty"`
	TotalOutputSize       int64  `json:"TotalOutputSize,omitempty"`
	TotalNetworkIO        int64  `json:"TotalNetworkIO,omitempty"`
	TotalInputBlocks      int64  `json:"-"`
	TotalDiskIO           int64  `json:"-"`
	TotalS3IOByte         int64  `json:"-"`
	TotalS3InputCount     int64  `json:"-"`
	TotalS3OutputCount    int64  `json:"-"`
	TotalScanTime         int64  `json:"-"`
	TotalInsertTime       int64  `json:"-"`
	TotalServiceTime      int64  `json:"-"`
}

func NewOperatorStats(operatorName string) *OperatorStats {
	return &OperatorStats{
		OperatorName: operatorName,
	}
}

func (ps *OperatorStats) Reset() {
	*ps = OperatorStats{}
}

func (ps *OperatorStats) String() string {
	return fmt.Sprintf(" CallNum:%d "+
		"TimeCost:%dns "+
		"WaitTime:%dns "+
		"InRows:%d "+
		"OutRows:%d "+
		"InSize:%dbytes "+
		"InBlock:%d "+
		"OutSize:%dbytes "+
		"MemSize:%dbytes "+
		"DiskIO:%dbytes "+
		"S3IOByte:%dbytes "+
		"S3InCount:%d "+
		"S3OutCount:%d "+
		"NetworkIO:%dbytes "+
		"ScanTime:%dns "+
		"ServiceTime:%dns",
		ps.CallNum,
		ps.TotalTimeConsumed,
		ps.TotalWaitTimeConsumed,
		ps.TotalInputRows,
		ps.TotalOutputRows,
		ps.TotalInputSize,
		ps.TotalInputBlocks,
		ps.TotalOutputSize,
		ps.TotalMemorySize,
		ps.TotalDiskIO,
		ps.TotalS3IOByte,
		ps.TotalS3InputCount,
		ps.TotalS3OutputCount,
		ps.TotalNetworkIO,
		ps.TotalScanTime,
		ps.TotalServiceTime)
}

func (ps *OperatorStats) ReducedString() string {
	return fmt.Sprintf(" CallNum:%d "+
		"TimeCost:%dns "+
		"WaitTime:%dns "+
		"InRows:%d "+
		"OutRows:%d "+
		"InSize:%dbytes "+
		"OutSize:%dbytes "+
		"MemSize:%dbytes "+
		"Network:%dbytes",
		ps.CallNum,
		ps.TotalTimeConsumed,
		ps.TotalWaitTimeConsumed,
		ps.TotalInputRows,
		ps.TotalOutputRows,
		ps.TotalInputSize,
		ps.TotalOutputSize,
		ps.TotalMemorySize,
		ps.TotalNetworkIO,
	)
}