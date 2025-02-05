// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type tableOffset struct {
	offset int
	end    int
}

func MergeCheckpoint(
	ctx context.Context,
	taskName string,
	sid string,
	ckpEntries []*checkpoint.CheckpointEntry,
	bf *bloomfilter.BloomFilter,
	end *types.TS,
	client checkpoint.Runner,
	pool *mpool.MPool,
	fs fileservice.FileService,
) (deleteFiles, newFiles []string, checkpointEntry *checkpoint.CheckpointEntry, ckpData *logtail.CheckpointData, err error) {
	ckpData = logtail.NewCheckpointData(sid, pool)
	datas := make([]*logtail.CheckpointData, 0)
	deleteFiles = make([]string, 0)
	for _, ckpEntry := range ckpEntries {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		default:
		}
		logutil.Info(
			"GC-Merge-Checkpoint",
			zap.String("task", taskName),
			zap.String("entry", ckpEntry.String()),
		)
		var data *logtail.CheckpointData
		var locations map[string]objectio.Location
		if _, data, err = logtail.LoadCheckpointEntriesFromKey(
			ctx,
			sid,
			fs,
			ckpEntry.GetLocation(),
			ckpEntry.GetVersion(),
			nil,
			&types.TS{},
		); err != nil {
			return
		}
		datas = append(datas, data)
		var nameMeta string
		if ckpEntry.GetType() == checkpoint.ET_Compacted {
			nameMeta = ioutil.EncodeCompactCKPMetadataFullName(
				ckpEntry.GetStart(), ckpEntry.GetEnd(),
			)
		} else {
			nameMeta = ioutil.EncodeCKPMetadataFullName(
				ckpEntry.GetStart(), ckpEntry.GetEnd(),
			)
		}

		// add checkpoint metafile(ckp/mete_ts-ts.ckp...) to deleteFiles
		deleteFiles = append(deleteFiles, nameMeta)
		// add checkpoint idx file to deleteFiles
		deleteFiles = append(deleteFiles, ckpEntry.GetLocation().Name().String())
		locations, err = logtail.LoadCheckpointLocations(
			ctx, sid, ckpEntry.GetTNLocation(), ckpEntry.GetVersion(), fs,
		)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				deleteFiles = append(deleteFiles, nameMeta)
				continue
			}
			return
		}

		for name := range locations {
			deleteFiles = append(deleteFiles, name)
		}
	}
	defer func() {
		for _, data := range datas {
			data.Close()
		}
	}()
	if len(datas) == 0 {
		return
	}

	newFiles = make([]string, 0)

	// merge objects referenced by sansphot and pitr
	for _, data := range datas {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		default:
		}
		ins := data.GetObjectBatchs()
		tombstone := data.GetTombstoneObjectBatchs()
		bf.Test(ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(),
			func(exists bool, i int) {
				if !exists {
					return
				}
				appendValToBatch(ins, ckpData.GetObjectBatchs(), i)
			})
		bf.Test(tombstone.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(),
			func(exists bool, i int) {
				if !exists {
					return
				}
				appendValToBatch(tombstone, ckpData.GetTombstoneObjectBatchs(), i)
			})
	}

	tidColIdx := 4
	objectBatch := containers.ToCNBatch(ckpData.GetObjectBatchs())
	err = mergeutil.SortColumnsByIndex(objectBatch.Vecs, tidColIdx, pool)
	if err != nil {
		return
	}

	tombstoneBatch := containers.ToCNBatch(ckpData.GetTombstoneObjectBatchs())
	err = mergeutil.SortColumnsByIndex(tombstoneBatch.Vecs, tidColIdx, pool)
	if err != nil {
		return
	}

	// Update checkpoint Dat[meta]
	tableInsertOff := make(map[uint64]*tableOffset)
	tableTombstoneOff := make(map[uint64]*tableOffset)
	tableInsertTid := vector.MustFixedColNoTypeCheck[uint64](
		ckpData.GetObjectBatchs().GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	tableTombstoneTid := vector.MustFixedColNoTypeCheck[uint64](
		ckpData.GetTombstoneObjectBatchs().GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < ckpData.GetObjectBatchs().Vecs[0].Length(); i++ {
		tid := tableInsertTid[i]
		if tableInsertOff[tid] == nil {
			tableInsertOff[tid] = &tableOffset{
				offset: i,
				end:    i,
			}
		}
		tableInsertOff[tid].end += 1
	}
	for i := 0; i < ckpData.GetTombstoneObjectBatchs().Vecs[0].Length(); i++ {
		tid := tableTombstoneTid[i]
		if tableTombstoneOff[tid] == nil {
			tableTombstoneOff[tid] = &tableOffset{
				offset: i,
				end:    i,
			}
		}
		tableTombstoneOff[tid].end += 1
	}

	for tid, table := range tableInsertOff {
		ckpData.UpdateObjectInsertMeta(tid, int32(table.offset), int32(table.end))
	}
	for tid, table := range tableTombstoneOff {
		ckpData.UpdateTombstoneInsertMeta(tid, int32(table.offset), int32(table.end))
	}
	cnLocation, tnLocation, files, err := ckpData.WriteTo(
		ctx, logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize, fs,
	)
	if err != nil {
		return
	}

	newFiles = append(newFiles, files...)
	bat := makeBatchFromSchema(checkpoint.CheckpointSchema)
	bat.GetVectorByName(checkpoint.CheckpointAttr_StartTS).Append(ckpEntries[0].GetStart(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EndTS).Append(*end, false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_MetaLocation).Append([]byte(cnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EntryType).Append(false, false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Version).Append(ckpEntries[len(ckpEntries)-1].GetVersion(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_AllLocations).Append([]byte(tnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_CheckpointLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_TruncateLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Type).Append(int8(checkpoint.ET_Compacted), false)
	defer bat.Close()
	name := ioutil.EncodeCompactCKPMetadataFullName(ckpEntries[0].GetStart(), *end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, fs)
	if err != nil {
		return
	}
	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(ctx)
	if err != nil {
		return
	}
	_, tsFile := ioutil.TryDecodeTSRangeFile(name)
	client.AddCheckpointMetaFile(tsFile.GetName())
	checkpointEntry = checkpoint.NewCheckpointEntry("", ckpEntries[0].GetStart(), *end, checkpoint.ET_Compacted)
	checkpointEntry.SetLocation(cnLocation, tnLocation)
	checkpointEntry.SetLSN(ckpEntries[len(ckpEntries)-1].LSN(), ckpEntries[len(ckpEntries)-1].GetTruncateLsn())
	checkpointEntry.SetState(checkpoint.ST_Finished)
	checkpointEntry.SetVersion(logtail.CheckpointCurrentVersion)
	newFiles = append(newFiles, name)
	return
}

func makeBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	bat := containers.NewBatch()
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	return bat
}

func appendValToBatch(src, dst *containers.Batch, row int) {
	for v, vec := range src.Vecs {
		val := vec.Get(row)
		if val == nil {
			dst.Vecs[v].Append(val, true)
		} else {
			dst.Vecs[v].Append(val, false)
		}
	}
}
