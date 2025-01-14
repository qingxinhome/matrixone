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

package catalog

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/tidwall/btree"
)

const (
	ObjectState_Create_Active uint8 = iota
	ObjectState_Create_PrepareCommit
	ObjectState_Create_ApplyCommit
	ObjectState_Delete_Active
	ObjectState_Delete_PrepareCommit
	ObjectState_Delete_ApplyCommit
)

type ObjectList struct {
	isTombstone bool
	*sync.RWMutex
	sortHint_objectID map[objectio.ObjectId]uint64
	tree              atomic.Pointer[btree.BTreeG[*ObjectEntry]]
}

func NewObjectList(isTombstone bool) *ObjectList {
	opts := btree.Options{
		Degree:  64,
		NoLocks: true,
	}
	tree := btree.NewBTreeGOptions((*ObjectEntry).Less, opts)
	list := &ObjectList{
		RWMutex:           &sync.RWMutex{},
		sortHint_objectID: make(map[types.Objectid]uint64),
		isTombstone:       isTombstone,
	}
	list.tree.Store(tree)
	return list
}

func (l *ObjectList) GetObjectByID(objectID *objectio.ObjectId) (obj *ObjectEntry, err error) {
	l.RLock()
	sortHint := l.sortHint_objectID[*objectID]
	l.RUnlock()
	obj = l.GetLastestNode(sortHint)
	if obj == nil {
		err = moerr.GetOkExpectedEOB()
	}
	return
}

func (l *ObjectList) deleteEntryLocked(sortHint uint64) error {
	l.Lock()
	defer l.Unlock()
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	if obj := l.getObjectBySortHint(sortHint); obj != nil {
		newTree.Delete(obj)
		delete(l.sortHint_objectID, *obj.ID())
	}
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
	return nil
}

func (l *ObjectList) getObjectBySortHint(sortHint uint64) (ret *ObjectEntry) {
	it := l.tree.Load().Iter()
	defer it.Release()
	pivot := ObjectEntry{
		ObjectNode: ObjectNode{SortHint: sortHint},
	}
	if ok := it.Seek(&pivot); !ok {
		return
	}
	if ret = it.Item(); ret.SortHint != sortHint {
		ret = nil
	}
	return
}

func (l *ObjectList) GetLastestNode(sortHint uint64) *ObjectEntry {
	return l.getObjectBySortHint(sortHint)
}

func (l *ObjectList) DropObjectByID(
	objectID *objectio.ObjectId,
	txn txnif.TxnReader,
) (
	droppedObj *ObjectEntry,
	isNew bool,
	err error,
) {
	obj, err := l.GetObjectByID(objectID)
	if err != nil {
		return
	}
	if obj.HasDropIntent() {
		return nil, false, moerr.GetOkExpectedEOB()
	}
	if !obj.DeleteNode.IsEmpty() {
		panic("logic error")
	}
	needWait, txnToWait := obj.CreateNode.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.CreateNode.CheckConflict(txn); err != nil {
		return nil, false, err
	}
	droppedObj, isNew = obj.GetDropEntry(txn)
	l.Update(droppedObj, obj)
	return
}
func (l *ObjectList) Set(object *ObjectEntry, registerSortHint bool) {
	if object.IsTombstone != l.isTombstone {
		panic("logic error")
	}
	l.Lock()
	defer l.Unlock()
	oldTree := l.tree.Load()
	if registerSortHint {
		obj, ok := oldTree.Get(&ObjectEntry{
			ObjectNode: ObjectNode{SortHint: object.SortHint},
		})
		if ok {
			panic(fmt.Sprintf("logic error, duplicate sort hint, obj %v %v, sort hint %v",
				obj.ID().String(), object.ID().String(), object.SortHint))
		}
		l.sortHint_objectID[*object.ID()] = object.SortHint
	} else {
		sortHint, ok := l.sortHint_objectID[*object.ID()]
		if !ok || sortHint != object.SortHint {
			panic("logic error")
		}
	}
	newTree := oldTree.Copy()
	newTree.Set(object)
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
}
func (l *ObjectList) Update(new, old *ObjectEntry) {
	l.Lock()
	defer l.Unlock()
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	if new.IsTombstone != l.isTombstone {
		panic("logic error")
	}
	newTree.Delete(old)
	newTree.Set(new)
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
}
func (l *ObjectList) Delete(obj *ObjectEntry) {
	l.Lock()
	defer l.Unlock()
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	newTree.Delete(obj)
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
}
func (l *ObjectList) UpdateObjectInfo(
	obj *ObjectEntry,
	txn txnif.TxnReader,
	stats *objectio.ObjectStats,
) (isNew bool, err error) {
	needWait, txnToWait := obj.GetLastMVCCNode().NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.GetLastMVCCNode().CheckConflict(txn); err != nil {
		return false, err
	}
	newObj, isNew := obj.GetUpdateEntry(txn, stats)
	l.Set(newObj, false)
	return
}
