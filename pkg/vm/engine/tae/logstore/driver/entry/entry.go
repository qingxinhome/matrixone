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

package entry

import (
	"bytes"
	"io"
	"os"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type Entry struct {
	Entry entry.Entry
	Info  *entry.Info //for wal in post append
	DSN   uint64      // driver sequence number
	Ctx   any         //for addr in batchstore
	err   error
	wg    *sync.WaitGroup

	//for replay
	isEnd bool
}

func NewEntry(e entry.Entry) *Entry {
	en := &Entry{
		Entry: e,
		wg:    &sync.WaitGroup{},
	}
	en.wg.Add(1)
	return en
}
func NewEmptyEntry() *Entry {
	en := &Entry{
		Entry: entry.GetBase(),
		wg:    &sync.WaitGroup{},
	}
	en.wg.Add(1)
	return en
}

func NewEndEntry() *Entry {
	return &Entry{
		isEnd: true,
	}
}
func (e *Entry) IsEnd() bool {
	return e.isEnd
}
func (e *Entry) SetInfo() {
	info := e.Entry.GetInfo()
	if info != nil {
		e.Info = info.(*entry.Info)
	}
}
func (e *Entry) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(types.EncodeUint64(&e.DSN)); err != nil {
		return
	}
	_, err = e.Entry.ReadFrom(r)
	if err != nil {
		panic(err)
	}
	e.Info = e.Entry.GetInfo().(*entry.Info)
	return
}

func (e *Entry) UnmarshalBinary(buf []byte) (n int64, err error) {
	e.DSN = types.DecodeUint64(buf[:8])
	n += 8
	n2, err := e.Entry.UnmarshalBinary(buf[n:])
	if err != nil {
		panic(err)
	}
	n += n2
	e.Info = e.Entry.GetInfo().(*entry.Info)
	return
}

func (e *Entry) ReadAt(r *os.File, offset int) (int, error) {
	lsnbuf := make([]byte, 8)
	n, err := r.ReadAt(lsnbuf, int64(offset))
	if err != nil {
		return n, err
	}
	offset += 8

	bbuf := bytes.NewBuffer(lsnbuf)
	if _, err := bbuf.Read(types.EncodeUint64(&e.DSN)); err != nil {
		return n, err
	}

	n2, err := e.Entry.ReadAt(r, offset)
	return n2 + n, err
}

func (e *Entry) WriteTo(w io.Writer) (int64, error) {
	if _, err := w.Write(types.EncodeUint64(&e.DSN)); err != nil {
		return 0, err
	}
	n, err := e.Entry.WriteTo(w)
	n += 8
	return n, err
}

func (e *Entry) WaitDone() error {
	e.wg.Wait()
	return e.err
}

func (e *Entry) DoneWithErr(err error) {
	e.err = err
	info := e.Entry.GetInfo()
	if info != nil {
		e.Info = info.(*entry.Info)
	}
	e.wg.Done()
}

func (e *Entry) GetSize() int {
	return e.Entry.TotalSize() + 8 //LSN
}
