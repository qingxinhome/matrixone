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

package service

import (
	"context"
	"sync"
	"time"

	"go.uber.org/ratelimit"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	defaultRequestChanSize = 512
	// defaultRequestDeadline : default deadline for every request (subscribe and unsubscribe).
	defaultRequestDeadline = 2 * time.Minute
)

type ClientOption func(*LogtailClient)

func WithClientRequestPerSecond(rps int) ClientOption {
	return func(c *LogtailClient) {
		c.options.rps = rps
	}
}

// LogtailClient encapsulates morpc stream.
type LogtailClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	// requestC is a chan, which receives all sub/unsub request.
	// There is another worker send the items in the chan to stream.
	requestC chan *LogtailRequest

	stream    morpc.Stream
	recvChan  chan morpc.Message
	breakChan chan struct{}
	broken    chan struct{} // mark morpc stream as broken when necessary
	once      sync.Once

	options struct {
		rps int
	}

	limiter ratelimit.Limiter
}

// NewLogtailClient constructs LogtailClient.
func NewLogtailClient(ctx context.Context, stream morpc.Stream, opts ...ClientOption) (*LogtailClient, error) {
	ctx, cancel := context.WithCancel(ctx)
	client := &LogtailClient{
		ctx:       ctx,
		cancel:    cancel,
		requestC:  make(chan *LogtailRequest, defaultRequestChanSize),
		stream:    stream,
		broken:    make(chan struct{}),
		breakChan: make(chan struct{}, 10),
	}

	recvChan, err := stream.Receive()
	if err != nil {
		logutil.Error("logtail client: fail to fetch message channel from morpc stream", zap.Error(err))
		return nil, err
	}
	client.recvChan = recvChan

	client.options.rps = 200
	for _, opt := range opts {
		opt(client)
	}
	client.limiter = ratelimit.New(client.options.rps)

	go func() {
		if wErr := client.sendWorker(); wErr != nil {
			logutil.Infof("logtail client send worker returned: %v", wErr)
		}
	}()

	return client, nil
}

// Close closes stream.
func (c *LogtailClient) Close() error {
	err := c.stream.Close(true)
	if err != nil {
		logutil.Error("logtail client: fail to close morpc stream", zap.Error(err))
	}
	if c.cancel != nil {
		c.cancel()
	}
	return err
}

// Subscribe subscribes table.
func (c *LogtailClient) Subscribe(
	ctx context.Context, table api.TableID,
) error {
	if c.streamBroken() {
		logutil.Error("logtail client: subscribe via broken morpc stream")
		return moerr.NewStreamClosedNoCtx()
	}

	c.limiter.Take()

	request := &LogtailRequest{}
	request.Request = &logtail.LogtailRequest_SubscribeTable{
		SubscribeTable: &logtail.SubscribeRequest{
			Table: &table,
		},
	}
	return c.sendRequest(request)
}

// Unsubscribe cancel subscription for table.
func (c *LogtailClient) Unsubscribe(
	ctx context.Context, table api.TableID,
) error {
	if c.streamBroken() {
		logutil.Error("logtail client: unsubscribe via broken morpc stream")
		return moerr.NewStreamClosedNoCtx()
	}

	c.limiter.Take()

	request := &LogtailRequest{}
	request.Request = &logtail.LogtailRequest_UnsubscribeTable{
		UnsubscribeTable: &logtail.UnsubscribeRequest{
			Table: &table,
		},
	}
	return c.sendRequest(request)
}

func (c *LogtailClient) BreakoutReceive() {
	c.breakChan <- struct{}{}
}

// Receive fetches logtail response.
//
// 1. response for error: *LogtailResponse.GetError() != nil
// 2. response for subscription: *LogtailResponse.GetSubscribeResponse() != nil
// 3. response for unsubscription: *LogtailResponse.GetUnsubscribeResponse() != nil
// 3. response for incremental logtail: *LogtailResponse.GetUpdateResponse() != nil
func (c *LogtailClient) Receive(ctx context.Context) (*LogtailResponse, error) {
	recvFunc := func() (*LogtailResponseSegment, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-c.breakChan:
			return nil, moerr.NewInternalErrorNoCtx("logtail client: reconnect breakout")

		case <-c.broken:
			return nil, moerr.NewStreamClosedNoCtx()

		case message, ok := <-c.recvChan:
			if !ok || message == nil {
				logutil.Error("logtail client: morpc stream broken",
					zap.Bool("is message nil", message == nil),
					zap.Bool("is message channel closed", !ok),
				)

				// mark stream as broken
				c.once.Do(func() { close(c.broken) })
				return nil, moerr.NewStreamClosedNoCtx()
			}
			v2.LogTailReceiveQueueSizeGauge.Set(float64(len(c.recvChan)))
			return message.(*LogtailResponseSegment), nil
		}
	}

	prev, err := recvFunc()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, prev.MessageSize)
	buf = AppendChunk(buf, prev.GetPayload())

	for prev.Sequence < prev.MaxSequence {
		segment, err := recvFunc()
		if err != nil {
			return nil, err
		}
		buf = AppendChunk(buf, segment.GetPayload())
		prev = segment
	}

	resp := &LogtailResponse{}
	if err := resp.Unmarshal(buf); err != nil {
		logutil.Error("logtail client: fail to unmarshal logtail response", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// streamBroken returns true if stream is borken.
func (c *LogtailClient) streamBroken() bool {
	select {
	case <-c.broken:
		return true
	default:
	}
	return false
}

func (c *LogtailClient) sendRequest(request *LogtailRequest) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()

	case c.requestC <- request:
		return nil
	}
}

func (c *LogtailClient) sendWorker() error {
	sendFn := func(request *LogtailRequest) error {
		request.SetID(c.stream.ID())
		ctx, cancel := context.WithTimeoutCause(c.ctx, defaultRequestDeadline, moerr.CauseLogTailRequest)
		defer cancel()
		return c.stream.Send(ctx, request)
	}

	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()

		case request := <-c.requestC:
			if err := sendFn(request); err != nil {
				logutil.Error("logtail client: fail to send sub/unsub request via morpc stream", zap.Error(err))
			}
		}
	}
}
