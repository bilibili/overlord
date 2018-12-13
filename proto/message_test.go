package proto

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessage(t *testing.T) {
	msgs := GetMsgs(1)
	assert.Len(t, msgs, 1)
	PutMsgs(msgs)
	msgs = GetMsgs(1, 1)
	assert.Len(t, msgs, 1)
	PutMsgs(msgs)

	msg := NewMessage()
	msg.Reset()
	msg.clear()
	msg.WithRequest(&mockRequest{})
	msg.ResetSubs()

	msg.WithRequest(&mockRequest{})
	req := msg.Request()
	assert.NotNil(t, req)
	reqs := msg.Requests()
	assert.Len(t, reqs, 2)
	isb := msg.IsBatch()
	assert.True(t, isb)
	msgs = msg.Batch()
	assert.Len(t, msgs, 2)

	msg.ResetSubs()
	req = msg.NextReq()
	assert.NotNil(t, req)

	wg := &sync.WaitGroup{}
	msg.WithWaitGroup(wg)
	msg.Add()
	msg.Done()

	msg.MarkStart()
	time.Sleep(time.Millisecond * 50)
	msg.MarkRead()
	time.Sleep(time.Millisecond * 50)
	msg.MarkWrite()
	time.Sleep(time.Millisecond * 50)
	msg.MarkEnd()
	ts := msg.TotalDur()
	assert.NotZero(t, ts)
	ts = msg.RemoteDur()
	assert.NotZero(t, ts)

	msg.WithError(errors.New("some error"))
	err := msg.Err()
	assert.EqualError(t, err, "some error")
}
