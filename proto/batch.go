package proto

import (
	"sync"
	"time"

	"overlord/lib/bufio"
	"overlord/lib/log"
	"overlord/lib/prom"

	"github.com/pkg/errors"
)

const (
	defaultRespBufSize  = 1024
	defaultMsgBatchSize = 2
)

var msgBatchPool = &sync.Pool{
	New: func() interface{} {
		return &MsgBatch{
			msgs: make([]*Message, defaultMsgBatchSize),
			buf:  bufio.Get(defaultRespBufSize),
		}
	},
}

// NewMsgBatchSlice returns new slice of msgs
func NewMsgBatchSlice(n int) []*MsgBatch {
	wg := &sync.WaitGroup{}
	m := make([]*MsgBatch, n)
	for i := 0; i < n; i++ {
		m[i] = NewMsgBatch()
		m[i].wg = wg
	}
	return m
}

// NewMsgBatch will get msg from pool
func NewMsgBatch() *MsgBatch {
	return msgBatchPool.Get().(*MsgBatch)
}

// PutMsgBatch will release the batch object
func PutMsgBatch(b *MsgBatch) {
	b.Reset()
	b.wg = nil
	msgBatchPool.Put(b)
}

// MsgBatch is a single execute unit
type MsgBatch struct {
	buf  *bufio.Buffer
	msgs []*Message
	// message count
	count int

	// TODO: change waitgroup to channel
	wg *sync.WaitGroup
}

// AddMsg will add new message reference to the buffer
func (m *MsgBatch) AddMsg(msg *Message) {
	if len(m.msgs) <= m.count {
		m.msgs = append(m.msgs, msg)
	} else {
		m.msgs[m.count] = msg
	}
	m.count++
}

// Count returns the count of the batch size
func (m *MsgBatch) Count() int {
	return m.count
}

// Nth will get the given positon, if not , nil will be return
func (m *MsgBatch) Nth(i int) *Message {
	if i < m.count {
		return m.msgs[i]
	}
	return nil
}

// Buffer will send back buffer to executor
func (m *MsgBatch) Buffer() *bufio.Buffer {
	return m.buf
}

// Done will set the total batch to done and notify the handler to check it.
func (m *MsgBatch) Done() {
	m.wg.Done()
}

// Reset will reset all the field as initial value but msgs
func (m *MsgBatch) Reset() {
	m.count = 0
	m.buf.Reset()
}

// Add adds n for WaitGroup
func (m *MsgBatch) Add(n int) {
	m.wg.Add(n)
}

// Wait waits until all the message was done
func (m *MsgBatch) Wait() {
	m.wg.Wait()
}

// Msgs returns a slice of Msg
func (m *MsgBatch) Msgs() []*Message {
	return m.msgs[:m.count]
}

// BatchDone will set done and report prom HandleTime.
func (m *MsgBatch) BatchDone(cluster, addr string) {
	if prom.On() {
		for _, msg := range m.Msgs() {
			prom.HandleTime(cluster, addr, msg.Request().CmdString(), int64(msg.RemoteDur()/time.Microsecond))
		}
		m.Done()
	}
}

// BatchDoneWithError will set done with error and report prom ErrIncr.
func (m *MsgBatch) BatchDoneWithError(cluster, addr string, err error) {
	for _, msg := range m.Msgs() {
		msg.DoneWithError(err)
		if log.V(1) {
			log.Errorf("cluster(%s) Msg(%s) cluster process handle error:%+v", cluster, msg.Request().Key(), err)
		}
		if prom.On() {
			prom.ErrIncr(cluster, addr, msg.Request().CmdString(), errors.Cause(err).Error())
		}
	}
	m.Done()
}
