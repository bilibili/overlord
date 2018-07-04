package proto

import (
	"sync"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
)

const (
	defaultRespBufSize  = 4096
	defaultMsgBatchSize = 2
)

var (
	defaultTime = time.Now()
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

// Msgs returns all buffered msg
func (m *MsgBatch) Msgs() []*Message {
	return m.msgs[:m.count]
}

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

func (m *MsgBatch) Add(n int) {
	m.wg.Add(n)
}

func (m *MsgBatch) Wait() {
	m.wg.Wait()
}

func MergeBatchSize(mbs []*MsgBatch) (size int) {
	for _, mb := range mbs {
		size += mb.Count()
	}
	return
}

func Flatten(b []*MsgBatch) []*Message {
	var collection = make([]*Message, 0)
	for _, batch := range b {
		collection = append(collection, batch.Msgs()...)
	}
	return collection
}
