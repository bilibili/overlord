package proto

import (
	"sync"
	"time"

	"overlord/lib/bufio"
	"overlord/lib/log"
	"overlord/lib/prom"

	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	defaultRespBufSize  = 1024
	defaultMsgBatchSize = 2
)

const (
	msgBatchStateNotReady = uint32(0)
	msgBatchStateDone     = uint32(1)
)

// errors
var (
	ErrTimeout         = errors.New("timeout reached")
	ErrMaxRetryReached = errors.New("too many times weak up")
)

var msgBatchPool = &sync.Pool{
	New: func() interface{} {
		return &MsgBatch{
			msgs:  make([]*Message, defaultMsgBatchSize),
			buf:   bufio.Get(defaultRespBufSize),
			state: msgBatchStateNotReady,
		}
	},
}

// NewMsgBatchSlice returns new slice of msgs
func NewMsgBatchSlice(n int) []*MsgBatch {
	m := make([]*MsgBatch, n)
	for i := 0; i < n; i++ {
		m[i] = NewMsgBatch()
	}
	return m
}

// NewMsgBatch will get msg from pool
func NewMsgBatch() *MsgBatch {
	return msgBatchPool.Get().(*MsgBatch)
}

// MsgBatch is a single execute unit
type MsgBatch struct {
	buf   *bufio.Buffer
	msgs  []*Message
	count int

	dc    chan struct{}
	state uint32
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
	atomic.StoreUint32(&m.state, msgBatchStateDone)
	select {
	case m.dc <- struct{}{}:
	default:
	}
}

// Reset will reset all the field as initial value but msgs
func (m *MsgBatch) Reset() {
	atomic.StoreUint32(&m.state, msgBatchStateNotReady)
	m.count = 0
	m.buf.Reset()
}

// Msgs returns a slice of Msg
func (m *MsgBatch) Msgs() []*Message {
	return m.msgs[:m.count]
}

// IsDone check if MsgBatch is done.
func (m *MsgBatch) IsDone() bool {
	return atomic.LoadUint32(&m.state) == msgBatchStateDone
}

// BatchDone will set done and report prom HandleTime.
func (m *MsgBatch) BatchDone(cluster, addr string) {
	if prom.On {
		for _, msg := range m.Msgs() {
			prom.HandleTime(cluster, addr, msg.Request().CmdString(), int64(msg.RemoteDur()/time.Microsecond))
		}
	}
	m.Done()
}

// BatchDoneWithError will set done with error and report prom ErrIncr.
func (m *MsgBatch) BatchDoneWithError(cluster, addr string, err error) {
	for _, msg := range m.Msgs() {
		msg.DoneWithError(err)
		if log.V(1) {
			log.Errorf("cluster(%s) Msg(%s) cluster process handle error:%+v", cluster, msg.Request().Key(), err)
		}
		if prom.On {
			prom.ErrIncr(cluster, addr, msg.Request().CmdString(), errors.Cause(err).Error())
		}
	}
	m.Done()
}

// DropMsgBatch put MsgBatch into recycle using pool.
func DropMsgBatch(m *MsgBatch) {
	m.buf.Reset()
	m.msgs = m.msgs[:0]
	m.count = 0
	m.dc = nil
	msgBatchPool.Put(m)
	m = nil
}

// MsgBatchAllocator will manage and allocate the msg batches
type MsgBatchAllocator struct {
	mbMap   map[string]*MsgBatch
	dc      chan struct{}
	timeout time.Duration
	// TODO: impl quick search for iterator
	quickSearch map[string]struct{}
}

// NewMsgBatchAllocator create mb batch from servers and dc
func NewMsgBatchAllocator(dc chan struct{}, timeout time.Duration) *MsgBatchAllocator {
	mba := &MsgBatchAllocator{
		mbMap: make(map[string]*MsgBatch),
		dc:    dc, quickSearch: make(map[string]struct{}),
		timeout: timeout,
	}
	return mba
}

// AddMsg will add new msg and create a new batch if node not exists.
func (m *MsgBatchAllocator) AddMsg(node string, msg *Message) {
	if mb, ok := m.mbMap[node]; ok {
		mb.AddMsg(msg)
	} else {
		mb := NewMsgBatch()
		mb.AddMsg(msg)
		mb.dc = m.dc
		m.mbMap[node] = mb
	}
}

// MsgBatchs will return the self mbMap for iterator
func (m *MsgBatchAllocator) MsgBatchs() map[string]*MsgBatch {
	return m.mbMap
}

// Wait until timeout reached or all msgbatch is done.
// if timeout, ErrTimeout will be return.
func (m *MsgBatchAllocator) Wait() error {
	mbLen := len(m.mbMap)
	to := time.After(m.timeout)
	for i := 0; i < mbLen; i++ {
		select {
		case <-m.dc:
			if m.checkAllDone() {
				return nil
			}
		case <-to:
			return ErrTimeout
		}
	}
	return ErrMaxRetryReached
}

// Reset inner MsgBatchs
func (m *MsgBatchAllocator) Reset() {
	for _, mb := range m.MsgBatchs() {
		mb.Reset()
	}
}

// Put all the resource back into pool
func (m *MsgBatchAllocator) Put() {
	m.Reset()
	for _, mb := range m.MsgBatchs() {
		msgBatchPool.Put(mb)
	}
}

func (m *MsgBatchAllocator) checkAllDone() bool {
	for _, mb := range m.MsgBatchs() {
		if !mb.IsDone() {
			return false
		}
	}
	return true
}
