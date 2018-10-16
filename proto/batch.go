package proto

import (
	"sync"
	"time"

	"overlord/lib/bufio"
	"overlord/lib/log"
	"overlord/lib/prom"

	"github.com/pkg/errors"
)

var msgBatchAllocPool = &sync.Pool{
	New: func() interface{} {
		return &MsgBatchAllocator{
			mbMap: make(map[string]*MsgBatch),
			wg:    &sync.WaitGroup{},
		}
	},
}

// GetMsgBatchAllocator get mb batch allocate.
func GetMsgBatchAllocator() *MsgBatchAllocator {
	return msgBatchAllocPool.Get().(*MsgBatchAllocator)
}

// PutMsgBatchAllocator all the resource back into pool
func PutMsgBatchAllocator(mba *MsgBatchAllocator) {
	for addr, mb := range mba.mbMap {
		mb.Reset()
		mb.msgs = nil
		mb.wg = nil
		delete(mba.mbMap, addr)
		msgBatchPool.Put(mb)
	}
	msgBatchAllocPool.Put(mba)
}

// MsgBatchAllocator will manage and allocate the msg batches
type MsgBatchAllocator struct {
	mbMap map[string]*MsgBatch
	wg    *sync.WaitGroup
}

// MsgBatchs will return the self mbMap for iterator
func (m *MsgBatchAllocator) MsgBatchs() map[string]*MsgBatch {
	return m.mbMap
}

func (m *MsgBatchAllocator) GetBatch(node string) *MsgBatch {
	return m.mbMap[node]
}

// AddMsg will add new msg and create a new batch if node not exists.
func (m *MsgBatchAllocator) AddMsg(node string, msg *Message) {
	if mb, ok := m.mbMap[node]; ok {
		mb.AddMsg(msg)
	} else {
		mb := NewMsgBatch()
		mb.wg = m.wg
		mb.AddMsg(msg)
		m.mbMap[node] = mb
	}
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (m *MsgBatchAllocator) Add(delta int) {
	m.wg.Add(delta)
}

// Wait waits until all the message was done
func (m *MsgBatchAllocator) Wait() {
	m.wg.Wait()
}

// Reset inner MsgBatchs
func (m *MsgBatchAllocator) Reset(node string) {
	mb := m.mbMap[node]
	mb.Reset()
	// for _, mb := range m.mbMap {
	// 	mb.Reset()
	// }
}

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

// NewMsgBatch will get msg from pool
func NewMsgBatch() *MsgBatch {
	return msgBatchPool.Get().(*MsgBatch)
}

// MsgBatch is a single execute unit
type MsgBatch struct {
	buf   *bufio.Buffer
	msgs  []*Message
	count int

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

// Nth will get the given position, if not, nil will be return
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

// Reset will reset all the field as initial value but msgs
func (m *MsgBatch) Reset() {
	m.count = 0
	m.buf.Reset()
}

// Msgs returns a slice of Msg
func (m *MsgBatch) Msgs() []*Message {
	return m.msgs[:m.count]
}

// Done will set done and report prom HandleTime.
func (m *MsgBatch) Done(cluster, addr string) {
	if prom.On {
		for _, msg := range m.Msgs() {
			prom.HandleTime(cluster, addr, msg.Request().CmdString(), int64(msg.RemoteDur()/time.Microsecond))
		}
	}
	if m.wg != nil {
		m.wg.Done()
	}
}

// DoneWithError will set done with error and report prom ErrIncr.
func (m *MsgBatch) DoneWithError(cluster, addr string, err error) {
	for _, msg := range m.Msgs() {
		msg.WithError(err)
		if log.V(1) {
			log.Errorf("cluster(%s) Msg(%s) cluster process handle error:%+v", cluster, msg.Request().Key(), err)
		}
		if prom.On {
			prom.ErrIncr(cluster, addr, msg.Request().CmdString(), errors.Cause(err).Error())
		}
	}
	if m.wg != nil {
		m.wg.Done()
	}
}
