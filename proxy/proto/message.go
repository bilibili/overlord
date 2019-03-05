package proto

import (
	"fmt"
	"sync"
	"time"

	"overlord/pkg/types"
)

var (
	defaultTime = time.Now()
)

var msgPool = &sync.Pool{
	New: func() interface{} {
		return &Message{}
	},
}

// GetMsgs alloc a slice to the message
func GetMsgs(n int, caps ...int) []*Message {
	largs := len(caps)
	if largs > 1 {
		panic(fmt.Sprintf("optional argument except 1, but get %d", largs))
	}
	var msgs []*Message
	if largs == 0 {
		msgs = make([]*Message, n)
	} else {
		msgs = make([]*Message, n, caps[0])
	}
	for idx := range msgs {
		msgs[idx] = getMsg()
	}
	return msgs
}

// PutMsgs Release message.
func PutMsgs(msgs []*Message) {
	for _, m := range msgs {
		for _, sm := range m.subs {
			sm.clear()
			putMsg(sm)
		}
		for _, r := range m.req {
			r.Put()
		}
		m.clear()
		putMsg(m)
	}
}

// getMsg get the msg from pool
func getMsg() *Message {
	return msgPool.Get().(*Message)
}

// putMsg put the msg into pool
func putMsg(m *Message) {
	msgPool.Put(m)
}

// Message read from client.
type Message struct {
	Type types.CacheType

	req  []Request
	reqn int
	subs []*Message
	wg   *sync.WaitGroup

	// Start Time, Write Time, ReadTime, EndTime
	st, wt, rt, et time.Time
	err            error
}

// NewMessage will create new message object.
// this will be used be sub msg req.
func NewMessage() *Message {
	return getMsg()
}

// Reset will clean the msg
func (m *Message) Reset() {
	m.Type = types.CacheTypeUnknown
	m.reqn = 0
	m.st, m.wt, m.rt, m.et = defaultTime, defaultTime, defaultTime, defaultTime
	m.err = nil
}

// clear will clean the msg
func (m *Message) clear() {
	m.Reset()
	m.reqn = 0
	m.req = nil
	m.wg = nil
	m.subs = nil
}

// TotalDur will return the total duration of a command.
func (m *Message) TotalDur() time.Duration {
	return m.et.Sub(m.st)
}

// RemoteDur will return the remote execute time of remote mc node.
func (m *Message) RemoteDur() time.Duration {
	return m.rt.Sub(m.wt)
}

// MarkStart will set the start time of the command to now.
func (m *Message) MarkStart() {
	m.st = time.Now()
}

// MarkWrite will set the write time of the command to now.
func (m *Message) MarkWrite() {
	m.wt = time.Now()
}

// MarkRead will set the read time of the command to now.
func (m *Message) MarkRead() {
	m.rt = time.Now()
}

// MarkEnd will set the end time of the command to now.
func (m *Message) MarkEnd() {
	m.et = time.Now()
}

// ResetSubs will return the Msg data to flush and reset
func (m *Message) ResetSubs() {
	if !m.IsBatch() {
		return
	}
	for i := range m.subs[:m.reqn] {
		m.subs[i].Reset()
	}
	m.reqn = 0
}

// NextReq will iterator itself until nil.
func (m *Message) NextReq() (req Request) {
	if m.reqn < len(m.req) {
		req = m.req[m.reqn]
		m.reqn++
	}
	return
}

// WithRequest with proto request.
func (m *Message) WithRequest(req Request) {
	m.req = append(m.req, req)
	m.reqn++
}

func (m *Message) setRequest(req Request) {
	m.req = m.req[:0]
	m.reqn = 0
	m.WithRequest(req)
}

// Request returns proto Msg.
func (m *Message) Request() Request {
	if m.req != nil && len(m.req) > 0 {
		return m.req[0]
	}
	return nil
}

// Requests return all request.
func (m *Message) Requests() []Request {
	if m.reqn == 0 {
		return nil
	}
	return m.req[:m.reqn]
}

// IsBatch returns whether or not batch.
func (m *Message) IsBatch() bool {
	return m.reqn > 1
}

// Batch returns sub Msg if is batch.
func (m *Message) Batch() []*Message {
	slen := m.reqn
	if slen == 0 {
		return nil
	}
	var min = minInt(len(m.subs), slen)
	for i := 0; i < min; i++ {
		m.subs[i].Type = m.Type
		m.subs[i].setRequest(m.req[i])
	}
	delta := slen - len(m.subs)
	for i := 0; i < delta; i++ {
		msg := getMsg()
		msg.Type = m.Type
		msg.setRequest(m.req[min+i])
		msg.WithWaitGroup(m.wg)
		m.subs = append(m.subs, msg)
	}
	return m.subs[:slen]
}

// WithWaitGroup with wait group.
func (m *Message) WithWaitGroup(wg *sync.WaitGroup) {
	m.wg = wg
}

// Slowlog impl Slowloger
func (m *Message) Slowlog() *SlowlogEntry {
	return nil
}

// Add add wait group.
func (m *Message) Add() {
	if m.wg != nil {
		m.wg.Add(1)
	}
}

// Done mark handle message done.
func (m *Message) Done() {
	if m.wg != nil {
		m.wg.Done()
	}
}

// WithError with error.
func (m *Message) WithError(err error) {
	m.err = err
}

// Err returns error.
func (m *Message) Err() error {
	if m.err != nil {
		return m.err
	}
	if !m.IsBatch() {
		return nil
	}
	for _, s := range m.subs[:m.reqn] {
		if s.err != nil {
			return s.err
		}
	}
	return nil
}

// ErrMessage return err Msg.
func ErrMessage(err error) *Message {
	return &Message{err: err}
}

func minInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}
