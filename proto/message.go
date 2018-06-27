package proto

import (
	"fmt"
	"sync"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
)

const (
	defaultRespBufSize = 4096
)

var (
	defaultTime = time.Now()
)

var msgPool = &sync.Pool{
	New: func() interface{} {
		return NewMessage()
	},
}

// GetMsg get the msg from pool
func GetMsg() *Message {
	return msgPool.Get().(*Message)
}

// GetMsgSlice alloc a slice to the message
func GetMsgSlice(n int, caps ...int) []*Message {
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
		msgs[idx] = GetMsg()
	}
	return msgs
}

// PutMsg Release Msg
func PutMsg(msg *Message) {
	msgPool.Put(msg)
}

// Message read from client.
type Message struct {
	Type CacheType
	wg   *sync.WaitGroup

	buf *bufio.Buffer

	req      []Request
	subs     []*Message
	subResps [][]byte

	// Start Time, Write Time, ReadTime, EndTime
	st, wt, rt, et time.Time
	err            error
}

// NewMessage will create new message object
func NewMessage() *Message {
	return &Message{
		wg: &sync.WaitGroup{},
		// TODO: get with suitable length
		buf: bufio.Get(defaultRespBufSize),
	}
}

// Reset will clean the msg
func (m *Message) Reset() {
	m.buf.Reset()
	m.Type = CacheTypeUnknown
	m.req = nil
	m.subs = nil
	m.subResps = m.subResps[:0]
	m.st, m.wt, m.rt, m.et = defaultTime, defaultTime, defaultTime, defaultTime
	m.err = nil
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

// Add add wg.
func (m *Message) Add(n int) {
	if m.wg == nil {
		panic("message waitgroup nil")
	}
	m.wg.Add(n)
}

// Done done.
func (m *Message) Done() {
	if m.wg == nil {
		panic("message waitgroup nil")
	}
	m.wg.Done()
}

// DoneWithError done with error.
func (m *Message) DoneWithError(err error) {
	if m.wg == nil {
		panic("message waitgroup nil")
	}
	m.err = err
	m.wg.Done()
}

// Wait wait group.
func (m *Message) Wait() {
	if m.wg == nil {
		panic("message waitgroup nil")
	}
	m.wg.Wait()
}

// Buffer will return request buffer
func (m *Message) Buffer() *bufio.Buffer {
	return m.buf
}

// ReleaseSubs will return the Msg data to flush and reset
func (m *Message) ReleaseSubs() {
	for i := range m.subs {
		sub := m.subs[i]
		sub.Reset()
		PutMsg(sub)
	}
}

// WithRequest with proto request.
func (m *Message) WithRequest(req ...Request) {
	m.req = req
}

// Request returns proto Msg.
func (m *Message) Request() Request {
	if m.req != nil && len(m.req) > 0 {
		return m.req[0]
	}
	return nil
}

// IsBatch returns whether or not batch.
func (m *Message) IsBatch() bool {
	return len(m.req) > 1
}

// Batch returns sub Msg if is batch.
func (m *Message) Batch() []*Message {
	slen := len(m.req)
	if slen == 0 {
		return nil
	}
	subs := make([]*Message, slen)
	for i := 0; i < slen; i++ {
		subs[i] = GetMsg()
		subs[i].Type = m.Type
		subs[i].WithRequest(m.req[i])
		subs[i].wg = m.wg
	}
	m.subs = subs
	return subs
}

// Response return all response bytes.
func (m *Message) Response() [][]byte {
	if !m.IsBatch() {
		return [][]byte{m.req[0].Resp()}
	}
	slen := len(m.req)
	res := make([][]byte, slen)
	for i := 0; i < slen; i++ {
		res[i] = m.req[i].Resp()
	}
	return res
}

// Err returns error.
func (m *Message) Err() error {
	return m.err
}

// ErrMessage return err Msg.
func ErrMessage(err error) *Message {
	return &Message{err: err}
}
