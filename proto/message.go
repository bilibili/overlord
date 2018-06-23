package proto

import (
	"sync"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
)

const (
	defaultReqBufSize  = 512
	defaultRespBufSize = 4096
)

// Message read from client.
type Message struct {
	Type CacheType
	wg   *sync.WaitGroup

	reqBuf  *bufio.Buffer
	respBuf *bufio.Buffer

	req      []Request
	subs     []Message
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
		reqBuf:  bufio.Get(defaultReqBufSize),
		respBuf: bufio.Get(defaultRespBufSize),

		st: time.Now(),
	}
}

// TotalDur will return the total duration of a command.
func (m *Message) TotalDur() time.Duration {
	return m.et.Sub(m.st)
}

// RemoteDur will return the remote execute time of remote mc node.
func (m *Message) RemoteDur() time.Duration {
	return m.rt.Sub(m.wt)
}

// SetWriteTime will set the remote duration of the command.
func (m *Message) SetWriteTime(t time.Time) {
	m.wt = t
}

// SetReadTime will set the remote duration of the command.
func (m *Message) SetReadTime(t time.Time) {
	m.rt = t
}

// SetEndTime will set the remote duration of the command.
func (m *Message) SetEndTime(t time.Time) {
	m.et = t
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

// ReqBuffer return req buffer.
func (m *Message) ReqBuffer() *bufio.Buffer {
	return m.reqBuf
}

// RespBuffer will return request buffer
func (m *Message) RespBuffer() *bufio.Buffer {
	return m.respBuf
}

// ReleaseBuffer will return the Msg data to flush and reset
func (m *Message) ReleaseBuffer() {
	bufio.Put(m.reqBuf)
	bufio.Put(m.respBuf)
	for i := 0; i < len(m.subs); i++ {
		(&m.subs[i]).ReleaseBuffer()
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
func (m *Message) Batch() []Message {
	slen := len(m.req)
	if slen == 0 {
		return nil
	}
	subs := make([]Message, slen)
	for i := 0; i < slen; i++ {
		subs[i] = *NewMessage()
		subs[i].Type = m.Type
		subs[i].WithRequest(m.req[i])
		subs[i].wg = m.wg
	}
	m.subs = subs
	return subs
}

// Response return all response bytes.
func (m *Message) Response() [][]byte {
	// TODO(wayslog): set Read/Write timer after merge
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
