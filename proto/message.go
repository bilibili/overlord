package proto

import (
	"sync"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
)

// Message read from client.
type Message struct {
	Type CacheType
	wg   *sync.WaitGroup

	buf *bufio.Buffer

	req  []Request
	subs []Message

	st  time.Time
	err error
}

// NewMessage will create new message object
func NewMessage() *Message {
	return &Message{
		wg:  &sync.WaitGroup{},
		buf: bufio.Get(0),
		st:  time.Now(),
	}
}

func (m *Message) Wg() *sync.WaitGroup {
	return m.wg
}

// Add add wg.
func (m *Message) Add(n int) {
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

// Buffer return req buffer.
func (m *Message) Buffer() *bufio.Buffer {
	return m.buf
}

// ResetBuffer will reset bufio's buffer,
// for bufio's grow
func (m *Message) ResetBuffer(buf *bufio.Buffer) {
	m.buf = buf
}

// Reset will return the Msg data to flush and reset
func (m *Message) Reset() {
	m.buf.Reset()
}

// ReleaseBuffer will return the Msg data to flush and reset
func (m *Message) ReleaseBuffer() {
	bufio.Put(m.buf)
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
	if !m.IsBatch() {
		return [][]byte{m.buf.Bytes()}
	}
	slen := len(m.subs)
	res := make([][]byte, slen)
	for i := 0; i < slen; i++ {
		res[i] = (&m.subs[i]).buf.Bytes()
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
