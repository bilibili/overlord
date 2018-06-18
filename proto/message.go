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

// Add add wg.
func (m *Message) Add() {
	m.wg.Add(1)
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

// ResetBuffer will return the Msg data to flush and reset
func (m *Message) ResetBuffer() {
	m.buf.Reset()
}

// ReleaseBuffer will return the Msg data to flush and reset
func (m *Message) ReleaseBuffer() {
	bufio.Put(m.buf)
}

// // SetRefData sets the rdata to r.refData
// func (r *Msg) SetRefData(rdata []byte) {
// 	r.refData = rdata
// }

// WithRequest with proto request.
func (m *Message) WithRequest(req ...Request) {
	m.req = req
}

// Request returns proto Msg.
func (m *Message) Request() Request {
	if len(m.req) > 0 {
		return m.req[0]
	}
	return nil
}

// // Cmd returns proto Msg cmd.
// func (m *Message) Cmd() string {
// 	return m.req[0].Cmd()
// }

// // Key returns proto Msg key.
// func (m *Message) Key() []byte {
// 	return m.req[0].Key()
// }

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

// // Since returns the time elapsed since t.
// func (r *Msg) Since() time.Duration {
// 	return time.Since(r.st)
// }

// // Bytes return the real Buf data
// func (r *Msg) Bytes() []byte {
// 	return r.buf[:r.wpos]
// }

// // Forward wpos size
// func (r *Msg) Forward(size int) {
// 	r.wpos += size
// }

// // WithError with error.
// func (r *Msg) WithError(err error) {
// 	r.err = err
// }

// Err returns error.
func (m *Message) Err() error {
	return m.err
}

// type errProto struct{}

// func (e *errProto) Cmd() string {
// 	return "ErrCmd"
// }
// func (e *errProto) Key() []byte {
// 	return []byte("Err")
// }

// func (e *errProto) IsBatch() bool {
// 	return false
// }
// func (e *errProto) Batch() []Msg {
// 	return nil
// }

// func (e *errProto) Merge() [][]byte {
// 	return nil
// }

// ErrMessage return err Msg.
func ErrMessage(err error) *Message {
	return &Message{err: err}
}
