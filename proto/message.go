package proto

import (
	"fmt"
	"sync"
	"time"
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

	req      []Request
	reqn     int
	subs     []*Message
	subResps [][]byte

	// Start Time, Write Time, ReadTime, EndTime
	st, wt, rt, et time.Time
	err            error
}

// NewMessage will create new message object.
// this will be used be sub msg req.
func NewMessage() *Message {
	return &Message{}
}

// Reset will clean the msg
func (m *Message) Reset() {
	m.Type = CacheTypeUnknown
	m.reqn = 0
	//	m.req = m.req[:0]
	//	m.subs = m.subs[:0]
	m.subResps = m.subResps[:0]
	m.st, m.wt, m.rt, m.et = defaultTime, defaultTime, defaultTime, defaultTime
	m.err = nil
}

// clear will clean the msg
func (m *Message) Clear() {
	m.Type = CacheTypeUnknown
	m.reqn = 0
	//	m.req = m.req[:0]
	//	m.subs = m.subs[:0]
	m.subResps = m.subResps[:0]
	m.st, m.wt, m.rt, m.et = defaultTime, defaultTime, defaultTime, defaultTime
	m.err = nil
	for _, r := range m.req {
		r.Put()
	}
	m.req = nil
	for _, s := range m.subs {
		s.Clear()
		PutMsg(s)
	}
	m.subs = nil
	PutMsg(m)
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

// DoneWithError done with error.
func (m *Message) DoneWithError(err error) {
	m.err = err
}

// ReleaseSubs will return the Msg data to flush and reset
func (m *Message) ReleaseSubs() {
	for i := range m.subs {
		sub := m.subs[i]
		sub.Reset()
	}
	m.reqn = 0
}
func (m *Message) GetRequest() (req Request) {
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
	var min int
	if len(m.subs) > slen {
		min = slen
	} else {
		min = len(m.subs)
	}
	for i := 0; i < min; i++ {
		m.subs[i].Type = m.Type
		m.subs[i].setRequest(m.req[i])
		// subs[i].wg = m.wg
	}
	delta := slen - len(m.subs)
	for i := 0; i < delta; i++ {
		msg := GetMsg()
		msg.Type = m.Type
		msg.setRequest(m.req[min+i])
		m.subs = append(m.subs, msg)
	}
	return m.subs[:slen]
}

// BatchReq returns the m.req field
func (m *Message) BatchReq() []Request {
	return m.req
}

// Response return all response bytes.
func (m *Message) Response() [][]byte {
	if !m.IsBatch() {
		m.subResps = append(m.subResps, m.req[0].Resp())
	} else {
		slen := len(m.req)
		for i := 0; i < slen; i++ {
			m.subResps = append(m.subResps, m.req[i].Resp())
		}
	}
	return m.subResps
}

// Err returns error.
func (m *Message) Err() error {
	return m.err
}

// ErrMessage return err Msg.
func ErrMessage(err error) *Message {
	return &Message{err: err}
}
