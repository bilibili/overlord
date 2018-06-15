package proto

import (
	errs "errors"
	"net"
	"sync"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
)

// errors
var (
	ErrMoreData           = errs.New("need more data")
	ErrNoSupportCacheType = errs.New("unsupported cache type")
)

// CacheType memcache or redis
type CacheType string

// Cache type: memcache or redis.
const (
	CacheTypeUnknown  CacheType = "unknown"
	CacheTypeMemcache CacheType = "memcache"
	CacheTypeRedis    CacheType = "redis"
)

const (
	defaultBufSize = 16
)

type protoMsg interface {
	Cmd() string
	Key() []byte
	IsBatch() bool
	Batch() []Msg
	Merge() [][]byte
}

// Msg read from client.
type Msg struct {
	Type  CacheType
	proto protoMsg
	wg    *sync.WaitGroup

	// refData storage all the bytes reference to
	// proxy.Handler's reader
	refData []byte

	// buf is the owner data to using to store response
	buf  []byte
	wpos int

	Resp net.Buffers
	st   time.Time
	err  error
}

// NewMsg will create new message object
func NewMsg() *Msg {
	return &Msg{
		wg:  &sync.WaitGroup{},
		buf: bufio.Get(defaultBufSize),
	}
}

type errProto struct{}

func (e *errProto) Cmd() string {
	return "ErrCmd"
}
func (e *errProto) Key() []byte {
	return []byte("Err")
}
func (e *errProto) IsBatch() bool {
	return false
}
func (e *errProto) Batch() []Msg {
	return nil
}

func (e *errProto) Merge() [][]byte {
	return nil
}

// ErrMsg return err Msg.
func ErrMsg() *Msg {
	return &Msg{proto: &errProto{}}
}

// Add add wg.
func (r *Msg) Add() {
	r.wg.Add(1)
}

// Buf return req buffer.
func (r *Msg) Buf() []byte {
	return r.buf
}

// RefData will return the Msg data to flush and reset
func (r *Msg) RefData() []byte {
	return r.refData
}

// SetRefData sets the rdata to r.refData
func (r *Msg) SetRefData(rdata []byte) {
	r.refData = rdata
}

// Start means Msg start processing.
func (r *Msg) Start() {
	r.st = time.Now()
}

// Done done.
func (r *Msg) Done() {
	if r.wg == nil {
		panic("Msg waitgroup nil")
	}
	r.wg.Done()
}

// DoneWithError done with error.
func (r *Msg) DoneWithError(err error) {
	// TODO(felix): handle error
	if r.wg == nil {
		panic("Msg waitgroup nil")
	}
	r.wg.Done()
}

// Wait wait group.
func (r *Msg) Wait() {
	if r.wg == nil {
		panic("Msg waitgroup nil")
	}
	r.wg.Wait()
}

// WithProto with proto Msg.
func (r *Msg) WithProto(proto protoMsg) {
	r.proto = proto
}

// Proto returns proto Msg.
func (r *Msg) Proto() protoMsg {
	return r.proto
}

// Cmd returns proto Msg cmd.
func (r *Msg) Cmd() string {
	return r.proto.Cmd()
}

// Key returns proto Msg key.
func (r *Msg) Key() []byte {
	return r.proto.Key()
}

// IsBatch returns whether or not batch.
func (r *Msg) IsBatch() bool {
	return r.proto.IsBatch()
}

// Batch returns sub Msg if is batch.
func (r *Msg) Batch() []Msg {
	subs := r.proto.Batch()
	subl := len(subs)
	if subl == 0 {
		return nil
	}
	for i := 0; i < subl; i++ {
		subs[i].wg = r.wg
	}
	return subs
}

// Merge merge all sub response.
func (r *Msg) Merge() {
	if r.IsBatch() {
		r.Resp = net.Buffers(r.proto.Merge())
	} else {
		r.Resp = net.Buffers([][]byte{r.Buf()})
	}
}

// Since returns the time elapsed since t.
func (r *Msg) Since() time.Duration {
	return time.Since(r.st)
}

// Bytes return the real Buf data
func (r *Msg) Bytes() []byte {
	return r.buf[:r.wpos]
}

// Forward wpos size
func (r *Msg) Forward(size int) {
	r.wpos += size
}

// WithError with error.
func (r *Msg) WithError(err error) {
	r.err = err
}

// Err returns error.
func (r *Msg) Err() error {
	return r.err
}

// Encoder encode response.
type Encoder interface {
	Encode(*Msg) error
}

// Decoder decode bytes from client.
type Decoder interface {
	Decode(*Msg, []byte) error
}

// Handler handle Msg to backend cache server and read response.
type Handler interface {
	Handle(*Msg) error
}

// Pinger ping node connection.
type Pinger interface {
	Ping() error
	Close() error
}

// MsgChan is queue be used process Msg.
type MsgChan struct {
	lock sync.Mutex
	cond *sync.Cond

	data []*Msg
	buff []*Msg

	waits  int
	closed bool
}

const defaultMsgChanBuffer = 128

// NewMsgChan new Msg chan, defalut buffer 128.
func NewMsgChan() *MsgChan {
	return NewMsgChanBuffer(0)
}

// NewMsgChanBuffer new Msg chan with buffer.
func NewMsgChanBuffer(n int) *MsgChan {
	if n <= 0 {
		n = defaultMsgChanBuffer
	}
	ch := &MsgChan{
		buff: make([]*Msg, n),
	}
	ch.cond = sync.NewCond(&ch.lock)
	return ch
}

// PushBack push Msg back queue.
func (c *MsgChan) PushBack(r *Msg) int {
	c.lock.Lock()
	if c.closed {
		c.lock.Unlock()
		return 0
	}
	c.data = append(c.data, r)
	n := len(c.data)
	if c.waits != 0 {
		c.cond.Signal()
	}
	c.lock.Unlock()
	return n
}

// PopFront pop front from queue.
func (c *MsgChan) PopFront() (*Msg, bool) {
	c.lock.Lock()
	for len(c.data) == 0 {
		if c.closed {
			c.lock.Unlock()
			return nil, false
		}
		c.data = c.buff[:0]
		c.waits++
		c.cond.Wait()
		c.waits--
	}
	r := c.data[0]
	c.data[0], c.data = nil, c.data[1:]
	c.lock.Unlock()
	return r, true
}

// Buffered returns buffer.
func (c *MsgChan) Buffered() int {
	c.lock.Lock()
	n := len(c.data)
	c.lock.Unlock()
	return n
}

// Close close Msg chan.
func (c *MsgChan) Close() {
	c.lock.Lock()
	if !c.closed {
		c.closed = true
		c.cond.Broadcast()
	}
	c.lock.Unlock()
}

// Closed return closed.
func (c *MsgChan) Closed() bool {
	c.lock.Lock()
	b := c.closed
	c.lock.Unlock()
	return b
}
