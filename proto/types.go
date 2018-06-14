package proto

import (
	errs "errors"
	"net"
	"sync"
	"time"
)

// errors
var (
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
	Resp  net.Buffers
	st    time.Time
	err   error
}

// NewMsg will create new message object
func NewMsg() *Msg {
	return &Msg{
		wg: &sync.WaitGroup{},
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

// Process means Msg processing.
func (r *Msg) Process() {
	r.wg.Add(1)
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
	r.Resp = net.Buffers(r.proto.Merge())
}

// Since returns the time elapsed since t.
func (r *Msg) Since() time.Duration {
	return time.Since(r.st)
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
	Decode(*Msg) error
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
