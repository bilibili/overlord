package proto

import (
	errs "errors"
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

type protoRequest interface {
	Cmd() string
	Key() []byte
	IsBatch() bool
	Batch() ([]Request, *Response)
}

// Request read from client.
type Request struct {
	Type  CacheType
	proto protoRequest
	wg    *sync.WaitGroup
	bWg   *sync.WaitGroup

	Resp *Response
	st   time.Time
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
func (e *errProto) Batch() ([]Request, *Response) {
	return nil, nil
}

// ErrRequest return err request.
func ErrRequest() *Request {
	return &Request{proto: &errProto{}}
}

// Process means request processing.
func (r *Request) Process() {
	if r.wg == nil {
		r.wg = &sync.WaitGroup{}
	}
	r.wg.Add(1)
	r.st = time.Now()
}

// Done done.
func (r *Request) Done(resp *Response) {
	if r.wg == nil {
		panic("request waitgroup nil")
	}
	r.Resp = resp
	r.wg.Done()
}

// DoneWithError done with error.
func (r *Request) DoneWithError(err error) {
	// TODO(felix): handle error
	if r.wg == nil {
		panic("request waitgroup nil")
	}
	r.Resp = &Response{Type: r.Type, err: err}
	r.wg.Done()
}

// Wait wait group.
func (r *Request) Wait() {
	if r.wg == nil {
		panic("request waitgroup nil")
	}
	r.wg.Wait()
}

// WithProto with proto request.
func (r *Request) WithProto(proto protoRequest) {
	r.proto = proto
}

// Proto returns proto request.
func (r *Request) Proto() protoRequest {
	return r.proto
}

// Cmd returns proto request cmd.
func (r *Request) Cmd() string {
	return r.proto.Cmd()
}

// Key returns proto request key.
func (r *Request) Key() []byte {
	return r.proto.Key()
}

// IsBatch returns whether or not batch.
func (r *Request) IsBatch() bool {
	return r.proto.IsBatch()
}

// Batch return sub requests if is batch.
func (r *Request) Batch() ([]Request, *Response) {
	subs, resp := r.proto.Batch()
	subl := len(subs)
	if subl == 0 {
		return nil, resp
	}
	r.bWg = &sync.WaitGroup{}
	for i := 0; i < subl; i++ {
		subs[i].wg = r.bWg
	}
	return subs, resp
}

// BatchWait blocks until the all sub request done.
func (r *Request) BatchWait() {
	if r.bWg != nil {
		r.bWg.Wait()
	}
}

// Since returns the time elapsed since t.
func (r *Request) Since() time.Duration {
	return time.Since(r.st)
}

type protoResponse interface {
	Merge([]Request)
}

// Response read from cache server.
type Response struct {
	Type  CacheType
	proto protoResponse
	err   error
}

// WithProto with proto response.
func (r *Response) WithProto(proto protoResponse) {
	r.proto = proto
}

// Proto returns proto response.
func (r *Response) Proto() protoResponse {
	return r.proto
}

// WithError with error.
func (r *Response) WithError(err error) {
	r.err = err
}

// Err returns error.
func (r *Response) Err() error {
	return r.err
}

// Merge merges subs response into self.
func (r *Response) Merge(subs []Request) {
	if r.err != nil || r.proto == nil {
		return
	}
	r.proto.Merge(subs)
}

// Encoder encode response.
type Encoder interface {
	Encode(*Response) error
}

// Decoder decode bytes from client.
type Decoder interface {
	Decode() (*Request, error)
}

// Handler handle request to backend cache server and read response.
type Handler interface {
	Handle(*Request) (*Response, error)
}

// Pinger ping node connection.
type Pinger interface {
	Ping() error
	Close() error
}

// RequestChan is queue be used process request.
type RequestChan struct {
	lock sync.Mutex
	cond *sync.Cond

	data []*Request
	buff []*Request

	waits  int
	closed bool
}

const defaultRequestChanBuffer = 128

// NewRequestChan new request chan, defalut buffer 128.
func NewRequestChan() *RequestChan {
	return NewRequestChanBuffer(0)
}

// NewRequestChanBuffer new request chan with buffer.
func NewRequestChanBuffer(n int) *RequestChan {
	if n <= 0 {
		n = defaultRequestChanBuffer
	}
	ch := &RequestChan{
		buff: make([]*Request, n),
	}
	ch.cond = sync.NewCond(&ch.lock)
	return ch
}

// PushBack push request back queue.
func (c *RequestChan) PushBack(r *Request) int {
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
func (c *RequestChan) PopFront() (*Request, bool) {
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
func (c *RequestChan) Buffered() int {
	c.lock.Lock()
	n := len(c.data)
	c.lock.Unlock()
	return n
}

// Close close request chan.
func (c *RequestChan) Close() {
	c.lock.Lock()
	if !c.closed {
		c.closed = true
		c.cond.Broadcast()
	}
	c.lock.Unlock()
}

// Closed return closed.
func (c *RequestChan) Closed() bool {
	c.lock.Lock()
	b := c.closed
	c.lock.Unlock()
	return b
}
