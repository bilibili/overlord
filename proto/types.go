package proto

import (
	errs "errors"
	"sync"
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
	Key() []byte
	IsBatch() bool
	Batch() ([]Request, *Response)
}

// Request read from client.
type Request struct {
	Type  CacheType
	proto protoRequest
	wg    *sync.WaitGroup

	Resp *Response
}

// Process means request processing.
func (r *Request) Process() {
	r.wg = &sync.WaitGroup{}
	r.wg.Add(1)
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

// Key returns proto request key.
func (r *Request) Key() []byte {
	return r.proto.Key()
}

// IsBatch returns whether or not batch.
func (r *Request) IsBatch() bool {
	return r.proto.IsBatch()
}

// Batch returns sub request if is batch.
func (r *Request) Batch() ([]Request, *Response) {
	subs, resp := r.proto.Batch()
	for i := range subs {
		subs[i].wg = r.wg
	}
	return subs, resp
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

// Err returns error.
func (r *Response) Err() error {
	return r.err
}

// Merge merges subs response into self.
func (r *Response) Merge(subs []Request) {
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
		panic("send on closed chan")
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
