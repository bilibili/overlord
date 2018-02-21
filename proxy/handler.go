package proxy

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)

	requestChanBuffer = 128 // TODO(felix): config???
)

// Handler handle conn.
type Handler struct {
	c      *Config
	ctx    context.Context
	cancel context.CancelFunc

	once sync.Once

	conn    net.Conn
	cluster *Cluster
	decoder proto.Decoder
	encoder proto.Encoder
	reqCh   *proto.RequestChan

	closed int32
	wg     sync.WaitGroup
	err    error
}

// NewHandler new a conn handler.
func NewHandler(ctx context.Context, c *Config, conn net.Conn, cluster *Cluster) (h *Handler) {
	h = &Handler{c: c}
	h.conn = conn
	h.cluster = cluster
	h.ctx, h.cancel = context.WithCancel(ctx)
	// cache type
	switch cluster.cc.CacheType {
	case proto.CacheTypeMemcache:
		h.decoder = memcache.NewDecoder(conn)
		h.encoder = memcache.NewEncoder(conn)
	case proto.CacheTypeRedis:
		// TODO(felix): support redis.
	default:
		panic(proto.ErrNoSupportCacheType)
	}
	h.reqCh = proto.NewRequestChanBuffer(requestChanBuffer)
	return
}

// Handle reads request from client connection and dispatchs request back to cache servers,
// then reads response from cache server and writes response into client connection.
func (h *Handler) Handle() {
	h.once.Do(func() {
		h.wg.Add(1)
		go h.handleWriter()
		h.wg.Add(1)
		go h.handleReader()
	})
}

func (h *Handler) handleReader() {
	var (
		req *proto.Request
		err error
	)
	defer func() {
		h.wg.Done()
		h.closeWithError(err)
	}()
	for {
		if h.Closed() || h.reqCh.Closed() {
			return
		}
		select {
		case <-h.ctx.Done():
			return
		default:
		}
		if h.c.Proxy.ReadTimeout > 0 {
			h.conn.SetReadDeadline(time.Now().Add(time.Duration(h.c.Proxy.ReadTimeout) * time.Millisecond))
		}
		if req, err = h.decoder.Decode(); err != nil {
			if ne, ok := err.(net.Error); ok {
				if ne.Timeout() || !ne.Temporary() {
					return // NOTE: break when timeout or fatal error!!!
				}
				err = nil
				continue
			}
			req = &proto.Request{}
			req.Process()
			h.reqCh.PushBack(req)
			req.DoneWithError(err)
			continue
		}
		req.Process()
		h.reqCh.PushBack(req)
		go h.dispatchRequest(req)
	}
}

func (h *Handler) dispatchRequest(req *proto.Request) {
	if !req.IsBatch() {
		h.handleRequest(req, nil)
		return
	}
	subs, resp := req.Batch()
	if len(subs) == 0 {
		req.Done(resp) // FIXME(felix): error or done???
		return
	}
	subl := len(subs)
	dones := make(chan struct{}, subl)
	for i := 0; i < subl; i++ {
		subs[i].Process()
		go h.handleRequest(&subs[i], dones)
	}
	for i := 0; i < subl; i++ {
		<-dones
	}
	resp.Merge(subs)
	req.Done(resp)
}

func (h *Handler) handleRequest(req *proto.Request, done chan<- struct{}) {
	if done != nil {
		defer func() {
			done <- struct{}{}
		}()
	}
	node, _ := h.cluster.Hash(req.Key())
	hdl, err := h.cluster.Get(node)
	if err != nil {
		req.DoneWithError(errors.Wrap(err, "Proxy Handler handle request"))
		return
	}
	resp, err := hdl.Handle(req)
	h.cluster.Put(node, hdl, err)
	if err != nil {
		req.DoneWithError(errors.Wrap(err, "Proxy Handler handle request"))
		return
	}
	req.Done(resp)
}

func (h *Handler) handleWriter() {
	var err error
	defer func() {
		h.wg.Done()
		h.closeWithError(err)
	}()
	for {
		// NOTE: no check handler closed, ensure that reqCh pop finished.
		if err != nil {
			if ne, ok := err.(net.Error); ok && (ne.Timeout() || ne.Temporary()) {
				err = nil
				continue
			}
			return
		}
		select {
		case <-h.ctx.Done():
			return
		default:
		}
		req, ok := h.reqCh.PopFront()
		if !ok {
			return
		}
		req.Wait()
		if h.c.Proxy.WriteTimeout > 0 {
			h.conn.SetWriteDeadline(time.Now().Add(time.Duration(h.c.Proxy.WriteTimeout) * time.Millisecond))
		}
		err = h.encoder.Encode(req.Resp)
	}
}

// Closed return handler whether or not closed.
func (h *Handler) Closed() bool {
	return atomic.LoadInt32(&h.closed) == handlerClosed
}

func (h *Handler) closeWithError(err error) {
	if atomic.CompareAndSwapInt32(&h.closed, handlerOpening, handlerClosed) {
		h.err = err
		h.cancel()
		h.wg.Wait()
		h.reqCh.Close()
		h.conn.Close()
	}
}
