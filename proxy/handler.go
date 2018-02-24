package proxy

import (
	"context"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/log"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)

	handlerChanBuffer = 10240 // TODO(felix): config???
	requestChanBuffer = 1024  // TODO(felix): config???
)

var (
	hdlNum = runtime.NumCPU()
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
	hdlCh   chan *proto.Request
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
	h.hdlCh = make(chan *proto.Request, handlerChanBuffer)
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
		for i := 0; i < hdlNum; i++ {
			go h.handle()
		}
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
			if log.V(3) {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler closed", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
			}
			return
		}
		select {
		case <-h.ctx.Done():
			if log.V(3) {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) context canceled", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
			}
			return
		default:
		}
		if h.c.Proxy.ReadTimeout > 0 {
			h.conn.SetReadDeadline(time.Now().Add(time.Duration(h.c.Proxy.ReadTimeout) * time.Millisecond))
		}
		if req, err = h.decoder.Decode(); err != nil {
			rerr := errors.Cause(err)
			if rerr == io.EOF {
				if log.V(2) {
					log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) close connection", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
				}
				return
			}
			if ne, ok := rerr.(net.Error); ok {
				if ne.Timeout() || !ne.Temporary() {
					log.Errorf("cluster(%s) addr(%s) remoteAddr(%s) decode error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
					return // NOTE: break when timeout or fatal error!!!
				}
				err = nil
				continue
			}
			req = &proto.Request{}
			req.Process()
			h.reqCh.PushBack(req)
			req.DoneWithError(err)
			if log.V(1) {
				log.Errorf("cluster(%s) addr(%s) remoteAddr(%s) decode error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
			}
			continue
		}
		req.Process()
		h.reqCh.PushBack(req)
		h.hdlCh <- req
	}
}

func (h *Handler) handle() {
	for {
		req, ok := <-h.hdlCh
		if !ok {
			if log.V(3) {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler chan not ok", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
			}
			return
		}
		h.dispatchRequest(req)
	}
}

func (h *Handler) dispatchRequest(req *proto.Request) {
	if !req.IsBatch() {
		h.handleRequest(req)
		return
	}
	subs, resp := req.Batch()
	if len(subs) == 0 {
		req.Done(resp) // FIXME(felix): error or done???
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) request(%s) batch return zero subs", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), req.Key())
		}
		return
	}
	subl := len(subs)
	for i := 0; i < subl; i++ {
		subs[i].Process()
		h.hdlCh <- &subs[i]
	}
	req.BatchWait()
	resp.Merge(subs)
	req.Done(resp)
}

func (h *Handler) handleRequest(req *proto.Request) {
	node, ok := h.cluster.Hash(req.Key())
	if !ok {
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) request(%s) hash node not ok", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), req.Key())
		}
	}
	hdl, err := h.cluster.Get(node)
	if err != nil {
		req.DoneWithError(errors.Wrap(err, "Proxy Handler handle request"))
		if log.V(2) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) request(%s) cluster get handler error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), req.Key(), err)
		}
		return
	}
	resp, err := hdl.Handle(req)
	h.cluster.Put(node, hdl, err)
	if err != nil {
		req.DoneWithError(errors.Wrap(err, "Proxy Handler handle request"))
		if log.V(1) {
			log.Errorf("cluster(%s) addr(%s) remoteAddr(%s) request(%s) handler handle error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), req.Key(), err)
		}
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
			rerr := errors.Cause(err)
			if ne, ok := rerr.(net.Error); ok && (ne.Timeout() || !ne.Temporary()) {
				if log.V(1) {
					log.Errorf("cluster(%s) addr(%s) remoteAddr(%s) handler writer error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
				}
				return
			}
			err = nil
			continue
		}
		select {
		case <-h.ctx.Done():
			if log.V(3) {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) context canceled", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
			}
			return
		default:
		}
		req, ok := h.reqCh.PopFront()
		if !ok {
			if log.V(3) {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) request chan pop not ok", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
			}
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
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler start close error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
		}
		h.err = err
		h.cancel()
		h.wg.Wait()
		close(h.hdlCh)
		h.reqCh.Close()
		h.conn.Close()
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler end close", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
		}
	}
}
