package proxy

import (
	"context"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
	"github.com/felixhao/overlord/lib/log"
	"github.com/felixhao/overlord/lib/net2"
	"github.com/felixhao/overlord/lib/stat"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)

	MsgChanBuffer = 1024 // TODO(felix): config???
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

	conn net.Conn
	br   *bufio.Reader

	cluster *Cluster
	decoder proto.Decoder
	encoder proto.Encoder
	reqCh   *proto.MsgChan

	closed int32
	wg     sync.WaitGroup
	err    error
}

// NewHandler new a conn handler.
func NewHandler(ctx context.Context, c *Config, conn net.Conn, cluster *Cluster) (h *Handler) {
	h = &Handler{c: c}
	h.conn = net2.NewConn(conn, time.Duration(h.c.Proxy.ReadTimeout), time.Duration(h.c.Proxy.WriteTimeout))
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
	h.br = bufio.NewReader(h.conn)
	h.reqCh = proto.NewMsgChanBuffer(MsgChanBuffer)
	stat.ConnIncr(cluster.cc.Name)
	return
}

// Handle reads Msg from client connection and dispatchs Msg back to cache servers,
// then reads response from cache server and writes response into client connection.
func (h *Handler) Handle() {
	h.once.Do(func() {
		go h.handleWriter()
		go h.handleReader()
	})
}

func (h *Handler) handleReader() {
	var (
		err error
	)
	// defer func() {
	// 	h.closeWithError(err)
	// }()
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

		err = h.br.ReadAll()
		if err == io.EOF {
			if len(h.br.Bytes()) == 0 {
				return
			}
		} else if err != nil {
			return
		}
		// //rerr := errors.Cause(err)
		// if ne, ok := err.(net.Error); ok {
		// 	if ne.Temporary() {
		// 		req = proto.ErrMsg()
		// 		req.Add()
		// 		if h.reqCh.PushBack(req) == 0 {
		// 			return
		// 		}
		// 		req.DoneWithError(err)
		// 		if log.V(1) {
		// 			log.Errorf("cluster(%s) addr(%s) remoteAddr(%s) decode error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
		// 		}
		// 		err = nil
		// 		continue
		// 	}
		// }
		// if log.V(1) {
		// 	log.Errorf("cluster(%s) addr(%s) remoteAddr(%s) close connection error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
		// }

		// TODO:get req from pool.

		err = h.tryingHandle()
		if err == proto.ErrMoreData {
			err = nil
			continue
		} else if err != nil {
			// TODO(wayslog): with PROXY_ERROR response
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) decode error with error %s", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
			break
		}
	}
}

// tryingHandle until reach error: ErrMoreData
func (h *Handler) tryingHandle() (err error) {
	var reqs = []*proto.Msg{}
	for {
		// TODO: get NewMsg from object pool
		req := proto.NewMsg()
		bs := h.br.Bytes()
		err = h.decoder.Decode(req, bs)
		if err != nil {
			break
		}
		h.br.Advance(len(req.RefData()))
		h.dispatchMsg(req)
		reqs = append(reqs, req)
	}

	for i := range reqs {
		reqs[i].Wait()
		reqs[i].Merge()
		if h.reqCh.PushBack(reqs[i]) == 0 {
			return
		}
	}
	return
}

func (h *Handler) dispatchMsg(req *proto.Msg) {
	req.Start()
	if !req.IsBatch() {
		req.Add()
		h.cluster.Dispatch(req)
		return
	}

	subs := req.Batch()
	if len(subs) == 0 {
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) Msg(%s) batch return zero subs", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), req.Key())
		}
		return
	}
	subl := len(subs)
	for i := 0; i < subl; i++ {
		subs[i].Add()
		h.cluster.Dispatch(&subs[i])
	}
}

func (h *Handler) handleWriter() {
	var err error
	defer func() {
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
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) Msg chan pop not ok", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
			}
			return
		}

		if h.c.Proxy.WriteTimeout > 0 {
			h.conn.SetWriteDeadline(time.Now().Add(time.Duration(h.c.Proxy.WriteTimeout) * time.Millisecond))
		}
		err = h.encoder.Encode(req)

		stat.ProxyTime(h.cluster.cc.Name, req.Cmd(), int64(req.Since()/time.Microsecond))
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
		h.reqCh.Close()
		h.conn.Close()
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler end close", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
		}
		stat.ConnDecr(h.cluster.cc.Name)
	}
}
