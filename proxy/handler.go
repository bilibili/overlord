package proxy

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/log"
	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/felixhao/overlord/lib/prom"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)

	messageChanBuffer = 1024 // TODO(felix): config???
)

// Handler handle conn.
type Handler struct {
	c      *Config
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	conn *libnet.Conn
	pc   proto.ProxyConn

	cluster *Cluster
	msgCh   *proto.MsgChan

	closed int32
	// wg     sync.WaitGroup
	err error
}

// NewHandler new a conn handler.
func NewHandler(ctx context.Context, c *Config, conn net.Conn, cluster *Cluster) (h *Handler) {
	h = &Handler{
		c:       c,
		cluster: cluster,
	}
	h.ctx, h.cancel = context.WithCancel(ctx)
	h.conn = libnet.NewConn(conn, time.Second*time.Duration(h.c.Proxy.ReadTimeout), time.Second*time.Duration(h.c.Proxy.WriteTimeout))
	// cache type
	switch cluster.cc.CacheType {
	case proto.CacheTypeMemcache:
		h.pc = memcache.NewProxyConn(h.conn)
	case proto.CacheTypeRedis:
		// TODO(felix): support redis.
	default:
		panic(proto.ErrNoSupportCacheType)
	}
	h.msgCh = proto.NewMsgChanBuffer(messageChanBuffer)
	prom.ConnIncr(cluster.cc.Name)
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
		m   *proto.Message
		err error
	)
	defer func() {
		// EOF represent that client never write again,
		// but server can write back data after client closed the socket.
		if err != io.EOF {
			h.closeWithError(err)
		}
	}()
	for {
		if h.Closed() || h.msgCh.Closed() {
			if log.V(3) {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler closed", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
			}
			return
		}
		m, err = h.pc.Decode()
		if err != nil {
			if log.V(1) {
				log.Errorf("cluster(%s) addr(%s) remoteAddr(%s) decode error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
			}
			return
		}
		m.Add(1) // NOTE: must front msgCh.PushBack
		if h.msgCh.PushBack(m) == 0 {
			m.Done()
			return
		}
		h.dispatch(m)
	}
}

func (h *Handler) dispatch(m *proto.Message) {
	if !m.IsBatch() {
		h.cluster.Dispatch(m)
		return
	}
	// batch
	subs := m.Batch()
	if len(subs) == 0 {
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) Msg(%s) batch return zero subs", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), m.Request().Cmd()+string(m.Request().Key()))
		}
		m.Done()
		return
	}
	subl := len(subs)
	m.Add(subl)
	for i := 0; i < subl; i++ {
		h.cluster.Dispatch(&subs[i])
	}
	m.Done()
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
		m, ok := h.msgCh.PopFront()
		if !ok {
			if log.V(3) {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) Msg chan pop not ok", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr())
			}
			return
		}
		m.Wait()
		err = h.pc.Encode(m)
		m.SetEndTime(time.Now())
		prom.ProxyTime(h.cluster.cc.Name, m.Request().Cmd(), int64(m.TotalDur()/time.Microsecond))
		m.ReleaseBuffer()
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
		h.msgCh.Close()
		_ = h.conn.Close()
		prom.ConnDecr(h.cluster.cc.Name)
		if log.V(3) {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler close error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
		}
	}
}
