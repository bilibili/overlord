package proxy

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/log"
	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/felixhao/overlord/lib/prom"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)

	messageChanBuffer = 1024 // TODO(felix): config???
)

// variables need to change
var (
	// TODO: config and optimus
	MaxConcurrent = 32
)

// Handler handle conn.
type Handler struct {
	c      *Config
	ctx    context.Context
	cancel context.CancelFunc

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
	var (
		messages  = proto.GetMsgSlice(MaxConcurrent)
		err       error
		completed bool
		count     int
	)

	defer func(err error) {
		h.closeWithError(err)
	}(err)

	for {
		// if completed, we need not to read but
		// just need to parse with buffered.
		if !completed {
			// 0. read it first
			err = h.pc.Read()
			if err != nil {
				return
			}
		}
		// 1. read until limit or error
		idx := 0
		for ; idx < MaxConcurrent; idx++ {
			completed, err = h.pc.Decode(messages[idx])
			if err != nil {
				return
			}

			messages[idx].MarkStart()
			// 不完整的buffer，则发送当前所有的msg到后台执行
			if !completed {
				messages[idx].Reset()
				break
			}
		}

		// set count
		count = idx + 1
		if !completed {
			count--
		}

		// 2. send to cluster
		for i := 0; i < count; i++ {
			msg := messages[i]
			msg.Add(1)
			h.dispatch(msg)
		}
		// 3. wait to done
		for i := 0; i < count; i++ {
			msg := messages[i]
			msg.Wait()
			// 3.5. write back client
			err = h.pc.Encode(msg)
			if err != nil {
				return
			}
			msg.MarkEnd()
			msg.ReleaseSubs()
			prom.ProxyTime(h.cluster.cc.Name, msg.Request().Cmd(), int64(msg.TotalDur()/time.Microsecond))
		}
		// 4. release resource
		for i := 0; i < count; i++ {
			messages[i].Reset()
		}
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
