package proxy

import (
	"context"
	"io"
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
	// TODO: config and reduce to small
	defaultConcurrent = 2
	maxConcurrent     = 1024
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
	go h.handle()
}

func (h *Handler) handle() {
	var (
		messages = proto.GetMsgSlice(defaultConcurrent)
		mbatch   = proto.NewMsgBatchSlice(len(h.cluster.cc.Servers))
		msgs     []*proto.Message
		err      error
	)

	defer func() {
		for _, msg := range msgs {
			msg.Clear()
		}
		h.closeWithError(err)
	}()

	for {
		// 1. read until limit or error
		msgs, err = h.pc.Decode(messages)
		if err != nil {
			return
		}

		// 2. send to cluster
		h.cluster.DispatchBatch(mbatch, msgs)
		// 3. wait to done
		for _, mb := range mbatch {
			mb.Wait()
		}

		// 4. encode
		for _, msg := range msgs {
			err = h.pc.Encode(msg)
			if err != nil {
				return
			}
			msg.MarkEnd()
			msg.ReleaseSubs()
			prom.ProxyTime(h.cluster.cc.Name, msg.Request().CmdString(), int64(msg.TotalDur()/time.Microsecond))
		}

		// 4. release resource
		for _, msg := range msgs {
			msg.Reset()
		}
		for _, mb := range mbatch {
			mb.Reset()
		}

		// 5. reset MaxConcurrent
		messages = h.resetMaxConcurrent(messages, len(msgs))
	}
}

func (h *Handler) resetMaxConcurrent(msgs []*proto.Message, lastCount int) []*proto.Message {
	// TODO: change the msgs by lastCount trending
	lm := len(msgs)
	if lm < maxConcurrent && lm == lastCount {
		for i := 0; i < lm; i++ {
			msgs = append(msgs, proto.GetMsg())
		}
	}
	return msgs
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
			if err != io.EOF {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler close error:%+v", h.cluster.cc.Name, h.cluster.cc.ListenAddr, h.conn.RemoteAddr(), err)
			}
		}
	}
}
