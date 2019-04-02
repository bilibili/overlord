package proxy

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"overlord/pkg/log"
	libnet "overlord/pkg/net"
	"overlord/pkg/prom"
	"overlord/pkg/types"
	"overlord/proxy/proto"
	"overlord/proxy/proto/memcache"
	mcbin "overlord/proxy/proto/memcache/binary"
	"overlord/proxy/proto/redis"
	rclstr "overlord/proxy/proto/redis/cluster"
	"overlord/proxy/slowlog"

	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)
)

// variables need to change
var (
	// TODO: config and reduce to small
	concurrent    = 2
	maxConcurrent = 1024
)

// Handler handle conn.
type Handler struct {
	p  *Proxy
	cc *ClusterConfig

	slog       slowlog.Handler
	slowerThan time.Duration

	forwarder proto.Forwarder

	conn *libnet.Conn
	pc   proto.ProxyConn

	closed int32
	err    error
}

// NewHandler new a conn handler.
func NewHandler(p *Proxy, cc *ClusterConfig, conn net.Conn, forwarder proto.Forwarder) (h *Handler) {
	h = &Handler{
		p:         p,
		cc:        cc,
		forwarder: forwarder,
	}

	if cc.SlowlogSlowerThan != 0 {
		h.slowerThan = time.Duration(cc.SlowlogSlowerThan) * time.Microsecond
		h.slog = slowlog.Get(cc.Name)
	}

	h.conn = libnet.NewConn(conn, time.Second*time.Duration(h.p.c.Proxy.ReadTimeout), time.Second*time.Duration(h.p.c.Proxy.WriteTimeout))
	// cache type
	switch cc.CacheType {
	case types.CacheTypeMemcache:
		h.pc = memcache.NewProxyConn(h.conn)
	case types.CacheTypeMemcacheBinary:
		h.pc = mcbin.NewProxyConn(h.conn)
	case types.CacheTypeRedis:
		h.pc = redis.NewProxyConn(h.conn)
	case types.CacheTypeRedisCluster:
		h.pc = rclstr.NewProxyConn(h.conn, forwarder)
	default:
		panic(types.ErrNoSupportCacheType)
	}
	prom.ConnIncr(cc.Name)
	return
}

// Handle reads Msg from client connection and dispatchs Msg back to cache servers,
// then reads response from cache server and writes response into client connection.
func (h *Handler) Handle() {
	go h.handle()
}

func (h *Handler) handle() {
	var (
		messages []*proto.Message
		msgs     []*proto.Message
		wg       = &sync.WaitGroup{}
		err      error
	)
	messages = h.allocMaxConcurrent(wg, messages, len(msgs))
	for {
		// 1. read until limit or error
		if msgs, err = h.pc.Decode(messages); err != nil {
			h.deferHandle(messages, err)
			return
		}
		// 2. send to cluster
		h.forwarder.Forward(msgs)
		wg.Wait()
		// 3. encode
		for _, msg := range msgs {
			if err = h.pc.Encode(msg); err != nil {
				h.pc.Flush()
				h.deferHandle(messages, err)
				return
			}
			msg.MarkEnd()
			msg.ResetSubs()
			if prom.On {
				prom.ProxyTime(h.cc.Name, msg.Request().CmdString(), int64(msg.TotalDur()/time.Microsecond))
			}
		}
		if err = h.pc.Flush(); err != nil {
			h.deferHandle(messages, err)
			return
		}

		if h.slowerThan != 0 {
			for _, msg := range msgs {
				h.slog.Record(msg.Slowlog())
			}
		}

		// 4. check slowlog before release resource
		for _, msg := range msgs {
			msg.Reset()
		}
		// 5. alloc MaxConcurrent
		messages = h.allocMaxConcurrent(wg, messages, len(msgs))
	}
}

func (h *Handler) allocMaxConcurrent(wg *sync.WaitGroup, msgs []*proto.Message, lastCount int) []*proto.Message {
	var alloc int
	if lm := len(msgs); lm == 0 {
		alloc = concurrent
	} else if lm < maxConcurrent && lm == lastCount {
		alloc = lm * concurrent
	}
	if alloc > 0 {
		proto.PutMsgs(msgs)
		msgs = proto.GetMsgs(alloc) // TODO: change the msgs by lastCount trending
		for _, msg := range msgs {
			msg.WithWaitGroup(wg)
		}
	}
	return msgs
}

func (h *Handler) deferHandle(msgs []*proto.Message, err error) {
	proto.PutMsgs(msgs)
	h.closeWithError(err)
	return
}

func (h *Handler) closeWithError(err error) {
	if atomic.CompareAndSwapInt32(&h.closed, handlerOpening, handlerClosed) {
		h.err = err
		_ = h.conn.Close()
		atomic.AddInt32(&h.p.conns, -1) // NOTE: decr!!!
		if prom.On {
			prom.ConnDecr(h.cc.Name)
		}
		if log.V(2) && errors.Cause(err) != io.EOF {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler close error:%+v", h.cc.Name, h.cc.ListenAddr, h.conn.RemoteAddr(), err)
		}
	}
}
