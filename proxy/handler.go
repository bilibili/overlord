package proxy

import (
	"io"
	"net"
	"sync/atomic"
	"time"

	"overlord/lib/log"
	libnet "overlord/lib/net"
	"overlord/lib/prom"
	"overlord/proto"
	"overlord/proto/memcache"
	mcbin "overlord/proto/memcache/binary"
	"overlord/proto/redis"
	rclstr "overlord/proto/redis/cluster"
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

	executor proto.Executor

	conn *libnet.Conn
	pc   proto.ProxyConn

	closed int32
	err    error
}

// NewHandler new a conn handler.
func NewHandler(p *Proxy, cc *ClusterConfig, conn net.Conn, executor proto.Executor) (h *Handler) {
	h = &Handler{
		p:        p,
		cc:       cc,
		executor: executor,
	}
	h.conn = libnet.NewConn(conn, time.Second*time.Duration(h.p.c.Proxy.ReadTimeout), time.Second*time.Duration(h.p.c.Proxy.WriteTimeout))
	// cache type
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		h.pc = memcache.NewProxyConn(h.conn)
	case proto.CacheTypeMemcacheBinary:
		h.pc = mcbin.NewProxyConn(h.conn)
	case proto.CacheTypeRedis:
		h.pc = redis.NewProxyConn(h.conn)
	case proto.CacheTypeRedisCluster:
		h.pc = rclstr.NewProxyConn(h.conn, executor)
	default:
		panic(proto.ErrNoSupportCacheType)
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
		messages = proto.GetMsgs(concurrent)
		mba      = proto.GetMsgBatchAllocator()
		msgs     []*proto.Message
		err      error
		nodem    = make(map[string]struct{})
	)
	for {
		// 1. read until limit or error
		if msgs, err = h.pc.Decode(messages); err != nil {
			h.deferHandle(messages, mba, err)
			return
		}
		// 2. send to cluster
		h.executor.Execute(mba, msgs, nodem)
		// 3. encode
		for _, msg := range msgs {
			if err = h.pc.Encode(msg); err != nil {
				h.pc.Flush()
				h.deferHandle(messages, mba, err)
				return
			}
			msg.MarkEnd()
			msg.ResetSubs()
			if prom.On {
				prom.ProxyTime(h.cc.Name, msg.Request().CmdString(), int64(msg.TotalDur()/time.Microsecond))
			}
		}
		if err = h.pc.Flush(); err != nil {
			h.deferHandle(messages, mba, err)
			return
		}
		// 4. release resource
		for _, msg := range msgs {
			msg.Reset()
		}
		for node := range nodem {
			mba.Reset(node)
			delete(nodem, node)
		}
		// 5. reset MaxConcurrent
		messages = h.resetMaxConcurrent(messages, len(msgs))
	}
}

func (h *Handler) resetMaxConcurrent(msgs []*proto.Message, lastCount int) []*proto.Message {
	lm := len(msgs)
	if lm < maxConcurrent && lm == lastCount {
		proto.PutMsgs(msgs)
		msgs = proto.GetMsgs(lm * 2) // TODO: change the msgs by lastCount trending
	}
	return msgs
}

func (h *Handler) deferHandle(msgs []*proto.Message, mba *proto.MsgBatchAllocator, err error) {
	proto.PutMsgs(msgs)
	proto.PutMsgBatchAllocator(mba)
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
		if log.V(2) {
			if err != io.EOF {
				log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler close error:%+v", h.cc.Name, h.cc.ListenAddr, h.conn.RemoteAddr(), err)
			}
		}
	}
}
