package proxy

import (
	"bytes"
	"context"
	errs "errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"overlord/pkg/conv"
	"overlord/pkg/hashkit"
	"overlord/pkg/log"
	libnet "overlord/pkg/net"
	"overlord/pkg/prom"
	"overlord/pkg/types"
	"overlord/proxy/proto"
	"overlord/proxy/proto/memcache"
	mcbin "overlord/proxy/proto/memcache/binary"
	"overlord/proxy/proto/redis"
	rclstr "overlord/proxy/proto/redis/cluster"

	"github.com/pkg/errors"
)

const (
	forwarderStateOpening = int32(0)
	forwarderStateClosed  = int32(1)
)

// errors
var (
	ErrConfigServerFormat  = errs.New("servers config format error")
	ErrForwarderHashNoNode = errs.New("forwarder hash no hit node")
	ErrForwarderClosed     = errs.New("forwarder already closed")
	ErrConnectionNotExist  = errs.New("connection of forwarder is not initialized")
)

var (
	defaultForwardCacheTypes = map[types.CacheType]struct{}{
		types.CacheTypeMemcache:       struct{}{},
		types.CacheTypeMemcacheBinary: struct{}{},
		types.CacheTypeRedis:          struct{}{},
	}
)

// NewForwarder new a Forwarder by cluster config.
func NewForwarder(cc *ClusterConfig) proto.Forwarder {
	// new Forwarder
	if _, ok := defaultForwardCacheTypes[cc.CacheType]; ok {
		return newDefaultForwarder(cc)
	}
	if cc.CacheType == types.CacheTypeRedisCluster {
		dto := time.Duration(cc.DialTimeout) * time.Millisecond
		rto := time.Duration(cc.ReadTimeout) * time.Millisecond
		wto := time.Duration(cc.WriteTimeout) * time.Millisecond
		return rclstr.NewForwarder(cc.Name, cc.ListenAddr, cc.Servers, cc.NodeConnections, cc.NodePipeCount, dto, rto, wto, []byte(cc.HashTag))
	}
	panic("unsupported protocol")
}

// defaultForwarder implement the default hashring router and msgbatch.
type defaultForwarder struct {
	cc      *ClusterConfig
	hashTag []byte
	conns   atomic.Value
	state   int32
}

// newDefaultForwarder must combinf.
func newDefaultForwarder(cc *ClusterConfig) proto.Forwarder {
	f := &defaultForwarder{cc: cc}
	f.hashTag = []byte(cc.HashTag)
	// parse servers config
	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		panic(err)
	}
	conns := newConnections(cc)
	conns.init(addrs, ans, ws, alias, nil)
	conns.startPinger()
	f.conns.Store(conns)
	return f
}

// Forward impl proto.Forwarder
func (f *defaultForwarder) Forward(msgs []*proto.Message) error {
	if closed := atomic.LoadInt32(&f.state); closed == forwarderStateClosed {
		return ErrForwarderClosed
	}
	conns, ok := f.conns.Load().(*connections)
	if !ok {
		return ErrConnectionNotExist
	}
	for _, m := range msgs {
		if m.IsBatch() {
			ctxMap := make(map[string]*nodeConnPipeContext)
			for _, subm := range m.Batch() {
				key := subm.Request().Key()
				ctx, ok := conns.getPipesContext(f.trimHashTag(key))
				if !ok {
					m.WithError(ErrForwarderHashNoNode)
					return errors.WithStack(ErrForwarderHashNoNode)
				}
				if _, ok = ctxMap[ctx.identifier]; !ok {
					ctxMap[ctx.identifier] = ctx
				}
				ctxMap[ctx.identifier].msgs = append(ctxMap[ctx.identifier].msgs, subm)
				subm.MarkStartPipe()
			}
			f.batchPush(ctxMap)
		} else {
			key := m.Request().Key()
			ncp, ok := conns.getPipes(f.trimHashTag(key))
			if !ok {
				m.WithError(ErrForwarderHashNoNode)
				return errors.WithStack(ErrForwarderHashNoNode)
			}
			m.MarkStartPipe()
			ncp.Push(m)
		}
	}
	return nil
}

func (f *defaultForwarder) Update(servers []string) error {
	addrs, ws, ans, alias, err := parseServers(servers)
	if err != nil {
		return err
	}
	oldConns, ok := f.conns.Load().(*connections)
	if !ok {
		return errors.WithStack(ErrConnectionNotExist)
	}
	newConns := newConnections(f.cc)
	copyed := newConns.init(addrs, ans, ws, alias, oldConns.nodePipe)
	f.conns.Store(newConns)
	oldConns.cancel()
	newConns.startPinger()
	// close unused
	for addr, conn := range oldConns.nodePipe {
		if copyed[addr] {
			continue
		}
		log.Infof("connection to node:%s is not used anymore, just close it", addr)
		conn.Close()
	}
	return nil
}

// Close close forwarder.
func (f *defaultForwarder) Close() error {
	if atomic.CompareAndSwapInt32(&f.state, forwarderStateOpening, forwarderStateClosed) {
		// first closed
		var curConns, ok = f.conns.Load().(*connections)
		if !ok {
			return errors.WithStack(ErrConnectionNotExist)
		}
		for _, np := range curConns.nodePipe {
			go np.Close()
		}
		curConns.cancel()
		return nil
	}
	return nil
}

func (f *defaultForwarder) batchPush(ctxMap map[string]*nodeConnPipeContext) {
	for _, ctx := range ctxMap {
		mainMsg := ctx.msgs[0]
		var reqs []proto.Request
		for i := 1; i < len(ctx.msgs); i++ {
			reqs = append(reqs, ctx.msgs[i].Request())
		}
		if err := mainMsg.Request().Merge(reqs); err != nil {
			// todo report error
		}

		ctx.ncp.Push(mainMsg)
	}
}

func (f *defaultForwarder) trimHashTag(key []byte) []byte {
	if len(f.hashTag) != 2 {
		return key
	}
	bidx := bytes.IndexByte(key, f.hashTag[0])
	if bidx == -1 {
		return key
	}
	eidx := bytes.IndexByte(key[bidx+1:], f.hashTag[1])
	if eidx == -1 {
		return key
	}
	return key[bidx+1 : bidx+1+eidx]
}

type connections struct {
	ctx    context.Context
	cancel context.CancelFunc
	// recording alias to real node
	cc         *ClusterConfig
	alias      bool
	addrs, ans []string
	ws         []int
	aliasMap   map[string]string
	nodePipe   map[string]*proto.NodeConnPipe
	ring       *hashkit.HashRing
}

func newConnections(cc *ClusterConfig) *connections {
	c := &connections{}
	c.cc = cc
	c.aliasMap = make(map[string]string)
	c.nodePipe = make(map[string]*proto.NodeConnPipe)
	c.ring = hashkit.NewRing(cc.HashDistribution, cc.HashMethod)
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *connections) init(addrs, ans []string, ws []int, alias bool, oldNcps map[string]*proto.NodeConnPipe) map[string]bool {
	c.alias = alias
	c.addrs = addrs
	c.ans = ans
	c.ws = ws
	if alias {
		for idx, aname := range ans {
			c.aliasMap[aname] = addrs[idx]
		}
		c.ring.Init(ans, ws)
	} else {
		c.ring.Init(addrs, ws)
	}
	copyed := make(map[string]bool)
	// start nbc
	for _, addr := range addrs {
		toAddr := addr // NOTE: avoid closure
		var cnn, ok = oldNcps[toAddr]
		if ok {
			c.nodePipe[toAddr] = cnn
			copyed[toAddr] = true
		} else {
			c.nodePipe[toAddr] = proto.NewNodeConnPipe(c.cc.NodeConnections, c.cc.NodePipeCount, func() proto.NodeConn {
				return newNodeConn(c.cc, toAddr)
			})
		}
	}
	return copyed
}

type nodeConnPipeContext struct {
	identifier string
	ncp        *proto.NodeConnPipe
	msgs       []*proto.Message
}

func (c *connections) getPipes(key []byte) (ncp *proto.NodeConnPipe, ok bool) {
	var addr string
	if addr, ok = c.ring.GetNode(key); !ok {
		return
	}
	if c.alias {
		if addr, ok = c.aliasMap[addr]; !ok {
			return
		}
	}
	ncp, ok = c.nodePipe[addr]
	return
}

func (c *connections) getPipesContext(key []byte) (ctx *nodeConnPipeContext, ok bool) {
	var addr string
	if addr, ok = c.ring.GetNode(key); !ok {
		return
	}
	if c.alias {
		if addr, ok = c.aliasMap[addr]; !ok {
			return
		}
	}
	ncp, ok := c.nodePipe[addr]
	if !ok {
		return
	}
	ctx = &nodeConnPipeContext{
		identifier: addr,
		ncp:        ncp,
	}
	return
}

func (c *connections) startPinger() {
	if !c.cc.PingAutoEject {
		return
	}
	for idx, addr := range c.addrs {
		p := &pinger{cc: c.cc, addr: addr, alias: addr, weight: c.ws[idx]}
		if c.alias {
			p.alias = c.ans[idx]
		}
		go c.processPing(p)
	}
}

// pingSleepTime for unit test override!!!
var pingSleepTime = func(t bool) time.Duration {
	if t {
		return 5 * time.Minute
	}
	return time.Second
}

func (c *connections) processPing(p *pinger) {
	var (
		err error
		del bool
	)
	p.ping = newPingConn(p.cc, p.addr)
	for {
		select {
		case <-c.ctx.Done():
			_ = p.ping.Close()
			log.Infof("node:%s addr:%s pinger is closed return directly", p.alias, p.addr)
			return
		default:
			err = p.ping.Ping()
			if err == nil {
				p.failure = 0
				if del {
					del = false
					c.ring.AddNode(p.alias, p.weight)
					if log.V(4) {
						log.Infof("node ping node:%s addr:%s success and readd", p.alias, p.addr)
					}
				}
				time.Sleep(pingSleepTime(false))
				continue
			} else {
				_ = p.ping.Close()
				if prom.On {
					prom.ErrIncr(c.cc.Name, p.addr, "ping", "network err")
				}
			}

			p.failure++
			if log.V(3) {
				log.Warnf("ping node:%s addr:%s fail:%d times with err:%v", p.alias, p.addr, p.failure, err)
			}
			if p.failure < c.cc.PingFailLimit {
				time.Sleep(pingSleepTime(false))
				p.ping = newPingConn(p.cc, p.addr)
				continue
			}
			if !del {
				c.ring.DelNode(p.alias)
				if prom.On {
					prom.ErrIncr(c.cc.Name, p.addr, "ping", "del node")
				}
				del = true
				if log.V(2) {
					log.Errorf("ping node:%s addr:%s fail times:%d ge to limit:%d then del", p.alias, p.addr, p.failure, c.cc.PingFailLimit)
				}
			} else if log.V(3) {
				log.Errorf("ping node:%s addr:%s fail times:%d ge to limit:%d and already deled", p.alias, p.addr, p.failure, c.cc.PingFailLimit)
			}
			time.Sleep(pingSleepTime(true))
			p.ping = newPingConn(p.cc, p.addr)
		}
	}
}

type pinger struct {
	cc     *ClusterConfig
	ping   proto.Pinger
	addr   string
	alias  string // NOTE: default is addr
	weight int

	failure int
}

func newNodeConn(cc *ClusterConfig, addr string) proto.NodeConn {
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond
	switch cc.CacheType {
	case types.CacheTypeMemcache:
		return memcache.NewNodeConn(cc.Name, addr, dto, rto, wto)
	case types.CacheTypeMemcacheBinary:
		return mcbin.NewNodeConn(cc.Name, addr, dto, rto, wto)
	case types.CacheTypeRedis:
		return redis.NewNodeConn(cc.Name, addr, dto, rto, wto)
	default:
		panic(types.ErrNoSupportCacheType)
	}
}

func newPingConn(cc *ClusterConfig, addr string) proto.Pinger {
	const timeout = 100 * time.Millisecond
	conn := libnet.DialWithTimeout(addr, timeout, timeout, timeout)
	switch cc.CacheType {
	case types.CacheTypeMemcache:
		return memcache.NewPinger(conn)
	case types.CacheTypeMemcacheBinary:
		return mcbin.NewPinger(conn)
	case types.CacheTypeRedis:
		return redis.NewPinger(conn)
	default:
		panic(types.ErrNoSupportCacheType)
	}
}

func parseServers(svrs []string) (addrs []string, ws []int, ans []string, alias bool, err error) {
	for _, svr := range svrs {
		if strings.Contains(svr, " ") {
			alias = true
		} else if alias {
			err = errors.Wrapf(ErrConfigServerFormat, "server:%s", svr)
			return
		}
		var (
			ss    []string
			addrW string
		)
		if alias {
			ss = strings.Split(svr, " ")
			if len(ss) != 2 {
				err = errors.Wrapf(ErrConfigServerFormat, "server:%s", svr)
				return
			}
			addrW = ss[0]
			ans = append(ans, ss[1])
		} else {
			addrW = svr
		}
		ss = strings.Split(addrW, ":")
		if len(ss) != 3 {
			err = errors.Wrapf(ErrConfigServerFormat, "server:%s", svr)
			return
		}
		addrs = append(addrs, net.JoinHostPort(ss[0], ss[1]))
		w, we := conv.Btoi([]byte(ss[2]))
		if we != nil || w <= 0 {
			err = errors.Wrapf(ErrConfigServerFormat, "server:%s", svr)
			return
		}
		ws = append(ws, int(w))
	}
	if len(addrs) != len(ans) && len(ans) > 0 {
		err = ErrConfigServerFormat
	}
	return
}
