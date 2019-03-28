package proxy

import (
	"bytes"
	"context"
	errs "errors"
	"net"
	"overlord/pkg/prom"
	"strings"
	"sync/atomic"
	"time"

	"overlord/pkg/conv"
	"overlord/pkg/hashkit"
	"overlord/pkg/log"
	libnet "overlord/pkg/net"
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
		return rclstr.NewForwarder(cc.Name, cc.ListenAddr, cc.Servers, cc.NodeConnections, dto, rto, wto, []byte(cc.HashTag))
	}
	panic("unsupported protocol")
}

type Connections struct {
	// recording alias to real node
	alias    bool
	aliasMap map[string]string
	nodePipe map[string]*proto.NodeConnPipe
	ctx      context.Context
	cancel   context.CancelFunc
	ring     *hashkit.HashRing
}

func newConnections(cc *ClusterConfig) *Connections {
	var conn = &Connections{}
	conn.aliasMap = make(map[string]string)
	conn.nodePipe = make(map[string]*proto.NodeConnPipe)
	conn.ctx, conn.cancel = context.WithCancel(context.Background())
	conn.ring = hashkit.NewRing(cc.HashDistribution, cc.HashMethod)
	return conn
}

func (c *Connections) init(cc *ClusterConfig, addrs []string, ws []int, ans []string, hasAlias bool,
	conns map[string]*proto.NodeConnPipe) map[string]bool {
	c.alias = hasAlias
	copyed := make(map[string]bool)
	if hasAlias {
		for idx, aname := range ans {
			c.aliasMap[aname] = addrs[idx]
		}
		c.ring.Init(ans, ws)
	} else {
		c.ring.Init(addrs, ws)
	}
	// start nbc
	for _, addr := range addrs {
		toAddr := addr // NOTE: avoid closure
		var cnn, ok = conns[toAddr]
		if ok {
			c.nodePipe[toAddr] = cnn
			copyed[toAddr] = true
		} else {
			c.nodePipe[toAddr] = proto.NewNodeConnPipe(cc.NodeConnections, func() proto.NodeConn {
				return newNodeConn(cc, toAddr)
			})
		}
	}
	return copyed
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
	// parse servers config
	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		panic(err)
	}
	var conns = newConnections(cc)
	conns.init(cc, addrs, ws, ans, alias, make(map[string]*proto.NodeConnPipe))
	f.hashTag = []byte(cc.HashTag)
	f.conns.Store(conns)
	f.startPinger(conns, addrs, ans, ws, alias)
	return f
}

// Forward impl proto.Forwarder
func (f *defaultForwarder) Forward(msgs []*proto.Message) error {
	if closed := atomic.LoadInt32(&f.state); closed == forwarderStateClosed {
		return ErrForwarderClosed
	}
	conns, ok := f.conns.Load().(*Connections)
	if !ok {
		return ErrConnectionNotExist
	}

	for _, m := range msgs {
		if m.IsBatch() {
			for _, subm := range m.Batch() {
				ncp, ok := conns.getPipes(f, subm.Request().Key())
				if !ok {
					m.WithError(ErrForwarderHashNoNode)
					return errors.WithStack(ErrForwarderHashNoNode)
				}
				ncp.Push(subm)
			}
		} else {
			ncp, ok := conns.getPipes(f, m.Request().Key())
			if !ok {
				m.WithError(ErrForwarderHashNoNode)
				return errors.WithStack(ErrForwarderHashNoNode)
			}
			ncp.Push(m)
		}
	}
	return nil
}
func (f *defaultForwarder) startPinger(conns *Connections, addrs, ans []string, ws []int, useAlias bool) {
	if !f.cc.PingAutoEject {
		return
	}
	for idx, addr := range addrs {
		p := &pinger{cc: f.cc, addr: addr, alias: addr, weight: ws[idx]}
		if useAlias {
			p.alias = ans[idx]
		}
		go f.processPing(conns, p)
	}
}

func (f *defaultForwarder) Update(servers []string) error {
	addrs, ws, ans, alias, err := parseServers(servers)
	if err != nil {
		return err
	}
	oldConns, ok := f.conns.Load().(*Connections)
	if !ok {
		return errors.WithStack(ErrConnectionNotExist)
	}
	newConns := newConnections(f.cc)
	copyed := newConns.init(f.cc, addrs, ws, ans, alias, oldConns.nodePipe)
	f.conns.Store(newConns)
	oldConns.cancel()
	for addr, conn := range oldConns.nodePipe {
		if copyed[addr] {
			continue
		}
		log.Infof("connection to node:%s is not used anymore, just close it\n", addr)
		conn.Close()
	}
	f.startPinger(newConns, addrs, ans, ws, alias)
	return nil
}

// Close close forwarder.
func (f *defaultForwarder) Close() error {
	if atomic.CompareAndSwapInt32(&f.state, forwarderStateOpening, forwarderStateClosed) {
		// first closed
		var curConns, ok = f.conns.Load().(*Connections)
		if !ok {
			return errors.WithStack(ErrConnectionNotExist)
		}

		for _, np := range curConns.nodePipe {
			go np.Close()
		}
		return nil
	}
	return nil
}

func (c *Connections) getPipes(f *defaultForwarder, key []byte) (ncp *proto.NodeConnPipe, ok bool) {
	var addr string
	if addr, ok = c.ring.GetNode(f.trimHashTag(key)); !ok {
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

// PingSleepTime for unit test override!!!
var PingSleepTime = func(t bool) time.Duration {
	if t {
		return 5 * time.Minute
	}
	return time.Second
}

func (f *defaultForwarder) processPing(conns *Connections, p *pinger) {
	var (
		err error
		del bool
	)
	p.ping = newPingConn(p.cc, p.addr)
	for {
		select {
		case <-conns.ctx.Done():
			_ = p.ping.Close()
			log.Infof("node pinger is closed, return directly\n")
			return
		default:
			if atomic.LoadInt32(&f.state) == forwarderStateClosed {
				_ = p.ping.Close()
				return
			}
			err = p.ping.Ping()
			if err == nil {
				p.failure = 0
				if del {
					del = false
					conns.ring.AddNode(p.alias, p.weight)
					if log.V(4) {
						log.Infof("node ping node:%s addr:%s success and readd", p.alias, p.addr)
					}
				}
				time.Sleep(PingSleepTime(false))
				continue
			} else {
				_ = p.ping.Close()
				if prom.On {
					prom.ErrIncr(f.cc.Name, p.addr, "ping", errors.Cause(err).Error())
				}
				p.ping = newPingConn(p.cc, p.addr)
			}

			p.failure++
			if log.V(3) {
				log.Warnf("ping node:%s addr:%s fail:%d times with err:%v", p.alias, p.addr, p.failure, err)
			}
			if p.failure < f.cc.PingFailLimit {
				time.Sleep(PingSleepTime(false))
				continue
			}
			if !del {
				conns.ring.DelNode(p.alias)
				if prom.On {
					prom.ErrIncr(f.cc.Name, p.addr, "ping", "del node")
				}
				del = true
				if log.V(2) {
					log.Errorf("ping node:%s addr:%s fail times:%d ge to limit:%d then del", p.alias, p.addr, p.failure, f.cc.PingFailLimit)
				}
			} else if log.V(3) {
				log.Errorf("ping node:%s addr:%s fail times:%d ge to limit:%d and already deled", p.alias, p.addr, p.failure, f.cc.PingFailLimit)
			}
			time.Sleep(PingSleepTime(true))
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
			log.Errorf("use alias but not contains blank:%s\n", svr)
			err = ErrConfigServerFormat
			return
		}
		var (
			ss    []string
			addrW string
		)
		if alias {
			ss = strings.Split(svr, " ")
			if len(ss) != 2 {
				err = ErrConfigServerFormat
				return
			}
			addrW = ss[0]
			ans = append(ans, ss[1])
		} else {
			addrW = svr
		}
		ss = strings.Split(addrW, ":")
		if len(ss) != 3 {
			err = ErrConfigServerFormat
			return
		}
		addrs = append(addrs, net.JoinHostPort(ss[0], ss[1]))
		w, we := conv.Btoi([]byte(ss[2]))
		if we != nil || w <= 0 {
			err = ErrConfigServerFormat
			return
		}
		ws = append(ws, int(w))
	}
	if len(addrs) != len(ans) && len(ans) > 0 {
		err = ErrConfigServerFormat
	}
	return
}
