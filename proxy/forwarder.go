package proxy

import (
	"bytes"
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
}

func newConnections() *Connections {
	var conn = &Connections{}
	conn.aliasMap = make(map[string]string)
	conn.nodePipe = make(map[string]*proto.NodeConnPipe)
	return conn
}

func (c *Connections) init(cc *ClusterConfig, addrs, ans []string, hasAlias, createConn bool) error {
	c.alias = hasAlias
	if hasAlias {
		for idx, aname := range ans {
			c.aliasMap[aname] = addrs[idx]
		}
	}
	if createConn {
		// start nbc
		for _, addr := range addrs {
			toAddr := addr // NOTE: avoid closure
			c.nodePipe[toAddr] = proto.NewNodeConnPipe(cc.NodeConnections, func() proto.NodeConn {
				return newNodeConn(cc, toAddr)
			})
		}
	}
	return nil
}

// defaultForwarder implement the default hashring router and msgbatch.
type defaultForwarder struct {
	cc *ClusterConfig

	ring    *hashkit.HashRing
	hashTag []byte

	conns   atomic.Value
	version int32
	state   int32
}

// newDefaultForwarder must combinf.
func newDefaultForwarder(cc *ClusterConfig) proto.Forwarder {
	f := &defaultForwarder{cc: cc, version: 2}
	// parse servers config
	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		panic(err)
	}
	var conns = newConnections()
	conns.init(cc, addrs, ans, alias, true)
	f.hashTag = []byte(cc.HashTag)
	f.ring = hashkit.NewRing(cc.HashDistribution, cc.HashMethod)
	f.conns.Store(conns)
	if alias {
		f.ring.Init(ans, ws)
	} else {
		f.ring.Init(addrs, ws)
	}
	if cc.PingAutoEject {
		var curVersion = atomic.LoadInt32(&f.version)
		for idx, addr := range addrs {
			p := &pinger{cc: cc, addr: addr, alias: addr, weight: ws[idx]}
			if alias {
				p.alias = ans[idx]
			}
			go f.processPing(p, curVersion)
		}
	}
	return f
}

// Forward impl proto.Forwarder
func (f *defaultForwarder) Forward(msgs []*proto.Message) error {
	if closed := atomic.LoadInt32(&f.state); closed == forwarderStateClosed {
		return ErrForwarderClosed
	}
	for _, m := range msgs {
		if m.IsBatch() {
			for _, subm := range m.Batch() {
				ncp, ok := f.getPipes(subm.Request().Key())
				if !ok {
					m.WithError(ErrForwarderHashNoNode)
					return errors.WithStack(ErrForwarderHashNoNode)
				}
				ncp.Push(subm)
			}
		} else {
			ncp, ok := f.getPipes(m.Request().Key())
			if !ok {
				m.WithError(ErrForwarderHashNoNode)
				return errors.WithStack(ErrForwarderHashNoNode)
			}
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
	var curConns, ok = f.conns.Load().(*Connections)
	if !ok {
		return errors.WithStack(ErrConnectionNotExist)
	}
	var newConns = newConnections()
	newConns.init(f.cc, addrs, ans, alias, false)
	var copyed = make(map[string]bool)
	for _, addr := range addrs {
		toAddr := addr
		conn, ok := curConns.nodePipe[toAddr]
		if ok {
			newConns.nodePipe[toAddr] = conn
			copyed[toAddr] = true
			continue
		}
		newConns.nodePipe[toAddr] = proto.NewNodeConnPipe(f.cc.NodeConnections, func() proto.NodeConn {
			return newNodeConn(f.cc, toAddr)
		})
	}
	atomic.AddInt32(&f.version, 1)
	f.conns.Store(newConns)
	if alias {
		f.ring.Replace(ans, ws)
	} else {
		f.ring.Replace(addrs, ws)
	}
	for addr, conn := range curConns.nodePipe {
		var _, find = copyed[addr]
		if find {
			continue
		}
		conn.Close()
	}

	if f.cc.PingAutoEject {
		var curVersion = atomic.LoadInt32(&f.version)
		for idx, addr := range addrs {
			p := &pinger{cc: f.cc, addr: addr, alias: addr, weight: ws[idx]}
			if alias {
				p.alias = ans[idx]
			}
			go f.processPing(p, curVersion)
		}
	}
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

func (f *defaultForwarder) getPipes(key []byte) (ncp *proto.NodeConnPipe, ok bool) {
	var addr string
	if addr, ok = f.ring.GetNode(f.trimHashTag(key)); !ok {
		return
	}
	var conns *Connections
	conns, ok = f.conns.Load().(*Connections)
	if !ok {
		log.Errorf("failed to load connections\n")
		return
	}
	if conns.alias {
		if addr, ok = conns.aliasMap[addr]; !ok {
			return
		}
	}
	ncp, ok = conns.nodePipe[addr]
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

func (f *defaultForwarder) processPing(p *pinger, initVersion int32) {
	var (
		err error
		del bool
	)
	p.ping = newPingConn(p.cc, p.addr)
	for {
		if atomic.LoadInt32(&f.state) == forwarderStateClosed {
			_ = p.ping.Close()
			return
		}
		if atomic.LoadInt32(&f.version) != initVersion {
			log.Infof("find forwarder version changed, closed ping to addr:%s\n", p.addr)
			_ = p.ping.Close()
			return
		}
		err = p.ping.Ping()
		if err == nil {
			p.failure = 0
			if del {
				del = false
				f.ring.AddNode(p.alias, p.weight)
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
			f.ring.DelNode(p.alias)
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
				log.Errorf("use alias but not len is not 2 svr:%s\n", svr)
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
			log.Errorf("use addr not 3 addr:%s\n", svr)
			err = ErrConfigServerFormat
			return
		}
		addrs = append(addrs, net.JoinHostPort(ss[0], ss[1]))
		w, we := conv.Btoi([]byte(ss[2]))
		if we != nil || w <= 0 {
			log.Errorf("use wait not < 0 addr:%s\n", svr)
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
