package proxy

import (
	"bytes"
	errs "errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"overlord/lib/backoff"
	"overlord/lib/conv"
	"overlord/lib/hashkit"
	"overlord/lib/log"
	libnet "overlord/lib/net"
	"overlord/proto"
	"overlord/proto/memcache"
	mcbin "overlord/proto/memcache/binary"
	"overlord/proto/redis"
	rclstr "overlord/proto/redis/cluster"

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
)

var (
	defaultForwardCacheTypes = map[proto.CacheType]struct{}{
		proto.CacheTypeMemcache:       struct{}{},
		proto.CacheTypeMemcacheBinary: struct{}{},
		proto.CacheTypeRedis:          struct{}{},
	}
)

// NewForwarder new a Forwarder by cluster config.
func NewForwarder(cc *ClusterConfig) proto.Forwarder {
	// new Forwarder
	if _, ok := defaultForwardCacheTypes[cc.CacheType]; ok {
		return newDefaultForwarder(cc)
	}
	if cc.CacheType == proto.CacheTypeRedisCluster {
		dto := time.Duration(cc.DialTimeout) * time.Millisecond
		rto := time.Duration(cc.ReadTimeout) * time.Millisecond
		wto := time.Duration(cc.WriteTimeout) * time.Millisecond
		return rclstr.NewForwarder(cc.Name, cc.ListenAddr, cc.Servers, cc.NodeConnections, dto, rto, wto, []byte(cc.HashTag))
	}
	panic("unsupported protocol")
}

// defaultForwarder implement the default hashring router and msgbatch.
type defaultForwarder struct {
	cc *ClusterConfig

	ring    *hashkit.HashRing
	hashTag []byte

	// recording alias to real node
	alias    bool
	aliasMap map[string]string
	nodePipe map[string]*proto.NodeConnPipe
	nodePing map[string]*pinger

	state int32
}

// newDefaultForwarder must combinf.
func newDefaultForwarder(cc *ClusterConfig) proto.Forwarder {
	f := &defaultForwarder{cc: cc}
	// parse servers config
	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		panic(err)
	}
	f.alias = alias
	f.hashTag = []byte(cc.HashTag)
	f.ring = hashkit.NewRing(cc.HashDistribution, cc.HashMethod)
	f.aliasMap = make(map[string]string)
	if alias {
		for idx, aname := range ans {
			f.aliasMap[aname] = addrs[idx]
		}
		f.ring.Init(ans, ws)
	} else {
		f.ring.Init(addrs, ws)
	}
	// start nbc
	f.nodePipe = make(map[string]*proto.NodeConnPipe)
	for _, addr := range addrs {
		f.nodePipe[addr] = proto.NewNodeConnPipe(cc.NodeConnections, func() proto.NodeConn {
			return newNodeConn(cc, addr)
		})
	}
	if cc.PingAutoEject {
		f.nodePing = make(map[string]*pinger)
		for idx, addr := range addrs {
			w := ws[idx]
			pc := newPingConn(cc, addr)
			p := &pinger{ping: pc, cc: cc, node: addr, weight: w}
			if f.alias {
				p.alias = ans[idx]
			}
			go f.processPing(p)
		}
	}
	return f
}

// Forward impl proto.Forwarder
func (f defaultForwarder) Forward(msgs []*proto.Message) error {
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

// Close close forwarder.
func (f defaultForwarder) Close() error {
	if !atomic.CompareAndSwapInt32(&f.state, forwarderStateOpening, forwarderStateClosed) {
		return nil
	}
	return nil
}

func (f defaultForwarder) processPing(p *pinger) {
	del := false
	for {
		if err := p.ping.Ping(); err != nil {
			p.failure++
			p.retries = 0
			if netE, ok := err.(net.Error); !ok || !netE.Temporary() {
				_ = p.ping.Close()
				p.ping = newPingConn(p.cc, p.node)
			}
			if log.V(3) {
				log.Warnf("node ping node:%s fail:%d times with err:%v", p.node, p.failure, err)
			}
		} else {
			p.failure = 0
			if del {
				if p.alias != "" {
					f.ring.AddNode(p.alias, p.weight)
				} else {
					f.ring.AddNode(p.node, p.weight)
				}
				del = false
				if log.V(4) {
					log.Infof("node ping node:%s success and readd", p.node)
				}
			}
		}
		if f.cc.PingAutoEject && p.failure >= f.cc.PingFailLimit {
			if p.alias != "" {
				f.ring.DelNode(p.alias)
			} else {
				f.ring.DelNode(p.node)
			}
			del = true
			if log.V(2) {
				log.Errorf("node ping node:%s fail times equals limit:%d then del", p.node, f.cc.PingFailLimit)
			}
		}
		<-time.After(backoff.Backoff(p.retries))
		p.retries++
	}
}

func (f defaultForwarder) getPipes(key []byte) (ncp *proto.NodeConnPipe, ok bool) {
	var addr string
	if addr, ok = f.ring.GetNode(f.trimHashTag(key)); !ok {
		return
	}
	if f.alias {
		if addr, ok = f.aliasMap[addr]; !ok {
			return
		}
	}
	ncp, ok = f.nodePipe[addr]
	return
}

func (f defaultForwarder) trimHashTag(key []byte) []byte {
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

type pinger struct {
	cc     *ClusterConfig
	ping   proto.Pinger
	node   string
	alias  string
	weight int

	failure int
	retries int
}

func newNodeConn(cc *ClusterConfig, addr string) proto.NodeConn {
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		return memcache.NewNodeConn(cc.Name, addr, dto, rto, wto)
	case proto.CacheTypeMemcacheBinary:
		return mcbin.NewNodeConn(cc.Name, addr, dto, rto, wto)
	case proto.CacheTypeRedis:
		return redis.NewNodeConn(cc.Name, addr, dto, rto, wto)
	default:
		panic(proto.ErrNoSupportCacheType)
	}
}

func newPingConn(cc *ClusterConfig, addr string) proto.Pinger {
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond

	conn := libnet.DialWithTimeout(addr, dto, rto, wto)
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		return memcache.NewPinger(conn)
	case proto.CacheTypeMemcacheBinary:
		return mcbin.NewPinger(conn)
	case proto.CacheTypeRedis:
		return redis.NewPinger(conn)
	default:
		panic(proto.ErrNoSupportCacheType)
	}
}

func parseServers(svrs []string) (addrs []string, ws []int, ans []string, alias bool, err error) {
	for _, svr := range svrs {
		if strings.Contains(svr, " ") {
			alias = true
		} else if alias {
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
	return
}
