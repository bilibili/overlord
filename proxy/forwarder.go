package proxy

import (
	"bytes"
	errs "errors"
	"net"
	"strings"
	"sync/atomic"
	"time"
	"runtime"

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

// defaultForwarder implement the default hashring router and msgbatch.
type defaultForwarder struct {
	cc *ClusterConfig

	ring    *hashkit.HashRing
	hashTag []byte

	// recording alias to real node
	alias    bool
	aliasMap map[string]string
	nodePipe map[string]*proto.NodeConnPipe

	state int32
}

func deleteForwarder(f *defaultForwarder) {
    log.Infof("start to delete default forwarder:%p for cluster:%d now", f, f.cc.ID)
    for name, conn := range(f.nodePipe) {
        log.Infof("close connection to node:%s for forwarder:%p when delete forwarder", name, f)
        conn.Close()
    }
}

// newDefaultForwarder must combinf.
func newDefaultForwarder(cc *ClusterConfig) proto.Forwarder {
	f := &defaultForwarder{cc: cc}
    log.Infof("create default forwarder:%p for cluster:%d", f, f.cc.ID)
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
    log.Infof("try to create connection for cluster:%s now, node count:%d", cc.Name, len(addrs))
	// start nbc
	f.nodePipe = make(map[string]*proto.NodeConnPipe)
	for _, addr := range addrs {
        log.Infof("try to new connection to addr:%s", addr)
		toAddr := addr // NOTE: avoid closure
		f.nodePipe[toAddr] = proto.NewNodeConnPipe(cc.NodeConnections, func() proto.NodeConn {
			return newNodeConn(cc, toAddr)
		})
	}
	if cc.PingAutoEject {
        log.Infof("try to create pinger for cluster:%s", cc.Name)
		for idx, addr := range addrs {
			p := &pinger{cc: cc, addr: addr, alias: addr, weight: ws[idx]}
			if f.alias {
				p.alias = ans[idx]
			}
			go f.processPing(p)
		}
	}
    runtime.SetFinalizer(f, deleteForwarder)
	return f
}

// Forward impl proto.Forwarder
func (f defaultForwarder) Forward(msgs []*proto.Message) error {
	// if closed := atomic.LoadInt32(&f.state); closed == forwarderStateClosed {
	//	return ErrForwarderClosed
	// }
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

// pingSleepTime for unit test override!!!
var pingSleepTime = func(t bool) int32 {
	if t {
		return 10
	}
	return 1
}

func (f defaultForwarder) sleep(timeInSec int32) {
    for i := int32(0); i < timeInSec; i++ {
        var closed = atomic.LoadInt32(&f.state)
        if closed == forwarderStateClosed {
            return
        }
        time.Sleep(time.Second)
    }
}

func (f defaultForwarder) processPing(p *pinger) {
	var (
		err error
		del bool
	)
	p.ping = newPingConn(p.cc, p.addr)
	for {
        if closed := atomic.LoadInt32(&f.state); closed == forwarderStateClosed {
            _ = p.ping.Close()
            log.Warnf("forwarder of cluster:%d is close, no need to ping anymore", f.cc.ID)
            return
        }
        log.Infof("pinger of forwarder:%p try to ping backend", f)
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
			f.sleep(pingSleepTime(false))
			continue
		} else {
			_ = p.ping.Close()
			p.ping = newPingConn(p.cc, p.addr)
		}

		p.failure++
		if log.V(3) {
			log.Warnf("ping node:%s addr:%s fail:%d times with err:%v", p.alias, p.addr, p.failure, err)
		}
		if p.failure < f.cc.PingFailLimit {
			f.sleep(pingSleepTime(false))
			continue
		}
		if !del {
			f.ring.DelNode(p.alias)
			del = true
			if log.V(2) {
				log.Errorf("ping node:%s addr:%s fail times:%d ge to limit:%d then del", p.alias, p.addr, p.failure, f.cc.PingFailLimit)
			}
		} else if log.V(3) {
			log.Errorf("ping node:%s addr:%s fail times:%d ge to limit:%d and already deled", p.alias, p.addr, p.failure, f.cc.PingFailLimit)
		}
		f.sleep(pingSleepTime(true))
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
