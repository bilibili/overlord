package proxy

import (
	errs "errors"
	"sync"
	"sync/atomic"
	"time"

	"overlord/lib/log"
	libnet "overlord/lib/net"
	"overlord/lib/slowlog"
	"overlord/proto"
	"overlord/proto/memcache"
	mcbin "overlord/proto/memcache/binary"
	"overlord/proto/redis"
	rclstr "overlord/proto/redis/cluster"

	"github.com/pkg/errors"
	"fmt"
)

// proxy errors
var (
	ErrProxyMoreMaxConns = errs.New("Proxy accept more than max connextions")
)

// Proxy is proxy.
type Proxy struct {
	c   *Config
	ccs []*ClusterConfig

	executors map[string]proto.Executor
	once      sync.Once

	conns int32

	lock   sync.Mutex
	closed bool
}

// New new a proxy by config.
func New(c *Config) (p *Proxy, err error) {
	if err = c.Validate(); err != nil {
		err = errors.Wrap(err, "Proxy New config validate error")
		return
	}
	p = &Proxy{}
	p.c = c
	return
}

// Serve is the main accept() loop of a server.
func (p *Proxy) Serve(ccs []*ClusterConfig) {
	p.once.Do(func() {
		p.ccs = ccs
		p.executors = map[string]proto.Executor{}
		if len(ccs) == 0 {
			log.Warnf("overlord will never listen on any port due to cluster is not specified")
		}
		for _, cc := range ccs {
			// TODO: remove when test pass
			cc.SlowlogSlowerThan = 100000 // test config as 100ms
			cc.SlowlogOutputFile = fmt.Sprintf("/tmp/slowlog.%s.out", cc.Name)
			cc.SlowlogMaxCount = 1024
			go p.serve(cc)
		}
	})
}

func (p *Proxy) serve(cc *ClusterConfig) {
	if cc.SlowlogSlowerThan > 0 {
		slowlog.Init(cc.Name, cc.SlowlogOutputFile, cc.SlowlogMaxCount)
	}

	executor := NewExecutor(cc)
	p.lock.Lock()
	p.executors[cc.Name] = executor
	p.lock.Unlock()
	// listen
	l, err := Listen(cc.ListenProto, cc.ListenAddr)
	if err != nil {
		panic(err)
	}
	log.Infof("overlord proxy cluster[%s] addr(%s) already listened", cc.Name, cc.ListenAddr)
	for {
		conn, err := l.Accept()
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			log.Errorf("cluster(%s) addr(%s) accept connection error:%+v", cc.Name, cc.ListenAddr, err)
			continue
		}
		if p.c.Proxy.MaxConnections > 0 {
			if conns := atomic.AddInt32(&p.conns, 1); conns > p.c.Proxy.MaxConnections {
				// cache type
				var encoder proto.ProxyConn
				switch cc.CacheType {
				case proto.CacheTypeMemcache:
					encoder = memcache.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case proto.CacheTypeMemcacheBinary:
					encoder = mcbin.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case proto.CacheTypeRedis:
					encoder = redis.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case proto.CacheTypeRedisCluster:
					encoder = rclstr.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second), nil)
				}
				if encoder != nil {
					_ = encoder.Encode(proto.ErrMessage(ErrProxyMoreMaxConns))
					_ = encoder.Flush()
				}
				_ = conn.Close()
				if log.V(3) {
					log.Warnf("proxy reject connection count(%d) due to more than max(%d)", conns, p.c.Proxy.MaxConnections)
				}
				continue
			}
		}
		NewHandler(p, cc, conn, executor).Handle()
	}
}

// Close close proxy resource.
func (p *Proxy) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil
	}
	for _, executor := range p.executors {
		executor.Close()
	}
	return nil
}
