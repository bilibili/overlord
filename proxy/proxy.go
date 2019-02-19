package proxy

import (
	errs "errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

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

// proxy errors
var (
	ErrProxyMoreMaxConns = errs.New("Proxy accept more than max connextions")
)

// Proxy is proxy.
type Proxy struct {
	c   *Config
	ccs []*ClusterConfig

	forwarders map[string]proto.Forwarder
	forwarders [1024]proto.Forwarder
    id2index [100]int32
    cur_id int32
	once       sync.Once

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
		p.forwarders = map[string]proto.Forwarder{}
		if len(ccs) == 0 {
			log.Warnf("overlord will never listen on any port due to cluster is not specified")
		}
        cur_id = 0
        for _, conf := range ccs {
            forwarder := NewForwarder(conf)
            p.forwarders[cur_id] = forwarder
            p.id2cluster[cur_id] = cur_id
            ++cur_id
		}
		for _, cc := range ccs {
			p.serve(cc)
		}
        // Here, start a go routine to change content of a forwarder
        // Also, we need to rember current conf for each forwarder
	})
}

func (p *Proxy) serve(cc *ClusterConfig, id int32) {
	// forwarder := NewForwarder(cc)
	// p.lock.Lock()
	// p.forwarders[cc.Name] = forwarder
	// p.lock.Unlock()
	// listen
	l, err := Listen(cc.ListenProto, cc.ListenAddr)
	if err != nil {
		panic(err)
	}
	log.Infof("overlord proxy cluster[%s] addr(%s) start listening", cc.Name, cc.ListenAddr)
	go p.accept(cc, l, id)
}

func (p *Proxy) accept(cc *ClusterConfig, l net.Listener, cid int32) {
	for {
		if p.closed {
			log.Infof("overlord proxy cluster[%s] addr(%s) stop listen", cc.Name, cc.ListenAddr)
			return
		}
		conn, err := l.Accept()
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			log.Errorf("cluster(%s) addr(%s) accept connection error:%+v", cc.Name, cc.ListenAddr, err)
			continue
		}
		if p.c.Proxy.MaxConnections > 0 {
			if conns := atomic.LoadInt32(&p.conns); conns > p.c.Proxy.MaxConnections {
				// cache type
				var encoder proto.ProxyConn
				switch cc.CacheType {
				case types.CacheTypeMemcache:
					encoder = memcache.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case types.CacheTypeMemcacheBinary:
					encoder = mcbin.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case types.CacheTypeRedis:
					encoder = redis.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case types.CacheTypeRedisCluster:
					encoder = rclstr.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second), nil)
				}
				if encoder != nil {
					_ = encoder.Encode(proto.ErrMessage(ErrProxyMoreMaxConns))
					_ = encoder.Flush()
				}
				_ = conn.Close()
				if log.V(4) {
					log.Warnf("proxy reject connection count(%d) due to more than max(%d)", conns, p.c.Proxy.MaxConnections)
				}
				continue
			}
		}
		atomic.AddInt32(&p.conns, 1)
		NewHandler(p, cc, conn, cid).Handle()
	}
}

// Close close proxy resource.
func (p *Proxy) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil
	}
	for _, forwarder := range p.forwarders {
		forwarder.Close()
	}
	p.closed = true
	return nil
}

// Close close proxy resource.
func (p *Proxy) GetForwarder(cid int32) (proto.Forwarder) {
    index = p.id2index[cid]
    f = p.forwarders[index]
    return f
}

func (p* Proxy) processConfChange(confs []*ClusterConfig) {
    for _, conf := range confs {
        // identify which cluser has changed 
        forwarder := NewForwarder(conf)
    }
}
