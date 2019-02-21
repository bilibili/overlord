package proxy

import (
	errs "errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"sort"

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
const MaxClusterCnt int32 = 128
const MonitorCfgIntervalSecs int32 = 50  // Time interval to monitor config change

// Proxy is proxy.
type Proxy struct {
	c   *Config
    ClusterConfFile string  // cluster configure file name
	ccs [MaxClusterCnt]*ClusterConfig

	forwarders [MaxClusterCnt]proto.Forwarder
    // current cluster count
    curClusterCnt int32
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
		if len(ccs) == 0 {
			log.Warnf("overlord will never listen on any port due to cluster is not specified")
		}
        p.curClusterCnt = 0
        for _, conf := range ccs {
            var clusterID = p.curClusterCnt
            forwarder := NewForwarder(conf)
            conf.ID = clusterID
            p.forwarders[clusterID] = forwarder
            p.ccs[clusterID] = conf
			p.serve(clusterID)
            p.curClusterCnt++
		}
        // Here, start a go routine to change content of a forwarder
        go p.monitorConfChange()
	})
}

func (p *Proxy) serve(cid int32) {
	// listen
    var conf = p.getClusterConf(cid)
	l, err := Listen(conf.ListenProto, conf.ListenAddr)
	if err != nil {
		panic(err)
	}
	log.Infof("overlord proxy cluster[%s] addr(%s) start listening", conf.Name, conf.ListenAddr)
	go p.accept(cid, l)
}

func (p *Proxy) accept(cid int32, l net.Listener) {
	for {
        var conf = p.getClusterConf(cid)
		if p.closed {
			log.Infof("overlord proxy cluster[%s] addr(%s) stop listen", conf.Name, conf.ListenAddr)
			return
		}
		conn, err := l.Accept()
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			log.Errorf("cluster(%s) addr(%s) accept connection error:%+v", conf.Name, conf.ListenAddr, err)
			continue
		}
		if p.c.Proxy.MaxConnections > 0 {
			if conns := atomic.LoadInt32(&p.conns); conns > p.c.Proxy.MaxConnections {
				// cache type
				var encoder proto.ProxyConn
				switch conf.CacheType {
				case types.CacheTypeMemcache:
					encoder = memcache.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case types.CacheTypeMemcacheBinary:
					encoder = mcbin.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case types.CacheTypeRedis:
					encoder = redis.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case types.CacheTypeRedisCluster:
					encoder = rclstr.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				}
				if encoder != nil {
                    var f = p.GetForwarder(cid)
					_ = encoder.Encode(proto.ErrMessage(ErrProxyMoreMaxConns), f)
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
		NewHandler(p, conf, conn).Handle()
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

// Get forwarder from proxy, thread safe
func (p *Proxy) GetForwarder(cid int32) (proto.Forwarder) {
    var f = p.forwarders[cid]
    return f
}

// Get forwarder from proxy, thread safe
func (p *Proxy) getClusterConf(cid int32) (*ClusterConfig) {
    var c = p.ccs[cid]
    return c
}

func (p* Proxy) parseChanged(newConfs, oldConfs []*ClusterConfig) (changed, newAdd []*ClusterConfig) {
    for _, newConf := range newConfs {
        var find = false
        var diff = false
        var oldConf *ClusterConfig = nil;
        var valid = true
        for _, conf := range oldConfs {
            if (newConf.Name != conf.Name) {
                continue
            }
            find = true
            diff, valid = compareConf(conf, newConf)
            if !valid {
                log.Errorf("configure change of cluster(%s) is invalid", newConf.Name)
                break
            }
            if diff {
                oldConf = conf
            }
            break
        }
        if !find {
            newAdd = append(newAdd, newConf)
            continue
        }
        if (!valid) || (find && !diff) {
            continue
        }
        newConf.ID = oldConf.ID
        changed = append(changed, newConf)
        continue
    }
    return changed, newAdd
}

func (p* Proxy) monitorConfChange() {
    for {
        time.Sleep(5 * time.Second)
        var succ, _, newConfs = LoadClusterConf(p.ClusterConfFile)
        if (!succ) {
            continue
        }
        var oldConfs []*ClusterConfig;
        for i := int32(0); i < p.curClusterCnt; i++ {
            var conf = p.ccs[i]
            oldConfs = append(oldConfs, conf)
        }
        var newAdd []*ClusterConfig;
        var changedConf []*ClusterConfig;
        changedConf, newAdd = p.parseChanged(newConfs, oldConfs)

        var clusterCnt = p.curClusterCnt + int32(len(newAdd))

        if (clusterCnt >= MaxClusterCnt) {
            log.Errorf("failed to reload conf as too much cluster will be added, new cluster count(%d) and max count(%d)",
                clusterCnt, MaxClusterCnt)
            continue
        }
        for _, conf := range changedConf {
            // use new forwarder now
            var forwarder = NewForwarder(conf)
            var prevForwarder = p.forwarders[conf.ID]
            p.forwarders[conf.ID] = forwarder
            prevForwarder.Close()
            p.ccs[conf.ID] = conf
            log.Infof("conf of cluster(%s:%d) is changed", conf.Name, conf.ID)
        }
        for _, conf := range newAdd {
            var forwarder = NewForwarder(conf)
            conf.ID = p.curClusterCnt
            p.forwarders[conf.ID] = forwarder
            p.ccs[conf.ID] = conf
            p.serve(p.curClusterCnt)
            p.curClusterCnt++
            log.Infof("Add new cluster(%s:%d)", conf.Name, conf.ID)
        }
    }
}

func compareConf(oldConf, newConf *ClusterConfig) (changed, valid bool) {
    valid = (oldConf.ListenAddr == newConf.ListenAddr)
    if ((oldConf.HashMethod != newConf.HashMethod) ||
            (oldConf.HashDistribution != newConf.HashDistribution) ||
            (oldConf.HashTag != newConf.HashTag) ||
            (oldConf.CacheType != newConf.CacheType) ||
            (oldConf.ListenProto != newConf.ListenProto) ||
            (oldConf.RedisAuth != newConf.RedisAuth) ||
            (oldConf.DialTimeout != newConf.DialTimeout) ||
            (oldConf.ReadTimeout != newConf.ReadTimeout) ||
            (oldConf.WriteTimeout != newConf.WriteTimeout) ||
            (oldConf.NodeConnections != newConf.NodeConnections) ||
            (oldConf.PingFailLimit != newConf.PingFailLimit) ||
            (oldConf.PingAutoEject != newConf.PingAutoEject)) {
        changed = true
        return
    }
    if (len(oldConf.Servers) != len(newConf.Servers)) {
        changed = true
        return
    }
    var server1 = oldConf.Servers
    var server2 = newConf.Servers
    sort.Strings(server1)
    sort.Strings(server2)
    for i := 0; i < len(server1); i++ {
        if (server1[i] != server2[i]) {
            changed = true
            return
        }
    }
    changed = false
    return
}
