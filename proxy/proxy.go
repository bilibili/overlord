package proxy

import (
	errs "errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"runtime"
	"sort"
	// "strings"

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
    GClusterSn int32 = 0
    MonitorCfgIntervalSecs int = 10  // Time interval to monitor config change
    GClusterChangeCount int32 = 0
    GClusterCount int32 = 0
)
const MaxClusterCnt int32 = 128

type Cluster struct {
    conf *ClusterConfig
    mutex sync.Mutex
    clientConns map[int64]*libnet.Conn
    forwarder proto.Forwarder
}

// Proxy is proxy.
type Proxy struct {
	c   *Config
    ClusterConfFile string  // cluster configure file name

    clusters [MaxClusterCnt]*Cluster
    curClusterCnt int32
	once       sync.Once

	conns int32

	lock   sync.Mutex
	closed bool
}

// New new a proxy by config.
func NewProxy(c *Config) (p *Proxy, err error) {
	if err = c.Validate(); err != nil {
		err = errors.Wrap(err, "Proxy New config validate error")
		return
	}
	p = &Proxy{}
	p.c = c
	return
}

func genClusterSn() int32 {
    var id = atomic.AddInt32(&GClusterSn, 1)
    return id
}

// Serve is the main accept() loop of a server.
func (p *Proxy) Serve(ccs []*ClusterConfig) {
	p.once.Do(func() {
		if len(ccs) == 0 {
			log.Warnf("overlord will never listen on any port due to cluster is not specified")
		}
        p.curClusterCnt = 0
        for _, conf := range ccs {
            p.addCluster(conf)
		}
        // Here, start a go routine to change content of a forwarder
        go p.monitorConfChange()
	})
}

func (p *Proxy) addCluster(newConf *ClusterConfig) {
    newConf.SN = genClusterSn()
    log.Infof("try to add cluster:%s#%d\n", newConf.Name, newConf.SN)
	p.lock.Lock()
    var clusterID = p.curClusterCnt
    newConf.ID = clusterID
    var newForwarder = NewForwarder(newConf)
    var cnt = newForwarder.AddRef()
    log.Infof("add cluster:%d with forwarder:%d refs cnt:%d\n", clusterID, newForwarder.ID(), cnt)
    var cluster = &Cluster{conf: newConf, forwarder: newForwarder}
    cluster.clientConns = make(map[int64]*libnet.Conn)
    p.clusters[clusterID] = cluster
    p.lock.Unlock()
    p.curClusterCnt++
    p.serve(clusterID)
    atomic.AddInt32(&GClusterCount, 1)
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
                    f.Release()
				}
				_ = conn.Close()
				if log.V(4) {
					log.Warnf("proxy reject connection count(%d) due to more than max(%d)", conns, p.c.Proxy.MaxConnections)
				}
				continue
			}
		}
		atomic.AddInt32(&p.conns, 1)
        var advConn = libnet.NewConn(conn, time.Second*time.Duration(p.c.Proxy.ReadTimeout), time.Second*time.Duration(p.c.Proxy.WriteTimeout))
        log.Infof("cluster:%d accept new connection:%d\n", cid, advConn.ID)
        runtime.SetFinalizer(advConn, deleteConn)
        var ret = p.addConnection(cid, conf.SN, advConn)
        if ret != 0 {
            // corner case, configure changed when we try to keep this connection
            log.Errorf("corner case, configure just changed when after accept a connection")
            advConn.Close()
            continue
        }
		NewHandler(p, conf, advConn).Handle()
	}
}

func deleteConn(conn *libnet.Conn) {
    log.Errorf("delete front connection:%d\n", conn.ID)
}

// Close close proxy resource.
func (p *Proxy) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil
	}
    for i := 0; i < int(p.curClusterCnt); i++ {
		p.clusters[i].Close()
    }
	p.closed = true
	return nil
}

// Get forwarder from proxy, thread safe
func (p *Proxy) addConnection(cid int32, sn int32, conn *libnet.Conn) int {
    var ret = p.clusters[cid].addConnection(sn, conn)
    return ret
}

func (p *Proxy) RemoveConnection(cid int32, connID int64) {
    p.clusters[cid].removeConnection(connID)
}

func (p *Proxy) CloseAndRemoveConnection(cid int32, connID int64) {
    p.clusters[cid].closeAndRemoveConnection(connID)
}

func (p *Proxy) CloseAllConnections(cid int32) {
    p.clusters[cid].closeAllConnections()
}

// Get forwarder from proxy, thread safe
func (p *Proxy) GetForwarder(cid int32) (proto.Forwarder) {
    return p.clusters[cid].getForwarder()
}

// Get forwarder from proxy, thread safe
func (p *Proxy) getClusterConf(cid int32) (*ClusterConfig) {
    return p.clusters[cid].getConf()
}

func (p* Proxy) parseChanged(newConfs, oldConfs []*ClusterConfig) (changed, newAdd []*ClusterConfig) {
    for _, newConf := range newConfs {
        var find = false
        var diff = false
        var oldConf *ClusterConfig = nil;
        var valid = true
        for _, conf := range oldConfs {
            if (newConf.Name != conf.Name) {
                // log.Infof("confname is different, %s VS %s\n", newConf.Name, conf.Name)
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
    // log.Infof("new conf len:%d old conf len:%d\n", len(newConfs), len(oldConfs))
    return changed, newAdd
}

func (p* Proxy) monitorConfChange() {
    log.Infof("start to check whether cluster conf file is changed or not from:%s\n", p.ClusterConfFile)
    for {
        time.Sleep(time.Duration(MonitorCfgIntervalSecs) * time.Second)
        var succ, _, newConfs = LoadClusterConf(p.ClusterConfFile)
        if (!succ) {
            log.Errorf("failed to load conf file:%s\n", p.ClusterConfFile)
            continue
        }
        var oldConfs []*ClusterConfig;
        for i := int32(0); i < p.curClusterCnt; i++ {
            var conf = p.clusters[i].getConf()
            oldConfs = append(oldConfs, conf)
        }
        var newAdd []*ClusterConfig;
        var changedConf []*ClusterConfig;
        changedConf, newAdd = p.parseChanged(newConfs, oldConfs)
        // log.Infof("check conf, change cnt:%d added cnt:%d\n", len(changedConf), len(newAdd))

        var clusterCnt = p.curClusterCnt + int32(len(newAdd))

        if (clusterCnt >= MaxClusterCnt) {
            log.Errorf("failed to reload conf as too much cluster will be added, new cluster count(%d) and max count(%d)",
                clusterCnt, MaxClusterCnt)
            continue
        }
        for _, conf := range changedConf {
            // use new forwarder now
            log.Infof("conf of cluster(%s:%d) is changed, start to process", conf.Name, conf.ID)
            p.clusters[conf.ID].processConfChange(conf)
            atomic.AddInt32(&GClusterChangeCount, 1)
        }
        for _, conf := range newAdd {
            p.addCluster(conf)
            log.Infof("Add new cluster(%s:%d)", conf.Name, conf.ID)
        }
    }
}

func (c *Cluster) Close() {
    c.forwarder.Close()
    c.closeAllConnections()
}

func (c *Cluster) addConnection(sn int32, conn* libnet.Conn) int {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    if sn != c.conf.SN {
        return -1
    }
    c.clientConns[conn.ID] = conn
    log.Infof("cluster:%d:%d add front connection:%d", c.conf.ID, c.conf.SN, conn.ID)
    return 0
}

func (c *Cluster) removeConnection(id int64) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    delete(c.clientConns, id)
    log.Infof("cluster:%d:%d remove front connection:%d", c.conf.ID, c.conf.SN, id)
}

func (c *Cluster) closeAndRemoveConnection(id int64) {
    c.mutex.Lock()
    var conn, ok = c.clientConns[id]
    if !ok {
        c.mutex.Unlock()
        return
    }
    delete(c.clientConns, id)
    c.mutex.Unlock()
    conn.Close()
    log.Infof("cluster:%d:%d close front connection:%d", c.conf.ID, c.conf.SN, id)
}

func (c *Cluster) closeAllConnections() {
    c.mutex.Lock()
    var curConns = c.clientConns
    c.clientConns = make(map[int64]*libnet.Conn)
    c.mutex.Unlock()
    for _, conn := range(curConns) {
        conn.Close()
    }
    log.Infof("cluster:%d:%d close all front connections", c.conf.ID, c.conf.SN)
}

func (c *Cluster) processConfChange(newConf *ClusterConfig) {
    newConf.ID = c.conf.ID
    newConf.SN = genClusterSn()
    var newForwarder = NewForwarder(newConf)
    var cnt = newForwarder.AddRef()
    c.mutex.Lock()
    var oldConns = c.clientConns
    var oldForwarder = c.forwarder
    c.forwarder = newForwarder
    if (newConf.CloseWhenChange) {
        c.clientConns = make(map[int64]*libnet.Conn)
    }
    c.conf = newConf
    c.mutex.Unlock()
    log.Infof("start to close forwarder of cluster:%d\n", c.conf.ID)
    oldForwarder.Close()
    oldForwarder.Release()
    if (newConf.CloseWhenChange) {
        log.Infof("start to close front connections:%d\n", len(oldConns))
        for _, conn := range(oldConns) {
            conn.Close()
        }
    }
    log.Infof("replace cluster:%d with forwarder:%d use cnt:%d\n", newConf.ID, newForwarder.ID(), cnt)
}

func (c *Cluster) getForwarder() (proto.Forwarder) {
    c.mutex.Lock()
    var f = c.forwarder
    c.mutex.Unlock()
    var cnt = f.AddRef()
    log.Infof("get forwarder:%d for cluster:%s#%d use cnt:%d\n", f.ID(), c.conf.Name, c.conf.ID, cnt)
    return f
}

func (c *Cluster) getConf() (*ClusterConfig) {
    c.mutex.Lock()
    var conf = c.conf
    c.mutex.Unlock()
    return conf
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
            (oldConf.PingAutoEject != newConf.PingAutoEject)) ||
            (oldConf.CloseWhenChange != newConf.CloseWhenChange) {
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
    // var str1 = strings.Join(server1, "_")
    // var str2 = strings.Join(server2, "_")
    // log.Infof("server1:%s server2:%s\n", str1, str2)
    for i := 0; i < len(server1); i++ {
        if (server1[i] != server2[i]) {
            changed = true
            return
        }
    }
    changed = false
    return
}
