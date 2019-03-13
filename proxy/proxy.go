package proxy

import (
	errs "errors"
	"fmt"
	"net"
	"sort"
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
	ErrProxyMoreMaxConns              = errs.New("Proxy accept more than max connextions")
	ErrProxyConfChanged               = errs.New("Proxy configure is changed, need to reconnect")
	ClusterID                   int32 = 0
	MonitorCfgIntervalMilliSecs int   = 50 * 1000 // Time interval to monitor config change
	ClusterChangeCount          int32 = 0
	ClusterConfChangeFailCnt    int32 = 0
	AddClusterFailCnt           int32 = 0
	LoadFailCnt                 int32 = 0
	FailedDueToRemovedCnt       int32 = 0
)

const (
	MaxClusterCnt  int32 = 128
	ClusterRunning int32 = 0
	ClusterStopped int32 = 1
)

type Cluster struct {
	conf         *ClusterConfig
	clientConns  map[int64]*Handler
	forwarder    proto.Forwarder
	mutex        sync.Mutex
	proxy        *Proxy
	state        int32
	connectionSN int64
}

// Proxy is proxy.
type Proxy struct {
	c               *Config
	ClusterConfFile string // cluster configure file name

	clusters      map[string]*Cluster
	CurClusterCnt int32
	once          sync.Once

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
	p.clusters = make(map[string]*Cluster)
	p.c = c
	return
}

func genClusterID() int32 {
	var id = atomic.AddInt32(&ClusterID, 1)
	return id
}

// Serve is the main accept() loop of a server.
func (p *Proxy) Serve(ccs []*ClusterConfig) {
	p.once.Do(func() {
		if len(ccs) == 0 {
			log.Warnf("overlord will never listen on any port due to cluster is not specified")
		}
		p.CurClusterCnt = 0
		for _, conf := range ccs {
			var err = p.addCluster(conf)
			if err != nil {
				// it is safety to panic here as it in start up logic
				panic(err)
			}
		}
		// Here, start a go routine to change content of a forwarder
		go p.monitorConfChange()
	})
}

func (p *Proxy) incConnectionCnt() (int32, bool) {
	if p.c.Proxy.MaxConnections <= 0 {
		return 0, true
	}
	var conn = atomic.LoadInt32(&p.conns)
	if conn > p.c.Proxy.MaxConnections {
		return conn, false
	}
	atomic.AddInt32(&p.conns, 1)
	return conn, true
}
func (p *Proxy) descConnectionCnt() {
	if p.c.Proxy.MaxConnections <= 0 {
		return
	}
	atomic.AddInt32(&p.conns, -1)
}

func (p *Proxy) addCluster(newConf *ClusterConfig) error {
	newConf.ID = genClusterID()
	p.lock.Lock()
	var newForwarder, err = NewForwarder(newConf)
	if err != nil {
		p.lock.Unlock()
		return err
	}
	// refs is incread in NewFowarder
	var cluster = &Cluster{conf: newConf, forwarder: newForwarder, proxy: p, connectionSN: 0}
	cluster.clientConns = make(map[int64]*Handler)
	p.clusters[cluster.conf.Name] = cluster
	var servErr = cluster.serve()
	if servErr != nil {
		delete(p.clusters, cluster.conf.Name)
		p.lock.Unlock()
		cluster.Close()
		cluster.forwarder = nil
		newForwarder.Release()
		return servErr
	}
	p.CurClusterCnt++
	p.lock.Unlock()
	log.Infof("succeed to add cluster:%s with addr:%s\n", newConf.Name, newConf.ListenAddr)
	return nil
}

func (c *Cluster) serve() error {
	// listen
	atomic.StoreInt32(&c.state, ClusterRunning)
	var conf = c.getConf()
	l, err := Listen(conf.ListenProto, conf.ListenAddr)
	if err != nil {
		log.Errorf("failed to listen on address:%s, got error:%s\n", conf.ListenAddr, err.Error())
		return err
	}
	log.Infof("overlord proxy cluster[%s] addr(%s) start listening", conf.Name, conf.ListenAddr)
	go c.accept(l)
	return nil
}

func (c *Cluster) accept(l net.Listener) {
	for {
		var conf = c.getConf()
		if atomic.LoadInt32(&c.state) != ClusterRunning {
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
		var newConf = c.getConf()
		if newConf.ID != conf.ID {
			_ = conn.Close()
			log.Errorf("cluster:%s conf is just changed from:%d to %d, close client conn and let client retry\n", conf.Name, conf.ID, newConf.ID)
			continue
		}
		if cnt, succ := c.proxy.incConnectionCnt(); !succ {
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
				var f = c.getForwarder()
				_ = encoder.Encode(proto.ErrMessage(ErrProxyMoreMaxConns), f)
				_ = encoder.Flush()
				f.Release()
			}
			_ = conn.Close()
			if log.V(4) {
				log.Warnf("proxy reject connection count(%d) due to more than max(%d)", cnt, c.proxy.c.Proxy.MaxConnections)
			}
			fmt.Printf("Exceed max connection limit\n")
			continue
		}
		var frontConn = libnet.NewConn(conn, time.Second*time.Duration(c.proxy.c.Proxy.ReadTimeout), time.Second*time.Duration(c.proxy.c.Proxy.WriteTimeout))
		var id = atomic.AddInt64(&c.connectionSN, 1)
		var handler = NewHandler(c, conf, id, frontConn)
		c.addConnection(id, handler)
		handler.Handle()
	}
}

// Close close proxy resource.
func (p *Proxy) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	for _, cluster := range p.clusters {
		cluster.Close()
	}
	return nil
}

func (p *Proxy) anyClusterRemoved(newConfs, oldConfs []*ClusterConfig) bool {
	var (
		newNames = make(map[string]int)
		oldNames = make(map[string]int)
	)
	for _, conf := range newConfs {
		newNames[conf.Name] = 1
	}
	for _, conf := range oldConfs {
		oldNames[conf.Name] = 1
	}
	for name, _ := range oldNames {
		_, find := newNames[name]
		if !find {
			return true
		}
	}
	return false
}

func (p *Proxy) parseChanged(newConfs, oldConfs []*ClusterConfig) (changed, newAdd []*ClusterConfig) {
	for _, newConf := range newConfs {
		var find = false
		var diff = false
		var valid = true
		for _, conf := range oldConfs {
			if newConf.Name != conf.Name {
				// log.Infof("confname is different, %s VS %s\n", newConf.Name, conf.Name)
				continue
			}
			find = true
			diff, valid = compareConf(conf, newConf)
			if !valid {
				log.Errorf("configure change of cluster(%s) is invalid", newConf.Name)
				break
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
		changed = append(changed, newConf)
		continue
	}
	// log.Infof("new conf len:%d old conf len:%d\n", len(newConfs), len(oldConfs))
	return changed, newAdd
}

func (p *Proxy) monitorConfChange() {
	for {
		time.Sleep(time.Duration(MonitorCfgIntervalMilliSecs) * time.Millisecond)
		var newConfs, err = LoadClusterConf(p.ClusterConfFile)
		if err != nil {
			log.Errorf("failed to load conf file:%s, got error:%s\n", p.ClusterConfFile, err.Error())
			atomic.AddInt32(&LoadFailCnt, 1)
			continue
		}
		var oldConfs []*ClusterConfig
		for _, cluster := range p.clusters {
			var conf = cluster.getConf()
			oldConfs = append(oldConfs, conf)
		}
		var removed = p.anyClusterRemoved(newConfs, oldConfs)
		if removed {
			log.Errorf("some cluster is removed from conf file, ignore this change")
			atomic.AddInt32(&FailedDueToRemovedCnt, 1)
			continue
		}

		var newAdd []*ClusterConfig
		var changedConf []*ClusterConfig
		changedConf, newAdd = p.parseChanged(newConfs, oldConfs)

		var clusterCnt = p.CurClusterCnt + int32(len(newAdd))

		if clusterCnt > MaxClusterCnt {
			log.Errorf("failed to reload conf as too much cluster will be added, new cluster count(%d) and max count(%d)",
				clusterCnt, MaxClusterCnt)
			continue
		}
		for _, conf := range changedConf {
			// use new forwarder now
			var err = p.clusters[conf.Name].processConfChange(conf)
			if err == nil {
				atomic.AddInt32(&ClusterChangeCount, 1)
				log.Infof("succeed to change conf of cluster:%s\n", conf.Name)
				continue
			}
			atomic.AddInt32(&ClusterConfChangeFailCnt, 1)
			log.Errorf("failed to change conf of cluster(%s), got error:%s\n", conf.Name, err.Error())
		}
		for _, conf := range newAdd {
			var err = p.addCluster(conf)
			if err != nil {
				atomic.AddInt32(&AddClusterFailCnt, 1)
				log.Errorf("failed to add new cluster:%s, got error:%s\n", conf.Name, err.Error())
				continue
			}
			log.Infof("succeed to add new cluster:%s", conf.Name)
		}
	}
}

func (c *Cluster) Close() {
	log.Infof("start to close all client connections of cluster:%s\n", c.conf.Name)
	atomic.StoreInt32(&c.state, ClusterStopped)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.forwarder.Close()

	var curConns = c.clientConns
	c.clientConns = make(map[int64]*Handler)
	for _, conn := range curConns {
		conn.Close()
	}

}

func (c *Cluster) addConnection(id int64, conn *Handler) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.clientConns[conn.ID] = conn
	return nil
}

func (c *Cluster) removeConnection(id int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.clientConns, id)
}

func (c *Cluster) processConfChange(newConf *ClusterConfig) error {
	if newConf.Name != c.conf.Name {
		return errors.New("invalid Cluster conf, name:" + newConf.Name + " not equal with old:" + c.conf.Name)
	}
	newConf.ID = genClusterID()
	var newForwarder, err = NewForwarder(newConf)
	if err != nil {
		return err
	}
	// refs of new forwarder is increased in NewForwarder
	c.mutex.Lock()
	if atomic.LoadInt32(&c.state) != ClusterRunning {
		c.mutex.Unlock()
		return errors.New("cluster:" + newConf.Name + " is stopped, no need to reload conf")
	}
	var oldConns = c.clientConns
	var oldForwarder = c.forwarder
	c.forwarder = newForwarder
	if newConf.CloseWhenChange {
		c.clientConns = make(map[int64]*Handler)
	}
	c.conf = newConf
	c.mutex.Unlock()
	oldForwarder.Stop()
	oldForwarder.Release()
	if newConf.CloseWhenChange {
		for _, conn := range oldConns {
			conn.Close()
		}
	}
	return nil
}

func (c *Cluster) getForwarder() proto.Forwarder {
	c.mutex.Lock()
	var f = c.forwarder
	c.mutex.Unlock()
	f.AddRef()
	return f
}

func (c *Cluster) getConf() *ClusterConfig {
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
	if len(oldConf.Servers) != len(newConf.Servers) {
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
		if server1[i] != server2[i] {
			changed = true
			return
		}
	}
	changed = false
	return
}
