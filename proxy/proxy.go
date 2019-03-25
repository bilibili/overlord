package proxy

import (
	errs "errors"
	"net"
	"path/filepath"
	"sort"
	"strings"
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

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
)

// proxy errors
var (
	ErrProxyMoreMaxConns       = errs.New("Proxy accept more than max connextions")
	LoadFailCnt          int32 = 0
	ClusterChangeCount   int32 = 0
)

// Proxy is proxy.
type Proxy struct {
	c               *Config
	ClusterConfFile string // cluster configure file name
	ccs             []*ClusterConfig

	forwarders map[string]proto.Forwarder
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
	p.ccs = ccs
	p.forwarders = map[string]proto.Forwarder{}
	if len(ccs) == 0 {
		log.Warnf("overlord will never listen on any port due to cluster is not specified")
	}
	for _, cc := range ccs {
		p.serve(cc)
	}
	go p.monitorConfChange()
}

func (p *Proxy) serve(cc *ClusterConfig) {
	forwarder := NewForwarder(cc)
	p.lock.Lock()
	p.forwarders[cc.Name] = forwarder
	p.lock.Unlock()

	// listen
	l, err := Listen(cc.ListenProto, cc.ListenAddr)
	if err != nil {
		panic(err)
	}
	log.Infof("overlord proxy cluster[%s] addr(%s) start listening", cc.Name, cc.ListenAddr)
	go p.accept(cc, l, forwarder)
}

func (p *Proxy) accept(cc *ClusterConfig, l net.Listener, forwarder proto.Forwarder) {
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
		NewHandler(p, cc, conn, forwarder).Handle()
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
func (p *Proxy) isClosed() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.closed
}

func (p *Proxy) monitorConfChange() {
	watch, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create file change watcher, get error:%s\n", err.Error())
		return
	}
	defer watch.Close()
	var absPath = ""
	absPath, err = filepath.Abs(filepath.Dir(p.ClusterConfFile))
	if err != nil {
		log.Errorf("failed to get abs path of file:%s, get error:%s\n", p.ClusterConfFile, err.Error())
		return
	}

	err = watch.Add(absPath)
	if err != nil {
		log.Errorf("failed to monitor content change of dir:%s with error:%s\n", absPath, err.Error())
		return
	}
	for {
		if p.isClosed() {
			log.Infof("proxy is closed, exit configure file monitor\n")
			return
		}
		select {
		case ev := <-watch.Events:
			{
				if ev.Op&fsnotify.Create == fsnotify.Create {
					log.Infof("find new created file:%s\n", ev.Name)
					p.handleConfigChange()
				}
				if ev.Op&fsnotify.Write == fsnotify.Write {
					log.Infof("find content of file:%s updated\n", ev.Name)
					p.handleConfigChange()
				}
				if ev.Op&fsnotify.Remove == fsnotify.Remove {
					log.Infof("find file:%s removed", ev.Name)
				}
				if ev.Op&fsnotify.Rename == fsnotify.Rename {
					log.Infof("find file:%s renamed", ev.Name)
					p.handleConfigChange()
				}
				if ev.Op&fsnotify.Chmod == fsnotify.Chmod {
					log.Infof("find mod of conf file:%s changed", ev.Name)
				}
			}
		case err := <-watch.Errors:
			{
				log.Errorf("watch dir:%s get error:%s\n", absPath, err.Error())
				return
			}
		}
	}
}

func (p *Proxy) handleConfigChange() {
	var newConfs, err = LoadClusterConf(p.ClusterConfFile)
	if err != nil {
		log.Errorf("failed to load conf file:%s, got error:%s\n", p.ClusterConfFile, err.Error())
		atomic.AddInt32(&LoadFailCnt, 1)
		return
	}
	var changedConf []*ClusterConfig
	changedConf = p.parseChanged(newConfs, p.ccs)

	for _, conf := range changedConf {
		var err = p.updateConfig(conf)
		if err == nil {
			atomic.AddInt32(&ClusterChangeCount, 1)
		}
		log.Infof("update conf of cluster:%s\n", conf.Name)
	}
}

func (p *Proxy) updateConfig(conf *ClusterConfig) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	var f, ok = p.forwarders[conf.Name]
	if !ok {
		return errors.New("Forwarder:" + conf.Name + " is not found when update cluster config")
	}
	var err = f.Update(conf.Servers)
	if err != nil {
		log.Errorf("failed to update conf of cluster:%s with error:%s\n", conf.Name, err.Error())
		return err
	}
	for index, oldConf := range p.ccs {
		if oldConf.Name != conf.Name {
			continue
		}
		p.ccs[index].Servers = conf.Servers
	}
	return nil
}

func (p *Proxy) supportChange(conf *ClusterConfig) bool {
	if conf.CacheType == types.CacheTypeRedisCluster {
		return false
	}
	return true
}

func (p *Proxy) parseChanged(newConfs, oldConfs []*ClusterConfig) []*ClusterConfig {
	var changed = make([]*ClusterConfig, 0, len(oldConfs))
	for _, newConf := range newConfs {
		if !p.supportChange(newConf) {
			continue
		}
		var find = false
		var diff = false
		for _, conf := range oldConfs {
			if newConf.Name != conf.Name {
				continue
			}
			if !p.supportChange(conf) {
				find = false
				break
			}
			find = true
			diff = compareConf(conf, newConf)
			break
		}
		if !find || !diff {
			continue
		}
		changed = append(changed, newConf)
		continue
	}
	return changed
}

func compareConf(oldConf, newConf *ClusterConfig) bool {
	if len(oldConf.Servers) != len(newConf.Servers) {
		return true
	}
	var server1 = oldConf.Servers
	var server2 = newConf.Servers
	sort.Strings(server1)
	sort.Strings(server2)
	var str1 = strings.Join(server1, "_")
	var str2 = strings.Join(server2, "_")
	if str1 != str2 {
		return true
	}
	return false
}
