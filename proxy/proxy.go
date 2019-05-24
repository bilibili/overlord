package proxy

import (
	errs "errors"
	"net"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"overlord/pkg/log"
	libnet "overlord/pkg/net"
	"overlord/pkg/prom"
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
	ErrProxyMoreMaxConns = errs.New("Proxy accept more than max connextions")
	ErrProxyReloadIgnore = errs.New("Proxy reload cluster config is ignored")
	ErrProxyReloadFail   = errs.New("Proxy reload cluster config is failed")
)

// Proxy is proxy.
type Proxy struct {
	c   *Config
	ccf string // cluster configure file name
	ccs []*ClusterConfig

	forwarders map[string]proto.Forwarder
	lock       sync.Mutex

	conns int32

	closed bool
}

// New new a proxy by config.
func New(c *Config) (p *Proxy, err error) {
	if err = c.Validate(); err != nil {
		err = errors.WithStack(err)
		return
	}
	p = &Proxy{}
	p.c = c
	return
}

// Serve is the main accept() loop of a server.
func (p *Proxy) Serve(ccs []*ClusterConfig) {
	p.ccs = ccs
	if len(ccs) == 0 {
		log.Warnf("overlord will never listen on any port due to cluster is not specified")
	}
	p.lock.Lock()
	p.forwarders = map[string]proto.Forwarder{}
	p.lock.Unlock()
	for _, cc := range ccs {
		p.serve(cc)
	}
}

func (p *Proxy) serve(cc *ClusterConfig) {
	forwarder := NewForwarder(cc)
	p.forwarders[cc.Name] = forwarder
	// listen
	l, err := Listen(cc.ListenProto, cc.ListenAddr)
	if err != nil {
		panic(err)
	}
	log.Infof("overlord proxy cluster[%s] addr(%s) start listening", cc.Name, cc.ListenAddr)
	if cc.SlowlogSlowerThan != 0 {
		log.Infof("overlord start slowlog to [%s] with threshold [%d]us", cc.Name, cc.SlowlogSlowerThan)
	}
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
	if p.closed {
		return nil
	}
	for _, forwarder := range p.forwarders {
		forwarder.Close()
	}
	p.closed = true
	return nil
}

// MonitorConfChange reload servers.
func (p *Proxy) MonitorConfChange(ccf string) {
	p.ccf = ccf
	// start watcher
	watch, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create file change watcher and get error:%v", err)
		return
	}
	defer watch.Close()
	absPath, err := filepath.Abs(filepath.Dir(p.ccf))
	if err != nil {
		log.Errorf("failed to get abs path of file:%s and get error:%v", p.ccf, err)
		return
	}
	if err = watch.Add(absPath); err != nil {
		log.Errorf("failed to monitor content change of dir:%s with error:%v", absPath, err)
		return
	}
	log.Infof("proxy is watching changes cluster config absolute path as %s", absPath)
	for {
		if p.closed {
			log.Infof("proxy is closed and exit configure file:%s monitor", p.ccf)
			return
		}
		select {
		case ev := <-watch.Events:
			if ev.Op&fsnotify.Create == fsnotify.Create || ev.Op&fsnotify.Write == fsnotify.Write || ev.Op&fsnotify.Rename == fsnotify.Rename {
				newConfs, err := LoadClusterConf(p.ccf)
				if err != nil {
					prom.ErrIncr(p.ccf, p.ccf, "config reload", err.Error())
					log.Errorf("failed to load conf file:%s and got error:%v", p.ccf, err)
					continue
				}
				changed := parseChanged(newConfs, p.ccs)
				for _, conf := range changed {
					if err = p.updateConfig(conf); err == nil {
						log.Infof("reload successful cluster:%s config succeed", conf.Name)
					} else {
						prom.ErrIncr(conf.Name, conf.Name, "cluster reload", err.Error())
						log.Errorf("reload failed cluster:%s config and get error:%v", conf.Name, err)
					}
				}
				log.Infof("watcher file:%s occurs event:%s and reload finish", ev.Name, ev.String())
				continue
			}
			if log.V(5) {
				log.Infof("watcher file:%s occurs event:%s and ignore", ev.Name, ev.String())
			}
		case err := <-watch.Errors:
			log.Errorf("watcher dir:%s get error:%v", absPath, err)
			return
		}
	}
}

func (p *Proxy) updateConfig(conf *ClusterConfig) (err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	f, ok := p.forwarders[conf.Name]
	if !ok {
		err = errors.Wrapf(ErrProxyReloadIgnore, "cluster:%s", conf.Name)
		return
	}
	if err = f.Update(conf.Servers); err != nil {
		err = errors.Wrapf(ErrProxyReloadFail, "cluster:%s error:%v", conf.Name, err)
		return
	}
	for _, oldConf := range p.ccs {
		if oldConf.Name != conf.Name {
			continue
		}
		oldConf.Servers = make([]string, len(conf.Servers), cap(conf.Servers))
		copy(oldConf.Servers, conf.Servers)
		return
	}
	return
}

func parseChanged(newConfs, oldConfs []*ClusterConfig) (changed []*ClusterConfig) {
	changed = make([]*ClusterConfig, 0, len(oldConfs))
	for _, newConf := range newConfs {
		for _, oldConf := range oldConfs {
			if newConf.Name != oldConf.Name || len(newConf.Servers) != len(oldConf.Servers) {
				continue
			}
			sort.Strings(newConf.Servers)
			sort.Strings(oldConf.Servers)
			if !reflect.DeepEqual(newConf.Servers, oldConf.Servers) {
				changed = append(changed, newConf)
			}
			break
		}
	}
	return
}
