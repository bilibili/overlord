package proxy

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// Proxy is proxy.
type Proxy struct {
	c *Config

	ctx    context.Context
	cancel context.CancelFunc

	ccs      []*ClusterConfig
	clusters map[string]*Cluster
	once     sync.Once

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
	p.ctx, p.cancel = context.WithCancel(context.Background())
	// pprof
	if c.Pprof != "" {
		go PprofListenAndServe(c.Pprof)
	}
	return
}

// Serve is the main accept() loop of a server.
func (p *Proxy) Serve(ccs []*ClusterConfig) {
	p.once.Do(func() {
		p.ccs = ccs
		p.clusters = map[string]*Cluster{}
		for _, cc := range ccs {
			go p.serve(cc)
		}
	})
}

func (p *Proxy) serve(cc *ClusterConfig) {
	cluster := NewCluster(p.ctx, cc)
	p.lock.Lock()
	p.clusters[cc.Name] = cluster
	p.lock.Unlock()
	// listen
	l, err := Listen(cc.ListenProto, cc.ListenAddr)
	if err != nil {
		panic(err)
	}
	for {
		// TODO(felix): check MaxConnections
		conn, err := l.Accept()
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			continue
		}
		NewHandler(p.ctx, p.c, conn, cluster).Handle()
	}
}

// Close close proxy resource.
func (p *Proxy) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil
	}
	p.cancel()
	for _, cluster := range p.clusters {
		cluster.Close()
	}
	return nil
}
