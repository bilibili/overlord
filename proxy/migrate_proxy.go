package proxy

import "overlord/proxy/proto"

// Proxy is proxy.
// type Proxy struct {
// 	c   *Config
// 	ccs []*ClusterConfig

// 	forwarders map[string]proto.Forwarder
// 	once       sync.Once

// 	conns int32

// 	lock   sync.Mutex
// 	closed bool
// }

// NewMigrate build a proxy for migrate usage
func NewMigrate() (p *Proxy, err error) {
	p = &Proxy{
		ccs:        []*ClusterConfig{},
		forwarders: make(map[string]proto.Forwarder),
		closed:     false,
	}
	return
}

// StartForward will create and handle new forwarder
func (p *Proxy) StartForward(cc *ClusterConfig) {
	f := NewForwarder(cc)
	p.lock.Lock()
	p.ccs = append(p.ccs, cc)
	p.forwarders[cc.Name] = f
	p.lock.Unlock()

	NewMigrateHandler(p, cc, f).Handle()
}
