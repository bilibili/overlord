package proxy

import (
	"log"

	"github.com/pkg/errors"
)

// Proxy is proxy.
type Proxy struct {
	c *Config
}

// New new a proxy by config.
func New(c *Config) (p *Proxy, err error) {
	if err = c.Validate(); err != nil {
		err = errors.Wrap(err, "Proxy New config validate error")
		return
	}
	p = &Proxy{}
	p.c = c
	// TODO(felix)
	return
}

// Serve is the main accept() loop of a server.
func (p *Proxy) Serve() {
	l, err := Listen(p.c.Proxy.Proto, p.c.Proxy.Addr)
	if err != nil {
		panic(err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Proxy Serve Error accepting connection from remote:", err.Error())
			if conn != nil {
				conn.Close()
			}
			continue
		}
		// TODO(felix): deal conn
	}
}

// Close close proxy resource.
func (p *Proxy) Close() {

}
