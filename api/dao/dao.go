package dao

import (
	"overlord/config"
	"overlord/lib/etcd"
)

// New create new dao layer
func New(cfg *config.ServerConfig) *Dao {
	e, err := etcd.New(cfg.Etcd)
	if err != nil {
		panic(err)
	}

	d := &Dao{e: e, m: cfg.Monitor, c: cfg.Cluster}
	return d
}

// Dao is the dao level abstraction
type Dao struct {
	e *etcd.Etcd
	m *config.MonitorConfig
	c *config.DefaultClusterConfig
}

func (d *Dao) ETCD() *etcd.Etcd {
	return d.e
}
