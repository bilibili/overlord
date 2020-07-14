package dao

import (
	"overlord/pkg/etcd"
	"overlord/platform/api/model"
)

// New create new dao layer
func New(cfg *model.ServerConfig) *Dao {
	e, err := etcd.New(cfg.Etcd)
	if err != nil {
		panic(err)
	}

	d := &Dao{e: e, m: cfg.Monitor, c: cfg.Cluster, vs: cfg.Versions}
	return d
}

// Dao is the dao level abstraction
type Dao struct {
	e  *etcd.Etcd
	m  *model.MonitorConfig
	c  *model.DefaultClusterConfig
	vs []*model.VersionConfig
}

func (d *Dao) ETCD() *etcd.Etcd {
	return d.e
}
