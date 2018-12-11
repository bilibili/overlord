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

	d := &Dao{e: e}
	return d
}

// Dao is the dao level abstraction
type Dao struct {
	e *etcd.Etcd
}

func (d *Dao) ETCD() *etcd.Etcd {
	return d.e
}
