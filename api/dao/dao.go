package dao

import (
	"context"
	"overlord/api/model"
	"overlord/config"
	"overlord/lib/etcd"

	"github.com/pkg/errors"
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

// CreateCluster will create new cluster
func (d *Dao) CreateCluster(ctx context.Context, p *model.ParamCluster) (int64, error) {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// check if master num is even
	if p.MasterNum%2 == 0 {
		return -1, ErrMasterNumMustBeEven
	}

	err := d.checkClusterName(p.Name)
	if err != nil {
		return -1, errors.WithStack(err)
	}

	err = d.checkVersion(p.Version)
	if err != nil {
		return -1, errors.WithStack(err)
	}

	t, err := d.createCreateClusterTask(p)
	if err != nil {
		return -1, err
	}

	return d.saveTask(subctx, t)
}
