package dao

import (
	"context"
	"overlord/api/model"
	"overlord/config"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/proto"
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
func (d *Dao) CreateCluster(ctx context.Context, p *model.ParamCluster) (string, error) {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// check if master num is even
	if ctype := proto.CacheType(p.CacheType); ctype == proto.CacheTypeRedisCluster {
		if p.Number%2 != 0 {
			log.Info("cluster master number is odd")
			return "", ErrMasterNumMustBeEven
		}
	}

	err := d.checkClusterName(p.Name)
	if err != nil {
		log.Info("cluster name must be unique")
		return "", err
	}

	err = d.checkVersion(p.Version)
	if err != nil {
		log.Info("version must be exists")
		return "", err
	}

	t, err := d.createCreateClusterJob(p)
	if err != nil {
		log.Infof("create fail due to %s", err)
		return "", err
	}

	return d.saveJob(subctx, t)
}
