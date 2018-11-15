package dao

import (
	"context"
	"encoding/json"
	"overlord/api/model"
	"overlord/job"
	"overlord/job/balance"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/proto"
	"strconv"
	"strings"
)

func (d *Dao) checkVersion(version string) error {
	return nil
}

func (d *Dao) checkClusterName(cname string) error {
	return nil
}

func (d *Dao) mapCacheType(cacheType string) (proto.CacheType, error) {
	ct := proto.CacheType(cacheType)
	if ct != proto.CacheTypeMemcache && ct != proto.CacheTypeRedis && ct != proto.CacheTypeRedisCluster {
		return ct, ErrCacheTypeNotSupport
	}

	return ct, nil
}

func (d *Dao) parseSpecification(spec string) (cpu float64, maxMem float64, err error) {
	ssp := strings.SplitN(spec, "c", 2)
	cpu, err = strconv.ParseFloat(ssp[0], 64)
	if err != nil {
		return
	}
	maxMem, err = strconv.ParseFloat(strings.TrimRight(ssp[1], "m"), 64)
	return
}

func (d *Dao) createCreateClusterJob(p *model.ParamCluster) (*job.Job, error) {
	t := &job.Job{
		OpType:  job.OpCreate,
		Name:    p.Name,
		Version: p.Version,
		Num:     p.Number,
	}

	cacheType, err := d.mapCacheType(p.CacheType)
	if err != nil {
		return nil, err
	}
	t.CacheType = cacheType

	specCPU, specMaxMem, err := d.parseSpecification(p.Spec)
	if err != nil {
		return nil, err
	}

	t.MaxMem = specMaxMem
	t.CPU = specCPU

	return t, nil
}

func (d *Dao) saveJob(ctx context.Context, t *job.Job) (string, error) {
	var sb strings.Builder
	encoder := json.NewEncoder(&sb)

	err := encoder.Encode(t)
	if err != nil {
		return "", err
	}

	jobID, err := d.e.GenID(ctx, etcd.JobsDir, sb.String())
	if err != nil {
		return "", err
	}

	err = d.e.SetJobState(ctx, jobID, job.StatePending)
	if err != nil {
		return "", err
	}

	return jobID, nil
}

// CreateCluster will create new cluster
func (d *Dao) CreateCluster(ctx context.Context, p *model.ParamCluster) (string, error) {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// check if master num is even
	ctype := proto.CacheType(p.CacheType)
	if ctype == proto.CacheTypeRedisCluster {
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

	// TODO: move it into mesos framework task
	if ctype == proto.CacheTypeRedisCluster {
		go func(name string) {
			err := balance.Balance(p.Name, d.e)
			if err != nil {
				log.Errorf("fail to balance cluster %s due to %v", name, err)
			}
		}(p.Name)
	}

	return d.saveJob(subctx, t)
}
