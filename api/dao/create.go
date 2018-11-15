package dao

import (
	"context"
	"encoding/json"
	"overlord/api/model"
	"overlord/job"
	"overlord/lib/etcd"
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

	jobID, err := d.e.GenID(ctx, etcd.JobDir, sb.String())
	if err != nil {
		return "", err
	}

	err = d.e.SetJobState(ctx, jobID, job.StatePending)
	if err != nil {
		return "", err
	}

	return jobID, nil
}
