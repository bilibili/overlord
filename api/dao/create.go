package dao

import (
	"context"
	"encoding/json"
	"overlord/api/model"
	"overlord/lib/etcd"
	"overlord/proto"
	"overlord/task"
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

func (d *Dao) createCreateClusterTask(p *model.ParamCluster) (*task.Task, error) {
	t := &task.Task{
		OpType:  task.OpCreate,
		Name:    p.Name,
		Version: p.Version,
		Num:     2 * p.MasterNum,
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

func (d *Dao) saveTask(ctx context.Context, t *task.Task) (string, error) {
	var sb strings.Builder
	encoder := json.NewEncoder(&sb)

	err := encoder.Encode(t)
	if err != nil {
		return "", err
	}

	taskID, err := d.e.GenID(ctx, etcd.TaskDir, sb.String())
	if err != nil {
		return "", err
	}

	err = d.e.SetTaskState(ctx, taskID, task.StatePending)
	if err != nil {
		return "", err
	}

	return taskID, nil
}
