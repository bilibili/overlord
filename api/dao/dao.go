package dao

import (
	"context"
	"encoding/json"
	"overlord/api/model"
	"overlord/lib/etcd"
	"overlord/proto"
	"overlord/task"
	"strings"

	"strconv"

	"github.com/pkg/errors"
)

// define errors
var (
	ErrMasterNumMustBeEven = errors.New("master number must be even")
	ErrCacheTypeNotSupport = errors.New("cache type only support memcache|redis|redis_cluster")
)

// Dao is the dao level abstraction
type Dao struct {
	e *etcd.Etcd
}

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
	maxMem, err = strconv.ParseFloat(strings.TrimRight(spec, "gmk"), 64)
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
		return nil, errors.WithStack(err)
	}
	t.CacheType = cacheType

	specCPU, specMaxMem, err := d.parseSpecification(p.Spec)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t.MaxMem = specMaxMem
	t.CPU = specCPU

	return t, nil
}

func (d *Dao) saveTask(ctx context.Context, t *task.Task) (int64, error) {
	var sb strings.Builder
	encoder := json.NewEncoder(&sb)

	err := encoder.Encode(t)
	if err != nil {
		return -1, errors.WithStack(err)
	}

	taskIDStr, err := d.e.GenID(ctx, etcd.TASKDIR, sb.String())
	if err != nil {
		return -1, errors.WithStack(err)
	}

	taskID, err := strconv.ParseInt(taskIDStr, 10, 64)
	if err != nil {
		// TODO: should we delete created task ?
		return -1, errors.WithStack(err)
	}
	return taskID, nil
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
