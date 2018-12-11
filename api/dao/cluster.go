package dao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"overlord/api/model"
	"overlord/job"
	"overlord/job/balance"
	"overlord/job/create"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/proto"
	"strconv"
	"strings"

	"path/filepath"

	"go.etcd.io/etcd/client"
)

// define cluster errors
var (
	ErrClusterAssigned = errors.New("cluster has be assigned with some appids")
)

// ScaleCluster scale the given cluster
func (d *Dao) ScaleCluster(ctx context.Context, p *model.ParamScale) (jobID string, err error) {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		val  string
		info *create.CacheInfo
	)

	val, err = d.e.Get(sub, fmt.Sprintf("%s/%s/info", etcd.ClusterDir, p.Name))
	if err != nil {
		return
	}

	de := json.NewDecoder(strings.NewReader(val))
	err = de.Decode(info)
	if err != nil {
		return
	}

	j := &job.Job{
		Name:   p.Name,
		Num:    p.Number,
		OpType: job.OpScale,
	}
	return d.saveJob(sub, j)
}

// GetCluster will search clusters by given cluster name
func (d *Dao) GetCluster(ctx context.Context, cname string) (*model.Cluster, error) {
	istr, err := d.e.ClusterInfo(ctx, cname)
	if err != nil {
		return nil, err
	}
	info := &create.CacheInfo{}
	de := json.NewDecoder(strings.NewReader(istr))
	err = de.Decode(info)
	if err != nil {
		return nil, err
	}

	clusterState, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/state", etcd.JobDetailDir, info.JobID))
	if err != nil {
		clusterState = model.StateError
	}

	if clusterState == job.StateDone {
		clusterState = model.StateDone
	} else if clusterState == job.StateLost || clusterState == job.StateFail {
		clusterState = model.StateError
	} else {
		clusterState = model.StateWaiting
	}

	nodes, err := d.e.LS(ctx, fmt.Sprintf("%s/%s/instances/", etcd.ClusterDir, cname))
	if err != nil && !client.IsKeyNotFound(err) {
		return nil, err
	}
	instances := []*model.Instance{}
	for _, node := range nodes {
		vsp := strings.Split(node.Value, ":")
		val, err := strconv.ParseInt(vsp[1], 10, 64)
		if err != nil {
			val = -1
		}

		inst := &model.Instance{
			IP:     vsp[0],
			Port:   int(val),
			State:  clusterState,
			Weight: -1,
		}

		if info.CacheType != proto.CacheTypeRedisCluster {
			alias, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/alias", etcd.InstanceDirPrefix, node.Value))
			if err != nil {
				continue
			}
			inst.Alias = alias

			weight, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/weight", etcd.InstanceDirPrefix, node.Value))
			if err != nil {
				continue
			}
			w, err := strconv.ParseInt(weight, 10, 64)
			if err != nil {
				continue
			}
			inst.Weight = int(w)
		}
		instances = append(instances, inst)
	}

	c := &model.Cluster{
		Name:      info.Name,
		CacheType: string(info.CacheType),
		MaxMemory: info.MaxMemory,
		Thread:    info.Thread,
		Version:   info.Version,
		Number:    info.Number,
		State:     clusterState,
		Instances: instances,
		Group:     info.Group,
		Monitor:   d.m.Href(info.Name),
	}

	return c, nil
}

// GetClusters will get all clusters
func (d *Dao) GetClusters(ctx context.Context, name string) (clusters []*model.Cluster, err error) {
	var nodes []*etcd.Node
	nodes, err = d.e.LS(ctx, etcd.ClusterDir)
	clusters = make([]*model.Cluster, 0)
	var cluster *model.Cluster
	for _, node := range nodes {
		_, cname := filepath.Split(node.Key)
		if !strings.Contains(cname, name) {
			continue
		}
		cluster, err = d.GetCluster(ctx, cname)
		if err != nil {
			log.Error("GetClusters.GetCluster err %s", err)
			continue
		}
		clusters = append(clusters, cluster)
	}
	return
}

// RemoveCluster will check if the cluster is assigned to anyone appids and
// remove the unassigned one.
func (d *Dao) RemoveCluster(ctx context.Context, cname string) (jobid string, err error) {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()
	var nodes []*etcd.Node
	nodes, err = d.e.LS(sub, fmt.Sprintf("%s/%s/instances", etcd.ClusterDir, cname))
	if err != nil && !client.IsKeyNotFound(err) {
		return
	}

	if len(nodes) > 0 {
		err = ErrClusterAssigned
		return
	}
	j := d.createDestroyClusterJob(ctx, cname)
	jobid, err = d.saveJob(ctx, j)
	return
}

// CreateCluster will create new cluster
func (d *Dao) CreateCluster(ctx context.Context, p *model.ParamCluster) (string, error) {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()

	specCPU, specMaxMem, err := d.parseSpecification(p.Spec)
	if err != nil {
		return "", err
	}
	p.SpecCPU = specCPU
	p.SpecMemory = specMaxMem

	number := p.TotalMemory / int(p.SpecMemory)

	// check if master num is even
	ctype := proto.CacheType(p.CacheType)
	if ctype == proto.CacheTypeRedisCluster && number%2 != 0 {
		number++
	}
	p.Number = number

	err = d.checkClusterName(p.Name)
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

	taskID, err := d.saveJob(subctx, t)
	if err != nil {
		return taskID, err
	}

	err = d.assignAppids(subctx, p.Name, p.Appids...)
	return taskID, err
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
	if strings.HasSuffix(ssp[1], "m") {
		maxMem, err = strconv.ParseFloat(strings.TrimRight(ssp[1], "m"), 64)
	} else if strings.HasSuffix(ssp[1], "g") {
		maxMem, err = strconv.ParseFloat(strings.TrimRight(ssp[1], "m"), 64)
		maxMem = maxMem * 1024.0
	}
	return
}

func (d *Dao) createCreateClusterJob(p *model.ParamCluster) (*job.Job, error) {
	t := &job.Job{
		OpType:  job.OpCreate,
		Name:    p.Name,
		Version: p.Version,
		Num:     p.Number,
		Group:   p.Group,
	}

	cacheType, err := d.mapCacheType(p.CacheType)
	if err != nil {
		return nil, err
	}
	t.CacheType = cacheType

	t.MaxMem = p.SpecMemory
	t.CPU = p.SpecCPU

	return t, nil
}

func (d *Dao) saveJob(ctx context.Context, t *job.Job) (string, error) {
	var sb strings.Builder
	encoder := json.NewEncoder(&sb)

	err := encoder.Encode(t)
	if err != nil {
		return "", err
	}

	jobID, err := d.e.GenID(ctx, fmt.Sprintf("%s/%s/", etcd.JobsDir, t.Group), sb.String())
	if err != nil {
		return "", err
	}

	err = d.e.SetJobState(ctx, t.Group, jobID, job.StatePending)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.%s", t.Group, jobID), nil
}

func (d *Dao) unassignAppids(ctx context.Context, cluster string, appids ...string) (err error) {
	for _, appid := range appids {
		err = d.e.Delete(ctx, fmt.Sprintf("%s/%s/appids/%s", etcd.ClusterDir, cluster, appid))
		if err != nil {
			return
		}
		var nodes []*etcd.Node
		nodes, err = d.e.LS(ctx, fmt.Sprintf("%s/%s", etcd.AppidsDir, appid))
		for _, node := range nodes {
			if node.Value == cluster {
				err = d.e.Delete(ctx, node.Key)
				if err != nil && !client.IsKeyNotFound(err) {
					return
				}
			}
		}
	}
	return
}

func (d *Dao) assignAppids(ctx context.Context, cluster string, appids ...string) (err error) {
	for _, appid := range appids {
		err = d.e.Set(ctx, fmt.Sprintf("%s/%s/appids/%s", etcd.ClusterDir, cluster, appid), "")
		if err != nil {
			return
		}

		_, err = d.e.GenID(ctx, fmt.Sprintf("%s/%s/", etcd.AppidsDir, appid), cluster)
		if err != nil {
			return
		}
	}

	return
}

// createDestroyClusterJob will create remove cluster job.
func (d *Dao) createDestroyClusterJob(ctx context.Context, cname string) (j *job.Job) {
	j = &job.Job{
		OpType: job.OpDestroy,
		Name:   cname,
	}
	return
}
