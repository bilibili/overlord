package dao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"overlord/pkg/etcd"
	"overlord/pkg/log"
	"overlord/pkg/types"
	"overlord/platform/api/model"
	"overlord/platform/job"
	"overlord/platform/job/create"

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

func (d *Dao) getClusterInstances(ctx context.Context, info *create.CacheInfo, state string) ([]*model.Instance, error) {
	nodes, err := d.e.LS(ctx, fmt.Sprintf("%s/%s/instances/", etcd.ClusterDir, info.Name))
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
			State:  state,
			Weight: -1,
		}

		if info.CacheType != types.CacheTypeRedisCluster {
			alias, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/alias", etcd.InstanceDirPrefix, node.Value))
			if err != nil && !client.IsKeyNotFound(err) {
				return nil, err
			}
			inst.Alias = alias

			weight, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/weight", etcd.InstanceDirPrefix, node.Value))
			if err != nil && !client.IsKeyNotFound(err) {
				return nil, err
			}
			w, err := strconv.ParseInt(weight, 10, 64)
			if err != nil && !client.IsKeyNotFound(err) {
				return nil, err
			}
			inst.Weight = int(w)
		} else {
			role, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/role", etcd.InstanceDirPrefix, node.Value))
			if err != nil && !client.IsKeyNotFound(err) {
				return nil, err
			}
			inst.Role = role
		}

		state, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/state", etcd.InstanceDirPrefix, node.Value))
		if err != nil {
			continue
		}
		inst.State = state
		instances = append(instances, inst)
	}
	return instances, nil
}

func (d *Dao) getClusterAppids(ctx context.Context, cname string) ([]string, error) {
	nodes, err := d.e.LS(ctx, fmt.Sprintf("%s/%s/appids", etcd.ClusterDir, cname))
	if client.IsKeyNotFound(err) {
		return []string{}, nil
	} else if err != nil {
		return nil, err
	}

	appids := make([]string, len(nodes))
	for i, node := range nodes {
		_, key := filepath.Split(node.Key)
		appids[i] = key
	}
	return appids, nil
}

func (d *Dao) checkClusterExists(ctx context.Context, cname string) (bool, error) {
	_, err := d.e.LS(ctx, fmt.Sprintf("%s/%s", etcd.ClusterDir, cname))
	if client.IsKeyNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (d *Dao) fillDefaultClusterConfig(cluster *model.Cluster) {
	cluster.DialTimeout = d.c.DialTimeout
	cluster.ReadTimeout = d.c.ReadTimeout
	cluster.WriteTimeout = d.c.WriteTimeout
	cluster.NodeConns = d.c.NodeConns
	cluster.PingFailLimit = d.c.PingFailLimit
	cluster.PingAutoEject = d.c.PingAutoEject
}

// GetCluster will search clusters by given cluster name
func (d *Dao) GetCluster(ctx context.Context, cname string) (*model.Cluster, error) {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()

	exists, err := d.checkClusterExists(sub, cname)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, model.ErrNotFound
	}

	istr, err := d.e.ClusterInfo(sub, cname)
	if client.IsKeyNotFound(err) {
		return &model.Cluster{Name: cname, State: model.StateWaiting}, nil
	} else if err != nil {
		return nil, err
	}
	info := &create.CacheInfo{}
	de := json.NewDecoder(strings.NewReader(istr))
	err = de.Decode(info)
	if err != nil {
		return nil, err
	}

	clusterState, err := d.e.Get(sub, fmt.Sprintf("%s/%s/%s/state", etcd.JobDetailDir, info.Group, info.JobID))
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

	instances, err := d.getClusterInstances(sub, info, clusterState)
	if err != nil {
		return nil, err
	}

	appids, err := d.getClusterAppids(sub, info.Name)
	if err != nil {
		return nil, err
	}

	var fePort int64
	feport, err := d.e.Get(sub, fmt.Sprintf("%s/%s/fe-port", etcd.ClusterDir, cname))
	if client.IsKeyNotFound(err) {
		fePort = -1
	} else if err != nil {
		return nil, err
	} else {
		fePort, err = strconv.ParseInt(feport, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	c := &model.Cluster{
		Name:         info.Name,
		CacheType:    string(info.CacheType),
		Appids:       appids,
		FrontEndPort: int(fePort),
		MaxMemory:    info.MaxMemory,
		Thread:       info.Thread,
		Version:      info.Version,
		Number:       info.Number,
		State:        clusterState,
		Instances:    instances,
		Group:        info.Group,
		Monitor:      d.m.Href(info.Name),
	}
	d.fillDefaultClusterConfig(c)
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

		if client.IsKeyNotFound(err) {
			err = nil
			continue
		} else if err != nil {
			log.Errorf("GetClusters.GetCluster err %s", err)
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
	var (
		appids []*etcd.Node
	)

	appids, err = d.e.LS(sub, fmt.Sprintf("%s/%s/appids", etcd.ClusterDir, cname))
	if client.IsKeyNotFound(err) {
		err = nil
	} else if err != nil {
		return
	}

	if len(appids) > 0 {
		err = ErrClusterAssigned
		return
	}

	j, err := d.createDestroyClusterJob(ctx, cname)
	if err != nil {
		log.Errorf("create destroy cluster job fail %v", err)
		return
	}
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

	number := int(p.TotalMemory) / int(p.SpecMemory)
	if number == 0 {
		return "", fmt.Errorf(
			"total memory(%fm) is less than each node's max memory(%fm) we can only create 0 node, reject", p.SpecMemory, p.TotalMemory)
	}

	if number > 1024 {
		return "", fmt.Errorf("cluster is too large. node number(%d) is more than 1024", number)
	}

	// check if master num is even
	ctype := types.CacheType(p.CacheType)
	if ctype == types.CacheTypeRedisCluster && number%2 != 0 {
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

	seq, err := d.e.Sequence(subctx, etcd.PortSequence)
	if err != nil {
		log.Errorf("fail to create front-end port due to %s", err)
		return "", err
	}
	err = d.e.Set(subctx, fmt.Sprintf("%s/%s/fe-port", etcd.ClusterDir, p.Name), fmt.Sprintf("%d", seq))
	if err != nil {
		log.Errorf("fail to set front-end port due to %s", err)
		return "", err
	}

	t, err := d.createCreateClusterJob(p)
	if err != nil {
		log.Infof("create fail due to %s", err)
		return "", err
	}

	// TODO: move it into mesos framework task
	// if ctype == types.CacheTypeRedisCluster {
	// 	go func(name string) {
	// 		err := balance.Balance(p.Name, d.e)
	// 		if err != nil {
	// 			log.Errorf("fail to balance cluster %s due to %v", name, err)
	// 		}
	// 	}(p.Name)
	// }

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
	_, err := d.e.LS(context.Background(), fmt.Sprintf("%s/%s/", etcd.ClusterDir, cname))
	if client.IsKeyNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("cluster %s has been existed", cname)
}

func (d *Dao) mapCacheType(cacheType string) (types.CacheType, error) {
	ct := types.CacheType(cacheType)
	if ct != types.CacheTypeMemcache && ct != types.CacheTypeRedis && ct != types.CacheTypeRedisCluster {
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
		maxMem, err = strconv.ParseFloat(strings.TrimRight(ssp[1], "g"), 64)
		maxMem = maxMem * 1024.0
	}
	return
}

func (d *Dao) createCreateClusterJob(p *model.ParamCluster) (*job.Job, error) {
	t := &job.Job{
		OpType:  job.OpCreate,
		Name:    p.Name,
		Version: p.Version,
		Image:   d.getClusterImage(p.CacheType),
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
		_, err = d.e.GenID(ctx, fmt.Sprintf("%s/%s/", etcd.AppidsDir, appid), cluster)
		if err != nil {
			return
		}

		err = d.e.Set(ctx, fmt.Sprintf("%s/%s/appids/%s", etcd.ClusterDir, cluster, appid), "")
		if err != nil {
			return
		}
	}

	return
}

// createDestroyClusterJob will create remove cluster job.
func (d *Dao) createDestroyClusterJob(ctx context.Context, cname string) (j *job.Job, err error) {
	var info string
	info, err = d.e.ClusterInfo(ctx, cname)
	if err != nil {
		log.Errorf("get cluster info err %v", err)
		return
	}
	t := new(create.CacheInfo)
	err = json.Unmarshal([]byte(info), t)
	if err != nil {
		log.Errorf("get cacheinfo err%v", err)
		return
	}
	j = &job.Job{
		OpType:    job.OpDestroy,
		Name:      cname,
		Group:     t.Group,
		CacheType: t.CacheType,
	}
	return
}

func (d *Dao) getClusterImage(ctype string) string {
	for _, v := range d.vs {
		if v.CacheType == ctype {
			return v.Image
		}
	}
	return ""
}
