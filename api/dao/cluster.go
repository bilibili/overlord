package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"overlord/api/model"
	"overlord/job/create"
	"overlord/lib/etcd"
	"strconv"
	"strings"

	"path/filepath"

	"go.etcd.io/etcd/client"
)

// GetCluster will search clusters by given cluster name
func (d *Dao) GetCluster(ctx context.Context, cname string) (*model.Cluster, error) {
	istr, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/info", etcd.ClusterDir, cname))
	if err != nil {
		return nil, err
	}
	info := &create.CacheInfo{}
	de := json.NewDecoder(strings.NewReader(istr))
	err = de.Decode(info)
	if err != nil {
		return nil, err
	}

	instances := []*model.Instance{}
	nodes, err := d.e.LS(ctx, fmt.Sprintf("%s/%s/instances", etcd.ClusterDir, cname))
	if err != nil && !client.IsKeyNotFound(err) {
		return nil, err
	}

	for _, node := range nodes {
		vsp := strings.Split(node.Value, ":")
		val, err := strconv.ParseInt(vsp[1], 10, 64)
		if err != nil {
			val = -1
		}

		instances = append(instances, &model.Instance{
			IP:   vsp[0],
			Port: int(val),
			// TODO: change it as really state.
			State: "RUNNING",
		})
	}

	val, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/state", etcd.JobDetailDir, info.JobID))
	if err != nil {
		val = "UNKNOWN"
	}

	c := &model.Cluster{
		Name:      info.Name,
		CacheType: string(info.CacheType),
		MaxMemory: info.MaxMemory,
		Thread:    info.Thread,
		Version:   info.Version,
		Number:    info.Number,
		State:     val,
		Instances: instances,
	}

	return c, nil
}

// GetClusters will get all clusters
func (d *Dao) GetClusters(ctx context.Context) (clusters []*model.Cluster, err error) {
	var nodes []*etcd.Node
	nodes, err = d.e.LS(ctx, etcd.ClusterDir)
	clusters = make([]*model.Cluster, len(nodes))
	for i, node := range nodes {
		_, cname := filepath.Split(node.Key)
		clusters[i], err = d.GetCluster(ctx, cname)
		if err != nil {
			return
		}
	}
	return
}
