package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/lib/etcd"
	"path/filepath"

	"strings"

	"go.etcd.io/etcd/client"
)

// GetPlainAppid will get all plain text appid list
func (d *Dao) GetPlainAppid(ctx context.Context) ([]string, error) {
	nodes, err := d.e.LS(ctx, etcd.AppidsDir)
	if err != nil {
		return nil, err
	}
	appids := make([]string, 0)
	for _, node := range nodes {
		_, appid := filepath.Split(node.Key)
		if strings.Contains(appid, ".") {
			appids = append(appids, appid)
		}
	}
	return appids, nil
}

// GetTreeAppid get the grouped all result
func (d *Dao) GetTreeAppid(ctx context.Context) ([]*model.TreeAppid, error) {
	nodes, err := d.e.LS(ctx, etcd.AppidsDir)
	if err != nil {
		return nil, err
	}
	appids := make([]string, 0)
	for _, node := range nodes {
		_, appid := filepath.Split(node.Key)
		if strings.Contains(appid, ".") {
			appids = append(appids, appid)
		}
	}

	return model.BuildTreeAppids(appids), nil
}

// GetGroupedAppid will query the grouped cluster by appid
func (d *Dao) GetGroupedAppid(ctx context.Context, appid string) (*model.GroupedAppid, error) {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()
	nodes, err := d.e.LS(sub, fmt.Sprintf("%s/%s", etcd.AppidsDir, appid))
	if err != nil {
		return nil, err
	}

	groups := map[string][]*model.Cluster{}
	for _, node := range nodes {
		cluster, err := d.GetCluster(sub, node.Value)
		if err != nil {
			continue
		}
		if cluster != nil {
			groups[cluster.Group] = append(groups[cluster.Group], cluster)
		}
	}

	gcs := []*model.GroupedClusters{}
	for group, clusters := range groups {
		gcs = append(gcs, &model.GroupedClusters{Group: group, Clusters: clusters})
	}
	return &model.GroupedAppid{Name: appid, GroupedClusters: gcs}, nil
}

// AssignAppid will assign appid with cluster
func (d *Dao) AssignAppid(ctx context.Context, cname, appid string) error {
	_, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/info", etcd.ClusterDir, cname))
	if err != nil {
		if client.IsKeyNotFound(err) {
			return model.ErrNotFound
		}
		return err
	}
	_, err = d.e.Get(ctx, fmt.Sprintf("%s/%s/appids/%s", etcd.ClusterDir, cname, appid))
	if client.IsKeyNotFound(err) {
		err := d.assignAppids(ctx, cname, appid)
		return err
	} else if err != nil {
		return err
	}

	return model.ErrConflict
}

// UnassignAppid will assign appid with cluster
func (d *Dao) UnassignAppid(ctx context.Context, cname, appid string) error {
	_, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/appids/%s", etcd.ClusterDir, cname, appid))
	if err != nil && !client.IsKeyNotFound(err) {
		return err
	}
	return d.unassignAppids(ctx, cname, appid)
}

// RemoveAppid will remove appid with check the appid assign to others cluster
func (d *Dao) RemoveAppid(ctx context.Context, appid string) error {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()

	nodes, err := d.e.LS(sub, fmt.Sprintf("%s/%s", etcd.AppidsDir, appid))
	if err != nil {
		return err
	}
	if len(nodes) != 0 {
		clusters := make([]string, len(nodes))
		for idx, node := range nodes {
			clusters[idx] = node.Value
		}

		return fmt.Errorf("appid %s has ben assigned to [%v]", appid, strings.Join(clusters, ", "))
	}

	return d.e.RMDir(sub, fmt.Sprintf("%s/%s", etcd.AppidsDir, appid))
}
