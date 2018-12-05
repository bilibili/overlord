package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/lib/etcd"
	"path/filepath"
	"sort"
	"strings"

	"go.etcd.io/etcd/client"
)

// SearchAppids will search all the apps
func (d *Dao) SearchAppids(ctx context.Context, name string, page *model.QueryPage) ([]*model.Appid, error) {
	nodes, err := d.e.LS(ctx, fmt.Sprintf("%s/", etcd.AppidsDir))
	if err != nil {
		return nil, err
	}
	appids := []*model.Appid{}

	for _, node := range nodes {
		_, appid := filepath.Split(node.Key)
		if strings.Contains(appid, name) {
			clusters, err := d.e.LS(ctx, fmt.Sprintf("%s/%s/", etcd.AppidsDir, appid))
			if err != nil {
				continue
			}

			cs := make([]string, len(clusters))
			for i, c := range clusters {
				cs[i] = c.Value
			}
			appids = append(appids, &model.Appid{Name: appid, Clusters: cs})
			if len(appids) == page.PageCount*page.PageNum {
				break
			}
		}
	}
	sort.Sort(byName(appids))
	lower, upper := page.Bounds()
	return appids[lower:upper], nil
}

type byName []*model.Appid

func (b byName) Less(i, j int) bool {
	return strings.Compare(b[i].Name, b[j].Name) > 0
}

func (b byName) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byName) Len() int {
	return len(b)
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
	_, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/info", etcd.ClusterDir, cname))
	if err != nil {
		return err
	}

	_, err = d.e.Get(ctx, fmt.Sprintf("%s/%s/appids/%s", etcd.ClusterDir, cname, appid))
	if err != nil {
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
