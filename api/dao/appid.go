package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/lib/etcd"
	"path/filepath"
	"sort"
	"strings"
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
			if len(appids) == page.PageNum {
				break
			}
		}
	}
	sort.Sort(byName(appids))
	return appids, nil
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
