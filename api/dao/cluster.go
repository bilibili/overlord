package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/lib/etcd"
	"overlord/job/create"
)

// GetCluster will search clusters by given cluster name
func (d *Dao) GetCluster(ctx context.Context, cname string) (*model.Cluster, error) {
	// istr, err := d.e.Get(ctx, fmt.Sprintf("%s/%s/info", etcd.ClusterDir, cname))
	// if err != nil {
	// 	return err
	// }

	return nil, nil
}
