package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/lib/etcd"
)

// GetJob will get job info from redis or etcd
func (d *Dao) GetJob(ctx context.Context, jobID string) (*model.Job, error) {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()
	state, err := d.e.Get(subctx, fmt.Sprintf("%s/%s/state", etcd.JobDetailDir, jobID))
	if err != nil {
		return nil, err
	}
	t := &model.Job{ID: jobID, State: state}
	return t, nil
}
