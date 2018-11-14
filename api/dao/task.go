package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/lib/etcd"
)

// GetTask will get task info from redis or etcd
func (d *Dao) GetTask(ctx context.Context, taskID string) (*model.Task, error) {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()
	state, err := d.e.Get(subctx, fmt.Sprintf("%s/%s/state", etcd.TaskDetailDir, taskID))
	if err != nil {
		return nil, err
	}
	t := &model.Task{ID: taskID, State: state}
	return t, nil
}
