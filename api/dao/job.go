package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/job"
	"overlord/lib/etcd"
	"path/filepath"
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

// GetJobs will get all jobs from etcd
func (d *Dao) GetJobs(ctx context.Context) ([]*model.Job, error) {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()

	nodes, err := d.e.LS(sub, etcd.JobsDir)
	if err != nil {
		return nil, err
	}
	jobs := make([]*model.Job, len(nodes))

	for idx, node := range nodes {
		jobs[idx] = &model.Job{ID: getID(node.Key), State: node.Value}
	}
	return jobs, nil
}

func getID(etcdKey string) string {
	_, file := filepath.Split(etcdKey)
	return file
}

// ApproveJob will approve the given job
func (d *Dao) ApproveJob(ctx context.Context, jobID string) error {
	return d.e.Cas(ctx, fmt.Sprintf("%s/%s/state", etcd.JobsDir, jobID), job.StateWaitApprove, job.StateApproved)
}
