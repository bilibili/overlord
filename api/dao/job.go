package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/job"
	"overlord/lib/etcd"
	"path/filepath"
	"strings"
)

// GetJob will get job info from redis or etcd
func (d *Dao) GetJob(ctx context.Context, jobID string) (*model.Job, error) {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()
	state, err := d.e.Get(subctx, fmt.Sprintf("%s/%s/state", etcd.JobDetailDir, jobID))
	if err != nil {
		return nil, err
	}
	param, err := d.e.Get(subctx, fmt.Sprintf("%s/%s", etcd.JobsDir, jobID))
	if err != nil {
		return nil, err
	}

	t := &model.Job{ID: strings.Replace(jobID, "/", ".", -1), State: state, Param: param}
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

	jobs := make([]*model.Job, 0)

	for _, node := range nodes {
		group := getID(node.Key)
		subs, err := d.e.LS(sub, fmt.Sprintf("%s/%s", etcd.JobDetailDir, group))
		if err != nil {
			return nil, err
		}

		for _, subn := range subs {
			subid := getID(subn.Key)
			mjob, err := d.GetJob(sub, fmt.Sprintf("%s/%s", group, subid))
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, mjob)
		}
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
