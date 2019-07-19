package dao

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"overlord/pkg/etcd"
	"overlord/pkg/log"
	"overlord/platform/api/model"
	"overlord/platform/job"
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

// SetJobState update job state.
func (d *Dao) SetJobState(ctx context.Context, group, jobID, state string) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	log.Infof("set job state to %s.%s as %s", group, jobID, state)
	_ = d.e.SetJobState(ctx, group, jobID, state)
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
		group := getLastField(node.Key)
		subnodes, err := d.e.LS(sub, node.Key)
		if err != nil {
			continue
		}
		for _, subnode := range subnodes {
			innerID := getLastField(subnode.Key)
			job := &model.Job{ID: group + "." + innerID, Param: subnode.Value}
			state, err := d.e.Get(sub, fmt.Sprintf("%s/%s/%s/state", etcd.JobDetailDir, group, innerID))
			if err != nil {
				continue
			}
			job.State = state
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func getLastField(etcdKey string) string {
	_, file := filepath.Split(etcdKey)
	return file
}

// ApproveJob will approve the given job
func (d *Dao) ApproveJob(ctx context.Context, jobID string) error {
	return d.e.Cas(ctx, fmt.Sprintf("%s/%s/state", etcd.JobsDir, jobID), job.StateWaitApprove, job.StateApproved)
}

// WatchJob watch on
func (d *Dao) WatchJob(ctx context.Context) (j chan *model.Job) {
	key, _ := d.e.WatchOn(ctx, etcd.JobsDir, etcd.ActionSet, etcd.ActionCreate)
	j = make(chan *model.Job)
	go func() {
		for {
			node := <-key
			pre, id := filepath.Split(node.Key)
			_, group := filepath.Split(pre[:len(pre)-1])
			job := &model.Job{ID: fmt.Sprintf("%s.%s", group, id), Param: node.Value}
			j <- job
		}
	}()
	return
}
