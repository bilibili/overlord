package service

import (
	"context"
	"encoding/json"
	"overlord/api/model"
	"overlord/job"
	"overlord/job/balance"
	"overlord/lib/log"
	"overlord/proto"
	"time"
)

// GetJob will get job by given jobID string
func (s *Service) GetJob(jobID string) (*model.Job, error) {
	return s.d.GetJob(context.Background(), jobID)
}

// GetJobs will get job by given jobID string
func (s *Service) GetJobs() ([]*model.Job, error) {
	return s.d.GetJobs(context.Background())
}

// ApproveJob will approve job and change the state from StateWaitApprove to StateDone
func (s *Service) ApproveJob(jobID string) error {
	return s.d.ApproveJob(context.Background(), jobID)
}

func (s *Service) jobManager() (err error) {
	ctx := context.Background()
	jobs, err := s.d.GetJobs(ctx)
	if err != nil {
		return
	}
	undoneJob := make(map[string]*model.Job)
	for _, j := range jobs {
		if j.State != job.StateDone {
			undoneJob[j.ID] = j
		}
	}
	newJob := s.d.WatchJob(ctx)
	for {
		for _, j := range undoneJob {
			var jobDetail job.Job
			err = json.Unmarshal([]byte(j.Param), &jobDetail)
			if err != nil {
				continue
			}
			cluster, err := s.d.GetCluster(ctx, jobDetail.Group)
			if err != nil {
				continue
			}
			var done = true
			for _, ins := range cluster.Instances {
				if ins.State != job.StateRunning {
					done = false
					break
				}
			}
			if done {
				if jobDetail.CacheType == proto.CacheTypeRedisCluster {
					err := balance.Balance(jobDetail.Cluster, s.d.ETCD())
					if err != nil {
						log.Errorf("error when balance %s", err)
					}
				}
				s.d.SetJobState(ctx, jobDetail.Cluster, jobDetail.ID, job.StateDone)
				delete(undoneJob, j.ID)
			}
		}
		j := <-newJob
		undoneJob[j.ID] = j
		if len(undoneJob) == 0 {
			time.Sleep(time.Second * 10)
		} else {
			time.Sleep(time.Second)
		}
	}
}
