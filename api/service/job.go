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
	log.Infof("[jobManager] recovery jobs for %v", undoneJob)
	newJob := s.d.WatchJob(ctx)
	for {
		removed := make([]string, 0)
		for key, j := range undoneJob {
			keySplit := strings.Split(key, ".")
			jobGroup := keySplit[0]
			jobID := keySplit[1]

			var jobDetail job.Job
			err = json.Unmarshal([]byte(j.Param), &jobDetail)
			if err != nil {
				log.Warnf("[jobManager] fail to decode job.Job due to %v", err)
				continue
			}
			cluster, err := s.d.GetCluster(ctx, jobDetail.Name)
			if err != nil {
				log.Warnf("[jobManager] fail to fetch cluster info due to %v", err)
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
					go func(cluster, group, jid string) {
						log.Infof("start balance tracing job cluster %s with %s.%s", cluster, group, jid)
						err := balance.Balance(cluster, s.d.ETCD())
						if err != nil {
							log.Errorf("[jobManager.Balance]error when balance %s due to %s", cluster, err)
						} else {
							s.d.SetJobState(ctx, group, jid, job.StateDone)
							log.Infof("balance success tracing cluster %s jid %s.%s", cluster, group, jid)
						}
					}(jobDetail.Name, jobDetail.Group, jobID)

					removed = append(removed, j.ID)
				} else {
					s.d.SetJobState(ctx, jobDetail.Group, jobID, job.StateDone)
					log.Infof("[jobManager] job %v finish done", *j)
					removed = append(removed, j.ID)
				}
			}
		}

		for _, jid := range removed {
			delete(undoneJob, jid)
		}

		j := <-newJob
		log.Infof("[jobManager] receive new job %v", *j)
		undoneJob[j.ID] = j
		if len(undoneJob) == 0 {
			time.Sleep(time.Second * 10)
		} else {
			time.Sleep(time.Second)
		}
	}
}
