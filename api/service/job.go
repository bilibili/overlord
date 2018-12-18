package service

import (
	"context"
	"encoding/json"
	"overlord/api/model"
	"overlord/job"
	"overlord/job/balance"
	"overlord/lib/log"
	"overlord/proto"
	"strings"
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
	var jm *JobManager
	jm, err = newJobManager(context.Background(), s)
	if err != nil {
		return
	}

	go func() {
		for {
			inerr := jm.run(context.Background())
			if inerr != nil {
				log.Errorf("fail to run jobmanager in background due %s", inerr)
			}
			time.Sleep(time.Second * 30)
		}
	}()
	return
}

func newJobManager(ctx context.Context, s *Service) (jm *JobManager, err error) {
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
	jm = &JobManager{
		undoneJobs:   undoneJob,
		newJob:       newJob,
		retryCounter: make(map[string]int),
		maxRetry:     20,
		svc:          s,
		ticker:       time.NewTicker(time.Second * 30),
	}
	return
}

// JobManager is the total jobs manager to watch and control them.
type JobManager struct {
	undoneJobs map[string]*model.Job
	newJob     chan *model.Job

	retryCounter map[string]int
	maxRetry     int

	svc    *Service
	ticker *time.Ticker
}

func (j *JobManager) fetchNewJob(ctx context.Context) error {
	// ticker := time.NewTicker(time.Second * 30)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-j.ticker.C:
	}

	select {
	case mJob := <-j.newJob:
		j.undoneJobs[mJob.ID] = mJob
	default:
	}
	return nil
}

// run the job manager loop.
func (j *JobManager) run(ctx context.Context) error {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		err := j.scanJobs(sub)
		if err != nil {
			return err
		}

		err = j.fetchNewJob(sub)
		if err != nil {
			return err
		}
	}
}

func (j *JobManager) counterUpper(key string) bool {
	if count, ok := j.retryCounter[key]; ok {
		ncount := count + 1
		j.retryCounter[key] = ncount
		if ncount > j.maxRetry {
			return false
		}
	} else {
		j.retryCounter[key] = 1
	}

	return true
}

func (j *JobManager) scanJobs(ctx context.Context) error {
	removed := make([]string, 0)
	for _, mJob := range j.undoneJobs {
		ok := j.counterUpper(mJob.ID)
		if !ok {
			log.Infof("max retry reached to the state check for job %s", mJob.ID)
			removed = append(removed, mJob.ID)
			delete(j.retryCounter, mJob.ID)
			continue
		}

		keySplit := strings.Split(mJob.ID, ".")
		// jobGroup := keySplit[0]
		var (
			jobID = keySplit[1]
			myjob job.Job
			done  = true
		)

		err := json.Unmarshal([]byte(mJob.Param), &myjob)
		if err != nil {
			log.Warnf("[jobManager] fail to decode job.Job due to %v", err)
			continue
		}
		myjob.ID = jobID

		cluster, err := j.svc.d.GetCluster(ctx, myjob.Name)
		if err != nil {
			log.Warnf("[jobManager] fail to fetch cluster info due to %v", err)
			continue
		}
		for _, ins := range cluster.Instances {
			if ins.State != job.StateRunning {
				done = false
				break
			}
		}

		if done {
			if myjob.CacheType == proto.CacheTypeRedisCluster {
				go j.bgBalance(ctx, &myjob, mJob)
				removed = append(removed, mJob.ID)
			} else {
				j.svc.d.SetJobState(ctx, myjob.Group, jobID, job.StateDone)
				log.Infof("[jobManager] job %v finish done", *j)
				delete(j.retryCounter, mJob.ID)
				removed = append(removed, mJob.ID)
			}
		}
	}

	for _, jid := range removed {
		delete(j.undoneJobs, jid)
	}

	return nil
}

func (j *JobManager) bgBalance(ctx context.Context, myJob *job.Job, mJob *model.Job) {
	cluster := myJob.Name
	group := myJob.Group
	jid := myJob.ID

	log.Infof("start balance tracing job cluster %s with %s.%s", cluster, group, jid)
	err := balance.Balance(cluster, j.svc.d.ETCD())
	if err != nil {
		j.newJob <- mJob
		log.Errorf("[jobManager.Balance]error when balance %s due to %s", cluster, err)
	} else {
		delete(j.retryCounter, mJob.ID)
		j.svc.d.SetJobState(ctx, group, jid, job.StateDone)
		log.Infof("balance success tracing cluster %s jid %s.%s", cluster, group, jid)
	}
}
