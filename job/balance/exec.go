package balance

import (
	"context"
	"encoding/json"
	"fmt"
	"overlord/job"
	"overlord/job/create"
	"overlord/lib/chunk"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/lib/myredis"
	"strings"
	"time"
)

// define state
const (
	TraceJobWaitConsistent = "cluster_wait_consistent"
	TraceJobTryBalancing   = "cluster_try_balancing"
	TraceJobBalanced       = "cluster_finally_balanced"
	TraceJobUnBalanced     = "cluster_finally_not_balanced"
)

// Balance will balance the given clusterName
func Balance(clusterName string, e *etcd.Etcd) error {
	t, err := genTryBalanceJob(clusterName, e)
	if err != nil {
		return err
	}
	return t.Balance()
}

// genTryBalanceJob generate balanced job into job
func genTryBalanceJob(clusterName string, e *etcd.Etcd) (*TryBalanceJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	path := fmt.Sprintf("%s/%s/info", etcd.ClusterDir, clusterName)
	info := &create.CacheInfo{}
	ticker := time.NewTicker(time.Second * 3)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}

		_, val, err := e.WatchOneshot(ctx, path, etcd.ActionSet)
		if err != nil {
			log.Warnf("fail to watch cluster due time")
			continue
		}

		rd := strings.NewReader(val)
		decoder := json.NewDecoder(rd)
		err = decoder.Decode(info)
		if err != nil {
			return nil, err
		}
		break
	}

	nodes, err := e.LS(ctx, fmt.Sprintf("%s/%s/instances", etcd.ClusterDir, clusterName))
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}

		allIsRunning := true
		for _, node := range nodes {
			val, err := e.Get(ctx, fmt.Sprintf("%s/%s/state", etcd.InstanceDirPrefix, node.Value))
			if err != nil {
				return nil, err
			}
			allIsRunning = allIsRunning && (val == create.SubStateRunning)
			if !allIsRunning {
				break
			}
		}

		if allIsRunning {
			break
		}
	}

	for {
		_, val, err := e.WatchOneshot(ctx, fmt.Sprintf("%s/%s/state", etcd.JobDetailDir, info.JobID), etcd.ActionSet)
		if err != nil {
			return nil, err
		}

		if val == job.StateNeedBalance {
			break
		}
	}

	tbi := &TryBalanceInfo{
		TraceJobID: info.JobID,
		Group:      info.Group,
		Cluster:    clusterName,
		Chunks:     info.Chunks,
	}

	tbt := &TryBalanceJob{
		info:   tbi,
		e:      e,
		client: myredis.New(),
	}

	return tbt, nil
}

// TryBalanceInfo is the job to balance the whole cluster
type TryBalanceInfo struct {
	TraceJobID string
	Group      string
	Cluster    string
	Chunks     []*chunk.Chunk
}

// TryBalanceJob is the struct descript balance job.
type TryBalanceJob struct {
	info   *TryBalanceInfo
	e      *etcd.Etcd
	client *myredis.Client
}

func (b *TryBalanceJob) waitForConsistent(ctx context.Context) (err error) {
	var consistent = false
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}

		consistent, err = b.client.IsConsistent()
		if err != nil {
			log.Errorf("fail to check consistent due to %s", err)
			continue
		}
		if consistent {
			log.Info("succeed to consistent with cluster")
			return
		}
	}
}

func (b *TryBalanceJob) tryBalance(ctx context.Context) (err error) {
	var (
		balanced = false
	)
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}

		balanced, err = b.client.IsBalanced()
		if err != nil {
			log.Errorf("check balanced fail due to %s", err)
			continue
		}
		if balanced {
			log.Info("succeed to balanced the cluster")
			return
		}

		err = b.client.TryBalance()
		if err != nil {
			log.Errorf("try execute balanced command fail due to %s", err)
			continue
		}
	}
}

// Balance will run balance job
func (b *TryBalanceJob) Balance() (err error) {
	nodeNum := len(b.info.Chunks) * 4
	timeout := time.Second*time.Duration(300) + time.Second*time.Duration(nodeNum*10)
	sub, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	b.client.SetChunks(b.info.Chunks)

	isTrace := b.info.TraceJobID == ""
	if isTrace {
		// should not report status of job
		log.Info("skip report job by unset TraceJobID")
	} else {
		log.Infof("trying to balanced the cluster %s with trace job %s in balancer", b.info.Cluster, b.info.TraceJobID)
	}

	if isTrace {
		err = b.e.SetJobState(sub, b.info.Group, b.info.TraceJobID, TraceJobWaitConsistent)
		if err != nil {
			return
		}
	}
	err = b.waitForConsistent(sub)
	if err != nil {
		return
	}

	if isTrace {
		err = b.e.SetJobState(sub, b.info.Group, b.info.TraceJobID, TraceJobTryBalancing)
		if err != nil {
			return
		}
	}

	err = b.tryBalance(sub)
	if err != nil {
		if err == context.DeadlineExceeded {
			if isTrace {
				err = b.e.SetJobState(sub, b.info.Group, b.info.TraceJobID, TraceJobUnBalanced)
				if err != nil {
					return
				}
			}
			err = nil
		}

		return
	}

	if isTrace {
		err = b.e.SetJobState(sub, b.info.Group, b.info.TraceJobID, TraceJobBalanced)
	}
	return
}
