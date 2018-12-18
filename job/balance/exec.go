package balance

import (
	"context"
	"encoding/json"
	"fmt"
	"overlord/job/create"
	"overlord/lib/chunk"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/lib/myredis"
	"strings"
	"time"

	"go.etcd.io/etcd/client"
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
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

		val, err := e.Get(ctx, path)

		if client.IsKeyNotFound(err) {
			log.Infof("waiting cluster %s for info was setted", clusterName)
			_, val, err = e.WatchOneshot(ctx, path, etcd.ActionSet)
			if err != nil {
				log.Warnf("fail to watch cluster due time")
				continue
			}
		} else if err != nil {
			log.Infof("get cluster %v error %v", clusterName, err)
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

	log.Infof("fetch instances for %s", clusterName)
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
			spath := fmt.Sprintf("%s/%s/state", etcd.InstanceDirPrefix, node.Value)
			val, err := e.Get(ctx, spath)
			if err != nil {
				log.Infof("cluster %s get state %s fail", clusterName, spath)
				continue
			}
			allIsRunning = allIsRunning && (val == create.SubStateRunning)
			if !allIsRunning {
				log.Infof("cluster %s node %s not done", clusterName, node.Value)
				break
			}
		}

		if allIsRunning {
			log.Infof("cluster %s all instances is done now", clusterName)
			break
		}
	}

	// for {
	// 	_, val, err := e.WatchOneshot(ctx, fmt.Sprintf("%s/%s/state", etcd.JobDetailDir, info.JobID), etcd.ActionSet)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	if val == job.StateNeedBalance {
	// 		break
	// 	}
	// }

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
		if err == myredis.ErrNoNode {
			log.Errorf("get an trash cluster err %s", err)
			return err
		} else if err != nil {
			log.Errorf("fail to check consistent due to %s", err)
			return
		}

		if consistent {
			log.Info("succeed to consistent with cluster")
			return
		}

		err := b.client.BumpEpoch()
		if err != nil {
			log.Errorf("bump epoch to cluster %s fail due %s", b.info.Cluster, err)
			return
		}
	}
}

func (b *TryBalanceJob) tryBalance(ctx context.Context) (err error) {
	var (
		balanced = false
		ticker   = time.NewTicker(time.Second * 3)
	)
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-ticker.C:
		}

		balanced, err = b.client.IsBalanced()
		if err != nil {
			log.Errorf("check balanced for cluster %s fail due to %s", b.info.Cluster, err)
			continue
		}
		if balanced {
			log.Infof("succeed to balanced the cluster %s", b.info.Cluster)
			return
		}
		log.Warnf("cluster %s is not balanced", b.info.Cluster)

		err = b.client.TryBalance()
		if err != nil {
			log.Errorf("try execute balanced command fail to %s due to %s", b.info.Cluster, err)
			continue
		}
		log.Infof("succeed execute TryBalance for %s", b.info.Cluster)
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
		log.Infof("trying to balance the cluster %s with trace job %s in balancer", b.info.Cluster, b.info.TraceJobID)
	}

	if isTrace {
		err = b.e.SetJobState(sub, b.info.Group, b.info.TraceJobID, TraceJobWaitConsistent)
		if err != nil {
			return
		}
	}

	log.Infof("trying to wait balance for cluster %s", b.info.Cluster)
	err = b.waitForConsistent(sub)
	if err != nil {
		log.Warnf("trying to wait consistent %s fail due %s", b.info.Cluster, err)
		return
	}

	if isTrace {
		err = b.e.SetJobState(sub, b.info.Group, b.info.TraceJobID, TraceJobTryBalancing)
		if err != nil {
			return
		}
	}

	log.Infof("trying to execute balance really for %s", b.info.Cluster)
	err = b.tryBalance(sub)
	if err != nil {
		log.Errorf("fail to balance the whole cluster %s due %s", b.info.Cluster, err)

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
	log.Infof("success executed balance really for %s for job %s.%s", b.info.Cluster, b.info.Group, b.info.TraceJobID)

	if isTrace {
		err = b.e.SetJobState(sub, b.info.Group, b.info.TraceJobID, TraceJobBalanced)
	}
	return
}
