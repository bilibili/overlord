package balance

import (
	"context"
	"encoding/json"
	"fmt"
	"overlord/lib/chunk"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/lib/myredis"
	"overlord/task"
	"overlord/task/create"
	"strings"
	"time"
)

// define state
const (
	TraceTaskWaitConsistent = "cluster_wait_consistent"
	TraceTaskTryBalancing   = "cluster_try_balancing"
	TraceTaskBalanced       = "cluster_finally_balanced"
	TraceTaskUnBalanced     = "cluster_finally_not_balanced"
)

// GenTryBalanceTask generate balanced task into task
func GenTryBalanceTask(taskID string, clusterName string, e *etcd.Etcd) (*TryBalanceTask, error) {
	path := fmt.Sprintf("%s/%s/info", etcd.ClusterDir, clusterName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	info := &create.RedisClusterInfo{}

	val, err := e.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	rd := strings.NewReader(val)
	decoder := json.NewDecoder(rd)
	err = decoder.Decode(info)
	if err != nil {
		return nil, err
	}

	nodes, err := e.LS(ctx, fmt.Sprintf("%s/%s/instances", etcd.ClusterDir, clusterName))
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
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
		_, val, err := e.WatchOneshot(ctx, fmt.Sprintf("%s/%s/state", etcd.TaskDetailDir, info.TaskID), etcd.ActionSet)
		if err != nil {
			return nil, err
		}

		if val == task.StateNeedBalance {
			break
		}
	}

	tbi := &TryBalanceInfo{
		TaskID:      taskID,
		TraceTaskID: info.TaskID,
		Cluster:     clusterName,
		Chunks:      info.Chunks,
	}

	tbt := &TryBalanceTask{
		info:   tbi,
		e:      e,
		client: myredis.New(),
	}

	return tbt, nil
}

// TryBalanceInfo is the task to balance the whole cluster
type TryBalanceInfo struct {
	TaskID      string
	TraceTaskID string
	Cluster     string
	Chunks      []*chunk.Chunk
}

// TryBalanceTask is the struct descript balance task.
type TryBalanceTask struct {
	info   *TryBalanceInfo
	e      *etcd.Etcd
	client *myredis.Client
}

func (b *TryBalanceTask) waitForConsistent(ctx context.Context) (err error) {
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

func (b *TryBalanceTask) tryBalance(ctx context.Context) (err error) {
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

// Balance will run balance task
func (b *TryBalanceTask) Balance() (err error) {
	nodeNum := len(b.info.Chunks) * 4
	timeout := time.Second*time.Duration(300) + time.Second*time.Duration(nodeNum*10)
	sub, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = b.e.SetTaskState(sub, b.info.TaskID, task.StateRunning)
	b.client.SetChunks(b.info.Chunks)

	isTrace := b.info.TraceTaskID == ""
	if isTrace {
		// should not report status of task
		log.Info("skip report task by unset TraceTaskID")
	} else {
		log.Infof("trying to balanced the cluster %s with trace task %s in task %s", b.info.Cluster, b.info.TraceTaskID, b.info.TaskID)
	}

	if isTrace {
		err = b.e.SetTaskState(sub, b.info.TraceTaskID, TraceTaskWaitConsistent)
		if err != nil {
			return
		}
	}
	err = b.waitForConsistent(sub)
	if err != nil {
		return
	}

	if isTrace {
		err = b.e.SetTaskState(sub, b.info.TraceTaskID, TraceTaskTryBalancing)
		if err != nil {
			return
		}
	}

	err = b.tryBalance(sub)
	if err != nil {

		if err == context.DeadlineExceeded {
			if isTrace {
				err = b.e.SetTaskState(sub, b.info.TraceTaskID, TraceTaskUnBalanced)
				if err != nil {
					return
				}
			}
			err = nil
		}

		return
	}

	if isTrace {
		err = b.e.SetTaskState(sub, b.info.TraceTaskID, TraceTaskBalanced)
		if err != nil {
			return
		}
	}

	err = b.e.SetTaskState(sub, b.info.TaskID, task.StateDone)
	return
}
