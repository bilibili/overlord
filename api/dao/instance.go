package dao

import (
	"context"
	"fmt"
	"overlord/api/model"
	"overlord/job"
	"overlord/lib/etcd"
)

// SetInstanceWeight will change the given instance weight
func (d *Dao) SetInstanceWeight(ctx context.Context, addr string, weight int) error {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()
	return d.e.Set(sub, fmt.Sprintf("%s/%s/weight", etcd.InstanceDirPrefix, addr), fmt.Sprint(weight))
}

// RestartInstance will try to save new task into job stats
func (d *Dao) RestartInstance(ctx context.Context, cname, addr string) (string, error) {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()
	cluster, err := d.GetCluster(sub, cname)
	if err != nil {
		return "", err
	}
	contains := false
	for _, inst := range cluster.Instances {
		if fmt.Sprintf("%s:%d", inst.IP, inst.Port) == addr {
			contains = true
			break
		}
	}

	if !contains {
		return "", fmt.Errorf("cluster %s doesn't contains node %s", cname, addr)
	}
	j := d.createResartInstance(cluster, addr)
	return d.saveJob(sub, j)
}

func (d *Dao) createResartInstance(c *model.Cluster, addr string) *job.Job {
	j := &job.Job{
		Cluster: c.Name,
		Nodes:   []string{addr},
		OpType:  job.OpRestart,
		Group:   c.Group,
	}
	return j
}
