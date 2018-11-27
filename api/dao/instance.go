package dao

import (
	"context"
	"fmt"
	"overlord/lib/etcd"
)

// SetInstanceWeight will change the given instance weight
func (d *Dao) SetInstanceWeight(ctx context.Context, addr string, weight int) error {
	sub, cancel := context.WithCancel(ctx)
	defer cancel()
	return d.e.Set(sub, fmt.Sprintf("%s/%s/weight", etcd.InstanceDir, addr), fmt.Sprint(weight))
}
