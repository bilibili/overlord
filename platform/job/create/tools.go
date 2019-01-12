package create

import (
	"context"
	"overlord/pkg/etcd"
	"fmt"
)

func cleanEtcdDirtyDir(ctx context.Context, e *etcd.Etcd, instance string) error {
	return e.RMDir(ctx, fmt.Sprintf("%s/%s", etcd.InstanceDirPrefix, instance))
}
