package dao

import (
	"context"
	"fmt"
	"overlord/lib/etcd"
)

// GetAllSpecs will query and load all specs
func (d *Dao) GetAllSpecs(ctx context.Context) ([]string, error) {
	return d.e.GetAllSpecs(ctx)
}

// RemoveSpec remove the given specification.
func (d *Dao) RemoveSpec(ctx context.Context, spec string) error {
	return d.e.Delete(ctx, fmt.Sprintf("%s/%s", etcd.SpecsDir, spec))
}
