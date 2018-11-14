package etcd

import (
	"context"
	"encoding/json"
	"overlord/proto"
	"overlord/task"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEtcd(t *testing.T) {
	e, err := New("http://127.0.0.1:2379")
	ctx := context.TODO()
	assert.NoError(t, err)
	e.GenID(ctx, "/order", "1")
	e.GenID(ctx, "/order", "2")
	_, err = e.Get(ctx, "/order")
	assert.NoError(t, err)
}
func TestSet(t *testing.T) {
	e, err := New("http://172.22.33.167:2379")
	ctx := context.TODO()
	assert.NoError(t, err)
	task := task.Task{
		Name:      "test",
		CacheType: proto.CacheTypeMemcache,
		Version:   "1.5.12",
		Num:       6,
		MaxMem:    10,
		CPU:       0.1,
	}
	bs, err := json.Marshal(task)
	assert.NoError(t, err)
	err = e.Set(ctx, "/overlord/task/task1", string(bs))
	assert.NoError(t, err)

}
