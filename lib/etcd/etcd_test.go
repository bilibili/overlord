package etcd

import (
	"context"
	"encoding/json"
	"overlord/job"
	"overlord/proto"
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
	e, err := New("http://127.0.0.1:2379")
	ctx := context.TODO()
	assert.NoError(t, err)
	mcjob := job.Job{
		Name:      "test",
		CacheType: proto.CacheTypeMemcache,
		Version:   "1.5.12",
		Num:       6,
		MaxMem:    10,
		CPU:       0.1,
	}
	bs, err := json.Marshal(mcjob)
	assert.NoError(t, err)
	err = e.Set(ctx, "/overlord/jobs/job1", string(bs))
	assert.NoError(t, err)

	// redisjob := &job.Job{
	// 	Name:      "test",
	// 	CacheType: proto.CacheTypeRedis,
	// 	Version:   "4.0.11",
	// 	Num:       6,
	// 	MaxMem:    10,
	// 	CPU:       0.1,
	// }
	// bs, err = json.Marshal(redisjob)
	// assert.NoError(t, err)
	// err = e.Set(ctx, "/overlord/jobs/job12", string(bs))
	// assert.NoError(t, err)
}
