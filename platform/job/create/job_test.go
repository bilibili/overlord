package create

import (
	"math/rand"
	"os"
	"overlord/platform/chunk"
	"overlord/pkg/etcd"
	"overlord/pkg/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func mockDist(num int) (dist *chunk.Dist) {
	rand.Seed(time.Now().Unix())
	dist = new(chunk.Dist)
	for i := 0; i < num; i++ {
		addr := &chunk.Addr{
			IP:   "0.0.0.0",
			Port: rand.Intn(30000),
		}
		dist.Addrs = append(dist.Addrs, addr)
	}
	return
}
func newEtcd(conf string) (e *etcd.Etcd, err error) {
	return etcd.New(conf)
}

func TestCreateJob(t *testing.T) {
	os.Setenv("RunMode", "test")
	SetWorkDir("/tmp/data/%d")
	info := &CacheInfo{
		JobID:     "test",
		Name:      "test",
		CacheType: types.CacheTypeMemcache,
		MaxMemory: 10,
		Number:    4,
		Thread:    1,
		Version:   "1.5.12",
	}
	info.Dist = mockDist(info.Number)
	db, err := newEtcd("http://127.0.0.1:2379")
	assert.NoError(t, err)
	job := NewCacheJob(db, info)
	_ = job.Create()
	for _, inst := range info.Dist.Addrs {
		dpinfo, err := GenDeployInfo(db, inst.IP, inst.Port)
		assert.NoError(t, err)
		// assert.Equal(t, info.CacheType, dpinfo.CacheType, "assert cache type")
		assert.Equal(t, info.JobID, dpinfo.JobID, "assert job id")
		assert.NoError(t, err)
		p, err := SetupCacheService(dpinfo)
		assert.NoError(t, err, "setup cache service")
		p.Stop()
	}
}
