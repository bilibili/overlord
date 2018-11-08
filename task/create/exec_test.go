package create

import (
	"fmt"
	"overlord/lib/etcd"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCatSleepOk(t *testing.T) {
	info := &DeployInfo{
		TaskID:    "12345",
		CacheType: "redis",
		Port:      4399,
		TplTree: map[string]string{
			"/tmp/data/redis/4399/boynextdoor.md": "ass we can",
		},
	}

	err := SetupCacheService(info)
	assert.NoError(t, err, "fail to create CatSleep process")
}
func TestGetDeployInfo(t *testing.T) {
	etcd, err := etcd.New("http://172.22.33.167:2379")
	assert.NoError(t, err)
	info, err := GenDeployInfo(etcd, "172.22.33.175", 31000)
	assert.NoError(t, err)
	fmt.Printf("%v", info)
}
