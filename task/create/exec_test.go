package create

import "testing"
import "github.com/stretchr/testify/assert"

func TestCreateCatSleepOk(t *testing.T) {
	info := &DeployInfo{
		TaskID:    "12345",
		CacheType: "redis",
		Port:      4399,

		ExecStart:   "sleep 1 && cat meta.toml",
		ExecStop:    "echo stopping...",
		ExecRestart: "echo restarting...",
		TplTree: map[string]string{
			"/tmp/data/redis/4399/boynextdoor.md": "ass we can",
		},
	}

	err := SetupCacheService(info)
	assert.NoError(t, err, "fail to create CatSleep process")
}
