package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"overlord/pkg/etcd"
	"overlord/platform/job/create"

	"github.com/stretchr/testify/assert"
)

func TestGenSdConfig(t *testing.T) {
	var (
		info         *create.CacheInfo
		metricLabels MetricLabels
		metricInfo   MetricInfo
		addrs        []string
		clusterPath  string
		clusterInfo  string
	)
	clusterPath = "/overlord/clusters/test-1"
	clusterInfo = fmt.Sprintf("%s/%s", "/overlord/clusters/test-1", "info")

	e, err := etcd.New("http://127.0.0.1:2379")
	assert.NoError(t, err)
	ctx := context.TODO()

	destMetricInfo := MetricInfo{
		Labels: MetricLabels{
			Job:      "redis_exporter",
			Owner:    "user1",
			Pro:      "test-1",
			Name:     "test-1",
			NodeType: "redis",
		},
		Targets: []string{"127.0.0.1:6379"},
	}

	e.Mkdir(ctx, clusterPath)
	assert.NoError(t, err)

	testData := "{\"JobID\": \"\", \"Name\":\"test-1\",\"CacheType\":\"redis\",\"Dist\":{\"Addrs\":[{\"ID\":\"00000000000000127340\",\"IP\":\"127.0.0.1\",\"Port\":6379}]}}"
	e.Set(ctx, clusterInfo, testData)
	assert.NoError(t, err)

	val, err := e.Get(context.TODO(), clusterInfo)
	assert.NoError(t, err)
	de := json.NewDecoder(strings.NewReader(val))
	err = de.Decode(&info)
	assert.NoError(t, err)

	metricLabels = MetricLabels{
		Job:      "redis_exporter",
		Owner:    "user1",
		Pro:      info.Name,
		Name:     info.Name,
		NodeType: string(info.CacheType),
	}

	for _, addr := range info.Dist.Addrs {
		addrs = append(addrs, addr.String())
	}

	metricInfo = MetricInfo{
		Labels:  metricLabels,
		Targets: addrs,
	}
	assert.Equal(t, destMetricInfo, metricInfo)

	err = e.Delete(ctx, clusterInfo)
	assert.NoError(t, err)

	err = e.RMDir(ctx, clusterPath)
	assert.NoError(t, err)
}
