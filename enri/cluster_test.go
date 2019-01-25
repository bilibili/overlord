package enri

import (
	"testing"

	"overlord/pkg/log"

	"github.com/stretchr/testify/assert"
)

func TestNode(t *testing.T) {
	var addr = "127.0.0.1:7000"
	n, err := NewNode(addr)
	assert.NoError(t, err)
	n.Init()
}

func TestCreate(t *testing.T) {
	log.InitHandle(log.NewStdHandler())
	addrs := []string{
		"127.0.0.1:7000",
		"127.0.0.1:7001",
		"127.0.0.1:7002",
		"127.0.0.1:7003",
		"127.0.0.1:7004",
		"127.0.0.1:7005",
	}
	cluster, err := Create(addrs, 1)
	assert.NoError(t, err)
	t.Logf("create cluster %v", cluster.nodes)
	cluster.Create()
}
