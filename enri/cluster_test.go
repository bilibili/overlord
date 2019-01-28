package enri

import (
	"testing"

	"overlord/pkg/log"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	log.InitHandle(log.NewStdHandler())
	m.Run()
}
func TestNode(t *testing.T) {
	var addr = "127.0.0.1:7000"
	n, err := NewNode(addr)
	assert.NoError(t, err)
	n.Init()
}

func TestCreate(t *testing.T) {
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
}

func TestAddNode(t *testing.T) {
	seed := "127.0.0.1:7000"
	addrs := []string{
		"127.0.0.1:7007",
		"127.0.0.1:7006",
	}
	cluster, err := Add(seed, addrs)
	assert.NoError(t, err)
	for _, node := range cluster.nodes {
		assert.Len(t, node.Nodes(), 8)
	}
}
func TestReshard(t *testing.T) {
	seed := "127.0.0.1:7000"
	c, err := Reshard(seed)
	assert.NoError(t, err)
	dispatch := divide(16384, len(c.master))
	c.updateNode("")
	c.sortNode()
	for i, node := range c.master {
		assert.Equal(t, len(node.slots), dispatch[i])
	}
}

func TestDelete(t *testing.T) {
	seed := "127.0.0.1:7000"
	addrs := []string{
		"127.0.0.1:7007",
		"127.0.0.1:7006",
	}
	cluster, err := Delete(seed, addrs)
	assert.NoError(t, err)
	for _, node := range cluster.nodes {
		assert.Len(t, node.Nodes(), 6)
	}
}

func TestFix(t *testing.T) {
	seed := "127.0.0.1:7000"
	c, err := Fix(seed)
	assert.NoError(t, err)
	assert.True(t, c.consistent())
}

func TestMigrate(t *testing.T) {

}
