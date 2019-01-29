package enri

import (
	"testing"
	"time"

	"overlord/pkg/log"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	log.InitHandle(log.NewStdHandler())
	m.Run()
}
func resetNode(addrs []string) {
	for _, addr := range addrs {
		node, _ := NewNode(addr)
		node.conn.Exec("CLUSTER RESET")
	}
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
	resetNode(addrs)
	cluster, err := Create(addrs, 1)
	assert.NoError(t, err)
	t.Logf("create cluster %v", cluster.nodes)
	cluster.updateNode("")
	for !cluster.consistent() {
		time.Sleep(time.Millisecond * 10)
	}
	checkCluster(t, cluster)
}

func TestAddNode(t *testing.T) {
	seed := "127.0.0.1:7000"
	addrs := []string{
		"127.0.0.1:7007",
		"127.0.0.1:7006",
	}
	resetNode(addrs)
	cluster, err := Add(seed, addrs)
	assert.NoError(t, err)
	for _, node := range cluster.nodes {
		assert.Len(t, node.Nodes(), 8)
	}
	cluster.updateNode("")
	for !cluster.consistent() {
		time.Sleep(time.Millisecond * 10)
	}
	checkCluster(t, cluster)
}
func TestReshard(t *testing.T) {
	seed := "127.0.0.1:7000"
	c, err := Reshard(seed)
	assert.NoError(t, err)
	c.updateNode("")
	c.sortNode()
	dispatch := divide(16384, len(c.master))
	for i, node := range c.master {
		assert.Equal(t, len(node.slots), dispatch[i])
	}
	for !c.consistent() {
		time.Sleep(time.Millisecond * 10)
	}
	checkCluster(t, c)
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
	checkCluster(t, cluster)
}

func TestFix(t *testing.T) {
	seed := "127.0.0.1:7000"
	c, err := Fix(seed)
	assert.NoError(t, err)
	assert.True(t, c.consistent())
	checkCluster(t, c)
}

func checkCluster(t *testing.T, c *Cluster) {
	info := c.nodes[0].Info()
	assert.Equal(t, "ok", info["cluster_state"])
}
func TestMigrate(t *testing.T) {
	src := "127.0.0.1:7000"
	dst := "127.0.0.1:7001"
	var count int64 = 10
	err := Migrate(src, dst, count, -1)
	assert.NoError(t, err)
	srcNode, err := NewNode(src)
	assert.NoError(t, err)
	srcNode.Init()
	err = Migrate(src, dst, 0, srcNode.slots[0])
	assert.NoError(t, err)
}
