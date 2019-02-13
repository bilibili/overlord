package enri

import (
	"fmt"
	"overlord/pkg/myredis"
	"testing"
	"time"

	"overlord/pkg/log"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	log.InitHandle(log.NewStdHandler())
	m.Run()
}
func resetNode(t *testing.T, addrs []string) {
	for _, addr := range addrs {
		node, _ := NewNode(addr)
		_, err := node.conn.Exec(myredis.NewCmd("CLUSTER").Arg("RESET"))
		assert.NoError(t, err)
	}
}
func TestNode(t *testing.T) {
	var addr = "127.0.0.1:7000"
	n, err := NewNode(addr)
	assert.NoError(t, err)
	n.Init()
	keys, err := n.keysInSlot(4310)
	assert.NoError(t, err)
	log.Infof("keys %s", keys)
	dst, _ := NewNode("127.0.0.1:7002")
	dst.Init()
	migrateSlot(n, dst, 4310)

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
	resetNode(t, addrs)
	cluster, err := Create(addrs, 1)
	assert.NoError(t, err)
	t.Logf("create cluster %v", cluster.nodes)
	cluster.updateNode("")
	for !cluster.consistent() {
		time.Sleep(time.Millisecond * 10)
	}
}

func TestAddNode(t *testing.T) {
	seed := "127.0.0.1:7000"
	addrs := []string{
		"127.0.0.1:7007",
		"127.0.0.1:7006",
	}
	resetNode(t, addrs)
	cluster, err := Add(seed, addrs)
	assert.NoError(t, err)

	cluster.updateNode("")
	for !cluster.consistent() {
		time.Sleep(time.Millisecond * 100)
	}
	cluster.updateNode("")
	for _, node := range cluster.nodes {
		assert.Len(t, node.Nodes(), 8)
	}
}
func TestReplicate(t *testing.T) {
	master := "127.0.0.1:7007"
	slave := "127.0.0.1:7006"
	c, err := Replicate(master, slave)
	c.updateNode("")
	assert.NoError(t, err)
	for !c.consistent() {
		time.Sleep(time.Millisecond * 100)
		c.updateNode("")
		t.Log(c.nodes)
	}
	c.updateNode("")
}
func TestReshard(t *testing.T) {
	seed := "127.0.0.1:7000"
	c, err := Reshard(seed)
	fmt.Println("reshard")
	assert.NoError(t, err)
	c.updateNode("")
	for !c.consistent() {
		time.Sleep(time.Millisecond * 100)
		c.updateNode("")
		fmt.Println("wait consi")
		t.Log(c.nodes)
	}
	c.updateNode("")
	c.sortNode()
	dispatch := divide(16384, len(c.master))
	for i, node := range c.master {
		assert.Equal(t, dispatch[i], len(node.slots), node.name)
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
	for !cluster.consistent() {
		time.Sleep(time.Millisecond * 10)
	}
}

func TestFix(t *testing.T) {
	seed := "127.0.0.1:7001"
	c, err := Fix(seed)
	assert.NoError(t, err)
	assert.True(t, c.consistent())
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
