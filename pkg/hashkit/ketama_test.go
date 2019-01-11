package hashkit

import (
	"bytes"
	"strconv"
	"testing"
)

var (
	ring  = Ketama()
	nodes = []string{
		"test1.server.com",
		"test2.server.com",
		"test3.server.com",
		"test4.server.com",
	}
	sis = []int{1, 1, 2, 5}

	node5 = "test5.server.com"

	delAll bool
)

func TestGetInfo(t *testing.T) {
	ring.Init(nodes, sis)
	testHash(t)
	t.Log("----init test ok:expect 1 1 2 5----\n")

	ring.AddNode(nodes[3], 1)
	testHash(t)
	t.Log("----add exist node test ok:expect 1 1 2 1----\n")

	ring.AddNode(node5, 5)
	testHash(t)
	t.Log("----add no exist node test ok:expect 1 1 2 1 5----\n")

	ring.DelNode(nodes[0])
	testHash(t)
	t.Log("----del exist node test ok:expect 0 1 2 1 5----\n")

	ring.DelNode("wocao")
	testHash(t)
	t.Log("----del not exist node test ok:expect 0 1 2 1 5----\n")

	for _, node := range nodes {
		ring.DelNode(node)
	}
	ring.DelNode(node5)
	delAll = true
	testHash(t)
	t.Log("----del all node test ok:expect 0 0 0 0 0----\n")
}

func testHash(t *testing.T) {
	m := make(map[string]int)
	for i := 0; i < 1e6; i++ {
		s := "test value" + strconv.FormatUint(uint64(i), 10)
		bs := []byte(s)
		n, ok := ring.GetNode(bs)
		if !ok {
			if !delAll {
				t.Error("unexpected not ok???")
			}
		}
		m[n]++
		if !bytes.Equal([]byte(s), bs) {
			t.Error("hash change the bytes")
		}
	}
	for _, node := range nodes {
		t.Log(node, m[node])
	}
	t.Log(node5, m[node5])
}

func BenchmarkHash(b *testing.B) {
	ring.Init(nodes, sis)
	for i := 0; i < b.N; i++ {
		s := "test value" + strconv.FormatUint(uint64(i), 10)
		ring.GetNode([]byte(s))
	}
}
