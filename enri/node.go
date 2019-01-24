package enri

import (
	"fmt"
	"overlord/pkg/myredis"
)

// Node present node info.
type Node struct {
	name      string
	ip        string
	port      string
	role      Role
	self      bool
	slaveof   string
	nodes     map[string]*Node
	slots     []int64
	migrating map[int64]string
	importing map[int64]string
	conn      *myredis.Conn
}

func (n *Node) setConfigEpoch() (err error) {
	_, err = n.conn.Exec("CLUSTER SET_CONFIG_EPOCH 1")
	return err
}

func (n *Node) meet(addr string) (err error) {
	_, err = n.conn.Exec(fmt.Sprintf("CLUSTER MEET %s", addr))
	return
}

func (n *Node) setSlave() {
	n.conn.Exec(fmt.Sprintf("CLUSTER REPLICATE %s", n.slaveof))
}
