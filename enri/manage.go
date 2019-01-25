package enri

import (
	"errors"
	"overlord/pkg/log"
	"strings"
	"time"
)

var (
	errMasterCount = errors.New("err num of master")
)

// Create create cluster.
func Create(addrs []string, slave int) (c *Cluster, err error) {
	master := len(addrs) / (slave + 1)
	if master < 3 {
		err = errMasterCount
		return
	}
	var nodes = make([]*Node, 0, len(addrs))
	for _, addr := range addrs {
		var node *Node
		node, err = NewNode(addr)
		if err != nil {
			return
		}
		node.Init()
		nodes = append(nodes, node)
	}

	c = &Cluster{
		nodes:       nodes,
		masterCount: master,
		slaveCount:  slave,
	}
	err = c.create()
	return
}

// Add get Cluster by cluster seed addr.
func Add(seed string, addrs []string) (c *Cluster, err error) {
	c, err = cluster(seed)
	for _, addr := range addrs {
		couple := strings.Split(addr, ",")
		var node *Node
		node, err = NewNode(couple[0])
		if err != nil {
			return
		}
		node.Init()
		if !node.valid() {
			err = errNode
			return
		}

		err = c.addNode(node.ip, node.port)
		if err != nil {
			return
		}
		if len(couple) == 2 {
			var slave *Node
			slave, err = NewNode(couple[1])
			if err != nil {
				return
			}
			slave.slaveof = node.name
			slave.setSlave()
		}
	}
	for !c.consistent() {
		time.Sleep(time.Second)
		log.Info("wait cluster to consistent")
	}
	return
}

// Delete delete node from cluster.
func Delete(seed string, addrs []string) (c *Cluster, err error) {
	c, err = cluster(seed)
	if err != nil {
		return
	}
	return
}
