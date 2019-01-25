package enri

import (
	"errors"
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
	return
}

// Add get Cluster by cluster seed addr.
func Add(cluster string) (c *Cluster, err error) {
	node, err := NewNode(cluster)
	if err != nil {
		return
	}
	node.Init()
	c = new(Cluster)
	for _, n := range node.nodes {
		c.nodes = append(c.nodes, n)
		if n.role == roleMaster {
			c.master = append(c.master, n)
		} else {
			c.salve = append(c.salve, n)
		}
	}
	return
}
