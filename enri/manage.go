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
		c.nodes = append(c.nodes, node)
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
	c.updateNode("")
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
	for _, addr := range addrs {
		if err = c.deleteNode(addr); err != nil {
			return
		}
	}
	return
}

// Migrate slot from src to dst.
func Migrate(src, dst string, count int64, slot int64) (c *Cluster, err error) {
	var (
		srcNode, dstNode *Node
	)
	if src != "" {
		if srcNode, err = NewNode(src); err != nil {
			return
		}
		srcNode.Init()
	}
	if dst != "" {
		if dstNode, err = NewNode(dst); err != nil {
			return
		}
		srcNode.Init()
	}
	switch {
	case slot >= 0:
		migrateSlot(srcNode, dstNode, slot)
	case count != 0:
		if len(srcNode.slots) < int(count) {
			log.Errorf("%s do not have enough slot to migrate", srcNode.addr())
			return
		}
		for _, slot := range srcNode.slots[:count] {
			migrateSlot(srcNode, dstNode, slot)
		}
	case dstNode == nil:
		om := otherMaster(srcNode.Nodes(), srcNode)
		start := 0
		dispatch := divide(len(srcNode.slots), len(om))
		for i, m := range om {
			end := start + dispatch[i]
			for _, slot := range srcNode.slots[start:end] {
				migrateSlot(srcNode, m, slot)
			}
			start = end
		}
	case count == 0:
		for _, slot := range srcNode.slots {
			migrateSlot(srcNode, dstNode, slot)
		}
	case srcNode == nil:
		om := otherMaster(dstNode.Nodes(), dstNode)
		dispatch := divide(int(count), len(om))
		for i, m := range om {
			for _, slot := range m.slots[:dispatch[i]] {
				migrateSlot(m, dstNode, slot)
			}
		}
	default:
		log.Error("migrate arg err")
	}
	return
}

// Fix fix cluster if cluster in error.
func Fix(node string) (c *Cluster, err error) {
	if c, err = cluster(node); err != nil {
		return
	}
	c.fixSlot()
	c.fillSlot()
	for !c.consistent() {
		time.Sleep(time.Second)
		log.Info("wait cluster to consistent")
	}
	return
}

// Reshard reshard cluster slot distribute.
func Reshard(node string) (c *Cluster, err error) {
	if c, err = cluster(node); err != nil {
		return
	}
	c.sortNode()
	c.reshard()
	return
}
