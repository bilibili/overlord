package enri

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"overlord/pkg/log"
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
	var slaves []*Node
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
			log.Errorf("add node to cluster err %v", err)
			return
		}
		c.nodes = append(c.nodes, node)
		if len(couple) == 2 {
			var slave *Node
			slave, err = NewNode(couple[1])
			if err != nil {
				log.Errorf("create new node %s err %v", slave.addr(), err)
				return
			}
			err = c.addNode(slave.ip, slave.port)
			if err != nil {
				log.Errorf("add slave err %v", err)
				return
			}
			slave.slaveof = node.name
			slaves = append(slaves, slave)
		}
	}
	c.updateNode("")
	for !c.consistent() {
		time.Sleep(time.Second)
		log.Info("wait cluster to consistent")
	}
	for _, slave := range slaves {
		slave.setSlave()
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
func Migrate(src, dst string, count int64, slot int64) (err error) {
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
		dstNode.Init()
	}
	switch {
	case slot >= 0:
		migrateSlot(srcNode, dstNode, slot)
	case count != 0:
		log.Infof("migrate  %d slots  from %s to %s", slot, src, dst)
		if len(srcNode.slots) < int(count) {
			log.Errorf("%s do not have enough slot to migrate", srcNode.addr())
			return
		}
		var finished = 1
		for _, slot := range srcNode.slots[:count] {
			migrateSlot(srcNode, dstNode, slot)
			log.Infof("migrate slot %d/%d", finished, count)
			finished++
		}
	case dstNode == nil:
		log.Infof("migrate all slots  from %s to other nodes", src)
		om := otherMaster(srcNode.Nodes(), srcNode)
		start := 0
		dispatch := divide(len(srcNode.slots), len(om))
		for i, m := range om {
			end := start + dispatch[i]
			var finished = 1
			for _, slot := range srcNode.slots[start:end] {
				migrateSlot(srcNode, m, slot)
				log.Infof("migrate slot %d/%d", finished, count)
				finished++
			}
			start = end
		}
	case count == 0:
		count := len(srcNode.slots)
		log.Infof("migrate %d slots  from %s to %s", count, src, dst)
		var finished = 1
		for _, slot := range srcNode.slots {
			migrateSlot(srcNode, dstNode, slot)
			log.Infof("migrate slot %d/%d", finished, count)
			finished++
		}
	case srcNode == nil:
		log.Infof("migrate  %d slots  from other nodes to %s", count, dst)
		om := otherMaster(dstNode.Nodes(), dstNode)
		dispatch := divide(int(count), len(om))
		var finished = 1
		for i, m := range om {
			for _, slot := range m.slots[:dispatch[i]] {
				migrateSlot(m, dstNode, slot)
				log.Infof("migrate slot %d/%d", finished, count)
				finished++
			}
		}
	default:
		log.Error("migrate arg err")
	}
	return
}

// Fix fix cluster if cluster in error.
func Fix(addr string) (c *Cluster, err error) {
	if c, err = cluster(addr); err != nil {
		return
	}
	c.fixSlot()
	c.fillSlot()
	node, err := NewNode(addr)
	if err != nil {
		log.Errorf("new node err %v", err)
		return
	}
	node.Init()
	if node.clusterState() {
		log.Infof("node %s in state ok", node.name)
		return
	}
	nodes := node.Nodes()
	var newestNode = nodes[0]
	for _, node := range nodes {
		if newestNode.configEpoch < node.configEpoch {
			newestNode = node
		}
	}
	newestNode.Init()
	if newestNode.clusterState() {
		for _, n := range newestNode.Nodes() {
			if n.addr() == node.addr() {
				diff := diffrence(n.slots, node.slots)
				if len(diff) > 0 {
					log.Infof("add slots %v to node %s", diff, node.addr())
					node.addSlots(diff)
				}
			}
		}
	}
	for !c.consistent() {
		time.Sleep(time.Second)
		log.Error("wait cluster to consistent")
	}
	log.Info("fix cluster success,cluster in consistency")
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

// Replicate set replicate to node.
func Replicate(master, slave string) (c *Cluster, err error) {
	c, err = cluster(master)
	masterNode, err := NewNode(master)
	if err != nil {
		return
	}
	masterNode.Init()
	for _, n := range c.nodes {
		if n.addr() == slave {
			if n.isMaster() && len(n.slots) != 0 {
				log.Errorf("can not set %s to be replicate,because it own slots", slave)
				return
			}
			n.slaveof = masterNode.name
			n.setSlave()
			return
		}
	}

	slaveNode, err := NewNode(slave)
	if err != nil {
		return
	}
	c.addNode(slaveNode.ip, slaveNode.port)
	slaveNode.slaveof = masterNode.name
	slaveNode.setSlave()
	return
}

// Info print cluster info.
func Info(node string) (err error) {
	n, err := NewNode(node)
	if err != nil {
		log.Errorf("newnode err %v", err)
		return
	}
	infos := n.Info()
	for k, v := range infos {
		fmt.Printf("%s:%s\r\n", k, v)
	}
	return
}

// Check cluster state.
func Check(node string) (err error) {
	c, err := cluster(node)
	if err != nil {
		return
	}

	var times = 5
	for !c.consistent() && times > 0 {
		time.Sleep(time.Second)
		times--
	}
	return
}
