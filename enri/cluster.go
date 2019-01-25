package enri

import (
	"errors"
	"overlord/pkg/log"
	"time"

	"overlord/pkg/myredis"
)

var (
	errNode = errors.New("can not add invalid node to cluster")
)

// Role cluster role
type Role int8

const (
	roleSalve Role = iota
	roleMaster
)

func (r Role) String() string {
	switch r {
	case roleMaster:
		return "master"
	case roleSalve:
		return "slave"
	}
	return ""
}

// NewNode new node by addr.
func NewNode(addr string) (n *Node, err error) {
	ip, port, err := parseAddr(addr)
	if err != nil {
		log.Infof("NewNode err %v", err)
		return
	}
	conn := myredis.NewConn(addr)
	n = &Node{
		port:      port,
		ip:        ip,
		conn:      conn,
		nodes:     make(map[string]*Node),
		migrating: make(map[int64]string),
		importing: make(map[int64]string),
	}
	return
}

const (
	clusterCount = 16384
)

// Cluster present cluster info.
type Cluster struct {
	nodes       []*Node
	masterCount int
	slaveCount  int
	master      []*Node
	salve       []*Node
	err         error
}

func (c *Cluster) initSlot() {
	var hosts = make(map[string][]*Node)
	for _, node := range c.nodes {
		hosts[node.ip] = append(hosts[node.ip], node)
	}
	masters := spread(hosts, c.masterCount)
	slots := splitSlot(clusterCount, c.masterCount)
	slaves := spread(hosts, len(c.nodes)-c.masterCount)
	distributeSlave(masters, slaves)
	for i, master := range masters {
		var slot []int64
		for j := slots[i][0]; j < slots[i][1]; j++ {
			slot = append(slot, int64(j))
		}
		master.slots = slot
		master.role = roleMaster
	}
	c.master = masters
	c.salve = slaves
	for _, node := range c.nodes {
		log.Info(node)
	}
}

func (c *Cluster) setConfigEpoch() {
	for _, node := range c.master {
		err := node.setConfigEpoch()
		if err != nil {
			c.err = err
			return
		}
	}
}

func (c *Cluster) join() {
	if c.err != nil || len(c.nodes) == 0 {
		return
	}
	var first = c.nodes[0]
	for _, node := range c.nodes[1:] {
		err := first.meet(node.ip, node.port)
		log.Infof("%s meet %s err %v", first.port, node.port, err)
	}
}

func (c *Cluster) setSlaves() {
	for _, slave := range c.salve {
		slave.setSlave()
	}
}

func (c *Cluster) check() (err error) {
	if c.err != nil {
		return
	}
	for _, node := range c.nodes {
		if !node.valid() {
			err = errNode
			return
		}
		log.Infof("check node %s:%s success", node.ip, node.port)
	}
	return
}

func (c *Cluster) addSlots() {
	if c.err != nil {
		return
	}
	for _, node := range c.master {
		log.Infof("add slots to %s %d", node.name, len(node.slots))
		err := node.addSlots(node.slots)
		if err != nil {
			println("Add slot err")
		}
	}
}

func (c *Cluster) consistent() bool {
	if c.err != nil {
		return false
	}
	nodeSlot := make(map[int64]*Node)
	for _, node := range c.nodes {
		slotNum := 0
		nodes := node.Nodes()
		for _, node := range nodes {
			for _, slot := range node.slots {
				tmp, ok := nodeSlot[slot]
				if !ok {
					nodeSlot[slot] = node
				} else if tmp.name != node.name {
					return false
				}
				slotNum++
			}
		}
		if slotNum != clusterCount {
			return false
		}
	}
	return true
}

// Create create cluster.
func (c *Cluster) create() (err error) {
	c.check()
	c.initSlot()
	c.addSlots()
	c.setConfigEpoch()
	c.join()
	for !c.consistent() {
		time.Sleep(time.Second)
		log.Info("wait cluster to consistent")
	}
	c.setSlaves()
	return c.err
}

func (c *Cluster) addNode(ip, port string) (err error) {
	log.Infof("add node %s:%s into cluster", ip, port)
	return c.nodes[0].meet(ip, port)
}

func (c *Cluster) deleteNode(addr string) (err error) {
	var otherMaster []*Node
	var del *Node
	for _, node := range c.nodes {
		if node.isMaster() && node.addr() != addr {
			otherMaster = append(otherMaster, node)
		}
		if node.addr() == addr {
			del = node
		}
	}
	//if master ,migrate slot to other master.
	if del.isMaster() {
		var start int
		dispatch := divide(len(del.slots), len(otherMaster))
		for i, node := range otherMaster {
			for _, slot := range del.slots[start : start+dispatch[i]] {
				migrateSlot(del, node, slot)
			}
			start += dispatch[i]
		}
	}
	delNode(c.nodes, del)
	return
}

func (c *Cluster) fixSlot() {
	for _, m := range c.master {
		m.fixNode()
	}
}

func (c *Cluster) fillSlot() {
	slots := make([]bool, 16384)
	var count int
	for _, m := range c.master {
		for _, s := range m.slots {
			slots[s] = true
			count++
		}
	}
	miss := clusterCount - count
	dispatch := divide(miss, len(c.master))
	var j int64
	for i, m := range c.master {
		var add []int64
		for ; j < clusterCount; j++ {
			if !slots[i] {
				add = append(add, j)
			}
			if len(add) == dispatch[i] {
				m.addSlots(add)
				break
			}
		}
	}
	return
}

func delNode(nodes []*Node, del *Node) {
	for _, n := range nodes {
		if n.name == del.name || n.slaveof == del.name {
			continue
		}
		n.forget(del)
	}
}
func cluster(cluster string) (c *Cluster, err error) {
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
