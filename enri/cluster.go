package enri

import (
	"errors"
	"overlord/pkg/log"
	"strings"
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

func migrateSlot(src, dst *Node, slot int64) {
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
func (c *Cluster) Create() (err error) {
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

// Add add addrs into cluster
func (c *Cluster) Add(addrs []string) (err error) {
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

func (c *Cluster) addNode(ip, port string) (err error) {
	log.Infof("add node %s:%s into cluster", ip, port)
	return c.nodes[0].meet(ip, port)
}
