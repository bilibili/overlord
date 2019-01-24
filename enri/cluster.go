package enri

import (
	"bufio"
	"bytes"
	"log"
	"strconv"

	"overlord/pkg/myredis"
)

// Role cluster role
type Role int8

const (
	master Role = iota
	salve
)

// NewNode new node by addr.
func NewNode(addr string) (n *Node, err error) {
	ip, port, err := parseAddr(addr)
	if err != nil {
		log.Printf("NewNode err %v", err)
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

// Init init node info.
func (n *Node) Init() {
	for _, node := range n.Nodes() {
		if node.self {
			n.name = node.name
			n.slaveof = node.slaveof
			n.slots = node.slots
		}
	}
}

// Nodes get nodes from node conn.
func (n *Node) Nodes() (nodes []*Node) {
	resp, err := n.conn.Exec("CLUSTER NODES")
	if err != nil {
		log.Printf("Nodes err %v", err)
		return
	}
	nodes = n.parseNodes(resp.Data)
	return
}

func (n *Node) parseNodes(data []byte) (nodes []*Node) {
	buf := bufio.NewReader(bytes.NewBuffer(data))
	for {
		line, _, err := buf.ReadLine()
		if err != nil {
			return
		}
		fields := bytes.Split(line, []byte{' '})
		if len(fields) < 8 {
			return
		}
		addr := bytes.Split(fields[1], []byte{'@'})
		node, err := NewNode(string(addr[0]))
		node.name = string(fields[0])
		if err != nil {
			log.Printf("NewNode err %v", err)
			return
		}
		if bytes.Contains(fields[2], []byte("master")) {
			node.role = master
		} else {
			node.role = salve

		}
		if bytes.Contains(fields[2], []byte("self")) {
			node.self = true
		}
		if !bytes.Equal(fields[3], []byte{'-'}) {
			node.slaveof = string(fields[3])
		}
		for _, content := range fields[8:] {
			if bytes.Contains(content, []byte("->-")) {
				migrate := bytes.Split(content[1:len(content)-1], []byte("->-"))
				slot, _ := strconv.ParseInt(string(migrate[0]), 10, 64)
				node.migrating[slot] = string(migrate[1])
			} else if bytes.Contains(content, []byte("-<-")) {
				migrate := bytes.Split(content[1:len(content)-1], []byte("-<-"))
				slot, _ := strconv.ParseInt(string(migrate[0]), 10, 64)
				node.importing[slot] = string(migrate[1])
			} else {
				scope := bytes.Split(content[1:len(content)-1], []byte("-"))
				start, _ := strconv.ParseInt(string(scope[0]), 10, 64)
				node.slots = append(node.slots, start)
				if len(scope) == 2 {
					end, _ := strconv.ParseInt(string(scope[1]), 10, 64)
					for i := start + 1; i <= end; i++ {
						node.slots = append(node.slots, i)
					}
				}
			}
		}
		n.nodes[node.name] = node
		nodes = append(nodes, node)
	}
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
		for j := slots[i][0]; j < slots[i][1]; j++ {
			master.slots = append(master.slots, int64(j))
		}
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
		first.meet(node.ip + node.port)
	}
}

func (c *Cluster) setSlaves() {
	for _, slave := range c.salve {
		slave.setSlave()
	}
}
