package enri

import (
	"bufio"
	"bytes"
	"fmt"
	"overlord/pkg/log"
	"overlord/pkg/myredis"
	"strconv"
	"strings"
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

func (n *Node) String() string {
	return fmt.Sprintf("name:%s ip:%s port:%s role %v slots %d", n.name, n.ip, n.port, n.role, len(n.slots))
}
func (n *Node) setConfigEpoch() (err error) {
	_, err = n.conn.Exec("CLUSTER SET_CONFIG_EPOCH 1")
	return err
}

func (n *Node) meet(ip, port string) (err error) {
	_, err = n.conn.Exec(fmt.Sprintf("CLUSTER MEET %s %s", ip, port))
	return
}

func (n *Node) setSlave() {
	n.conn.Exec(fmt.Sprintf("CLUSTER REPLICATE %s", n.slaveof))
	log.Infof("set %s slaveof %s", n.name, n.slaveof)
}

// Info get node info by cluster info.
func (n *Node) Info() (res map[string]string) {
	resp, err := n.conn.Exec("CLUSTER INFO")
	if err != nil {
		return
	}
	res = make(map[string]string)
	buf := bufio.NewReader(bytes.NewBuffer(resp.Data))
	for {
		line, _, err := buf.ReadLine()
		if err != nil {
			break
		}
		kv := bytes.Split(line, []byte(":"))
		if len(kv) != 2 {
			break
		}
		res[string(kv[0])] = string(kv[1])
	}
	return
}

func (n *Node) addSlots(slots []int64) (err error) {
	_, err = n.conn.Exec(fmt.Sprintf("CLUSTER ADDSLOTS %s", joinInts(slots, ' ')))
	return
}

func (n *Node) setSlot(state string, nodeid string, slot int64) (err error) {
	_, err = n.conn.Exec(fmt.Sprintf("CLUSTER SETSLOT %d %s %s", slot, state, nodeid))
	return
}

func (n *Node) setSlotStable(slot int64) (err error) {
	_, err = n.conn.Exec(fmt.Sprintf("CLUSTER SETSLOT %d STABLE", slot))
	return
}

func (n *Node) keysInSlot(slot int64) (keys []string, err error) {
	resp, err := n.conn.Exec(fmt.Sprintf("CLUSTER GETKEYSINSLOT %d %d", slot, 100))
	if err != nil {
		return
	}
	for _, key := range resp.Array {
		keys = append(keys, string(key.Data))
	}
	return
}

func (n *Node) forget(node *Node) (err error) {
	_, err = n.conn.Exec(fmt.Sprintf("CLUSTER FORGET %s", node.name))
	return
}

func (n *Node) fixNode() {
	for slot, node := range n.migrating {
		target := n.nodes[node]
		if _, ok := target.importing[slot]; ok {

		}
	}
}

func (n *Node) migrate(dst string, key []string) (err error) {
	const (
		timeout = 500
	)
	_, err = n.conn.Exec(fmt.Sprintf("MIGRATE %s  0 %d KEYS %s", dst, timeout, strings.Join(key, " ")))
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
		log.Infof("Nodes err %v", err)
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
			log.Infof("NewNode err %v", err)
			return
		}
		if bytes.Contains(fields[2], []byte("master")) {
			node.role = roleMaster
		} else {
			node.role = roleSalve

		}
		if bytes.Contains(fields[2], []byte("self")) {
			node.self = true
		}
		if !bytes.Equal(fields[3], []byte{'-'}) {
			node.slaveof = string(fields[3])
		}

		for _, content := range fields[8:] {
			if bytes.Contains(content, []byte("->-")) {
				migrate := bytes.Split(content[:len(content)], []byte("->-"))
				slot, _ := strconv.ParseInt(string(migrate[0]), 10, 64)
				node.migrating[slot] = string(migrate[1])
			} else if bytes.Contains(content, []byte("-<-")) {
				migrate := bytes.Split(content[:len(content)], []byte("-<-"))
				slot, _ := strconv.ParseInt(string(migrate[0]), 10, 64)
				node.importing[slot] = string(migrate[1])
			} else {
				var slot []int64
				scope := bytes.Split(content[:len(content)], []byte("-"))
				start, _ := strconv.ParseInt(string(scope[0]), 10, 64)
				slot = append(slot, start)
				if len(scope) == 2 {
					end, _ := strconv.ParseInt(string(scope[1]), 10, 64)
					for i := start + 1; i <= end; i++ {
						slot = append(slot, i)
					}
					node.slots = slot
				}
			}
		}
		n.nodes[node.name] = node
		nodes = append(nodes, node)
	}
}

// check if node is valid to add into cluster.
func (n *Node) valid() bool {
	info := n.Info()
	known, ok := info["cluster_known_nodes"]
	if !ok || known != "1" {

		return false
	}
	return true
}
