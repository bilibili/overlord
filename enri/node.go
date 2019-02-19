package enri

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"

	"overlord/pkg/log"
	"overlord/pkg/myredis"
)

// Node present node info.
type Node struct {
	name        string
	ip          string
	port        string
	role        Role
	self        bool
	slaveof     string
	configEpoch int64
	nodes       map[string]*Node
	slots       []int64
	migrating   map[int64]string
	importing   map[int64]string
	conn        *myredis.Conn
}

func (n *Node) String() string {
	return fmt.Sprintf("name:%s ip:%s port:%s role %v slots %d", n.name, n.ip, n.port, n.role, len(n.slots))
}
func (n *Node) setConfigEpoch() (err error) {
	_, err = n.conn.Exec(myredis.NewCmd("CLUSTER").Arg([]string{"SET_CONFIG_EPOCH", "1"}...))
	return err
}

func (n *Node) meet(ip, port string) (err error) {
	_, err = n.conn.Exec(myredis.NewCmd("CLUSTER").Arg([]string{"MEET", ip, port}...))
	return
}

func (n *Node) setSlave() {
	resp, err := n.conn.Exec(myredis.NewCmd("CLUSTER").Arg([]string{"REPLICATE", n.slaveof}...))
	log.Infof("set %s slaveof %s resp %s,err %v", n.name, n.slaveof, string(resp.Data), err)
}

// Info get node info by cluster info.
func (n *Node) Info() (res map[string]string) {
	resp, err := n.conn.Exec(myredis.NewCmd("CLUSTER").Arg("INFO"))
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

func (n *Node) clusterState() bool {
	info := n.Info()
	return info["cluster_state"] == "ok"
}

func (n *Node) addSlots(slots []int64) (err error) {
	cmd := myredis.NewCmd("CLUSTER").Arg("ADDSLOTS")
	for _, slot := range slots {
		cmd = cmd.Arg(strconv.FormatInt(slot, 10))
	}
	resp, err := n.conn.Exec(cmd)
	if err != nil || string(resp.Data) != "OK" {
		log.Errorf("add slot (%v) fail err(%v),resp(%v), ", slots, err, string(resp.Data))
	}
	return
}

func (n *Node) setSlot(state string, nodeid string, slot int64) (err error) {
	_, err = n.conn.Exec(myredis.NewCmd("CLUSTER").Arg([]string{"SETSLOT", strconv.FormatInt(slot, 10), state, nodeid}...))
	return
}

func (n *Node) addr() string {
	return n.ip + ":" + n.port
}
func (n *Node) isMaster() bool {
	return n.role == roleMaster
}
func (n *Node) setSlotStable(slot int64) (err error) {
	_, err = n.conn.Exec(myredis.NewCmd("CLUSTER").Arg([]string{"SETSLOT", strconv.FormatInt(slot, 10), "STABLE"}...))
	return
}

func (n *Node) keysInSlot(slot int64) (keys []string, err error) {
	resp, err := n.conn.Exec(myredis.NewCmd("CLUSTER").Arg([]string{"GETKEYSINSLOT", strconv.FormatInt(slot, 10), "100"}...))
	if err != nil {
		return
	}
	for _, key := range resp.Array {
		keys = append(keys, string(key.Data[:len(key.Data)-2]))
	}
	return
}

func (n *Node) forget(node *Node) (err error) {
	_, err = n.conn.Exec(myredis.NewCmd("CLUSTER").Arg("FORGET").Arg(node.name))
	return
}

func (n *Node) fixNode() {
	for slot, node := range n.migrating {
		target := n.nodes[node]
		if _, ok := target.importing[slot]; ok {
			migrateSlot(n, target, slot)
			continue
		}
		n.setSlotStable(slot)
	}
	for slot, node := range n.importing {
		src := n.nodes[node]
		if _, ok := src.migrating[slot]; ok {
			migrateSlot(src, n, slot)
			continue
		}
		n.setSlotStable(slot)
	}
}

func (n *Node) migrate(ip, port string, key []string) (err error) {
	const (
		timeout = 500
	)
	cmd := myredis.NewCmd("MIGRATE").Arg(ip).Arg(port).Arg("").Arg("0").Arg("500").Arg("KEYS").Arg(key...)
	resp, err := n.conn.Exec(cmd)
	if string(resp.Data) != "OK" {
		log.Errorf("migrate key err resp %v", string(resp.Data))
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
	resp, err := n.conn.Exec(myredis.NewCmd("CLUSTER").Arg("NODES"))
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
		epoch, err := strconv.ParseInt(string(fields[6]), 10, 64)
		if err != nil {
			return
		}
		node.configEpoch = epoch
		var slot []int64
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

				scope := bytes.Split(content[:len(content)], []byte("-"))
				start, _ := strconv.ParseInt(string(scope[0]), 10, 64)
				slot = append(slot, start)
				if len(scope) == 2 {
					end, _ := strconv.ParseInt(string(scope[1]), 10, 64)
					for i := start + 1; i <= end; i++ {
						slot = append(slot, i)
					}

				}
			}
		}
		node.slots = slot
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

func migrateSlot(src, dst *Node, slot int64) {
	log.Infof("start migrate slot(%d) from %s to %s", slot, src.addr(), dst.addr())
	dst.setSlot("IMPORTING", dst.name, slot)
	src.setSlot("MIGRATING", src.name, slot)
	var total int
	for {
		keys, err := src.keysInSlot(slot)
		total += len(keys)
		if err != nil {
			return
		}
		if len(keys) == 0 {
			break
		}
		err = src.migrate(dst.ip, dst.port, keys)
		if err != nil {
			log.Errorf("migrate key to %s fail %v", dst.addr(), err)
		}
	}
	dst.setSlot("NODE", dst.name, slot)
	src.setSlot("NODE", dst.name, slot)
	log.Infof("migrate slot(%d) from %s to %s finish,total keys num %d", slot, src.addr(), dst.addr(), total)
}
