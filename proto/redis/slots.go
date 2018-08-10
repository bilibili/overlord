package redis

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
)

const slotsCount = 16384

// errors
var (
	ErrAbsentField   = errors.New("Node fields is absent")
	ErrEmptyNodeLine = errors.New("empty line of cluster nodes")
	ErrBadReplyType  = errors.New("bad reply type")
)
var (
	roleMaster = "master"
	roleSlave  = "slave"
)

// ParseSlots must be call as "CLSUTER NODES" response
func ParseSlots(data []byte) (*NodeSlots, error) {
	br := bufio.NewReader(bytes.NewBuffer(data))
	lines := []string{}
	for {
		// NOTICE: we assume that each line is not longer
		// than 65535.
		token, _, err := br.ReadLine()
		if err != nil && err != io.EOF {
			return nil, err
		}
		if len(token) != 0 {
			lines = append(lines, string(token))
		}
		if err == io.EOF {
			break
		}
	}
	nodes := make(map[string]*Node)
	slots := make([]string, slotsCount)
	slaveSlots := make([][]string, slotsCount)
	masterIDMap := make(map[string]*Node)
	// full fill master slots
	for _, line := range lines {
		node, err := parseNode(line)
		if err != nil {
			return nil, err
		}
		nodes[node.Addr()] = node
		subSlots := node.Slots()
		if node.Role() != roleMaster {
			continue
		}
		masterIDMap[node.ID] = node
		for _, slot := range subSlots {
			slots[slot] = node.Addr()
		}
	}
	// full fill slave slots
	for _, node := range nodes {
		if node.Role() != roleSlave {
			continue
		}
		if mn, ok := masterIDMap[node.SlaveOf()]; ok {
			for _, slot := range mn.Slots() {
				if slaveSlots[slot] == nil {
					slaveSlots[slot] = []string{}
				}
				slaveSlots[slot] = append(slaveSlots[slot], node.Addr())
			}
		}
	}
	return &NodeSlots{nodes: nodes, slots: slots, slaveSlots: slaveSlots}, nil
}

// NodeSlots is the slots map collections.
type NodeSlots struct {
	nodes      map[string]*Node
	slaveSlots [][]string
	slots      []string
}

// GetSlots will return all the 16384 Slots with Address.
func (ns *NodeSlots) GetSlots() []string {
	return ns.slots
}

// GetSlaveSlots returns all slots slaves of 16384 slots.
// For one-to-many arch, slaves may more than one.
func (ns *NodeSlots) GetSlaveSlots() [][]string {
	return ns.slaveSlots
}

// GetNodes returns all the Node address of backend.
func (ns *NodeSlots) GetNodes() []*Node {
	nodes := make([]*Node, len(ns.nodes))
	idx := 0
	for _, val := range ns.nodes {
		nodes[idx] = val
		idx++
	}
	return nodes
}

// GetMasters return all the Masters address.
func (ns *NodeSlots) GetMasters() []string {
	masters := make([]string, 0)
	for _, node := range ns.GetNodes() {
		if node.Role() == roleMaster {
			masters = append(masters, node.Addr())
		}
	}
	return masters
}

// Node is a struct for each CLUSTER NODES response line.
type Node struct {
	// 有别于 runID
	ID   string
	addr string
	// optional port
	gossipAddr string
	// Role is the special flag
	role        string
	flags       []string
	slaveOf     string
	pingSent    int
	pongRecv    int
	configEpoch int
	linkState   string
	slots       []int
}

func parseNode(line string) (*Node, error) {
	if len(strings.TrimSpace(line)) == 0 {
		return nil, ErrEmptyNodeLine
	}
	fields := strings.Fields(line)
	if len(fields) < 8 {
		return nil, ErrAbsentField
	}
	n := &Node{}
	n.setID(fields[0])
	n.setAddr(fields[1])
	n.setFlags(fields[2])
	n.setSlaveOf(fields[3])
	n.setPingSent(fields[4])
	n.setPongRecv(fields[5])
	n.setConfigEpoch(fields[6])
	n.setLinkState(fields[7])
	n.setSlots(fields[8:]...)
	// i++
	return n, nil
}

func (n *Node) setID(val string) {
	n.ID = strings.TrimSpace(val)
}

func (n *Node) setAddr(val string) {
	trimed := strings.TrimSpace(val)
	// adaptor with 4.x
	splited := strings.Split(trimed, "@")
	n.addr = splited[0]
	if len(splited) == 2 {
		asp := strings.Split(n.addr, ":")
		n.gossipAddr = asp[0] + splited[1]
	}
}

func (n *Node) setFlags(val string) {
	flags := strings.Split(val, ",")
	n.flags = flags
	if strings.Contains(val, roleMaster) {
		n.role = roleMaster
	} else if strings.Contains(val, "slave") {
		n.role = roleSlave
	}
}

func (n *Node) setSlaveOf(val string) {
	n.slaveOf = val
}

func (n *Node) setPingSent(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.pingSent = 0
	}
	n.pingSent = ival
}

func (n *Node) setPongRecv(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.pongRecv = 0
	}
	n.pongRecv = ival
}

func (n *Node) setConfigEpoch(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.configEpoch = 0
	}
	n.configEpoch = ival
}

func (n *Node) setLinkState(val string) {
	n.linkState = val
}

func (n *Node) setSlots(vals ...string) {
	slots := []int{}
	for _, val := range vals {
		subslots, ok := parseSlotField(val)
		if ok {
			slots = append(slots, subslots...)
		}
	}
	//sort.IntSlice(slots).Sort()
	n.slots = slots
}

func parseSlotField(val string) ([]int, bool) {
	if len(val) == 0 || val == "-" {
		return nil, false
	}

	// for slot field: `[15495->-9a44630c1dbbf7c116e90f21d1746198d3a1305a]`
	// we ignore for slots which is IMPORTING.
	if strings.HasPrefix(val, "[") && strings.HasSuffix(val, "]") {
		if strings.Contains(val, "->-") {
			// MIGRATING, slots need store in there
			vsp := strings.SplitN(val, "->-", 2)
			slot, err := strconv.Atoi(strings.TrimLeft(vsp[0], "["))
			if err != nil {
				return nil, false
			}
			return []int{slot}, true
		}
		return nil, false
	}

	vsp := strings.SplitN(val, "-", 2)
	begin, err := strconv.Atoi(vsp[0])
	if err != nil {
		return nil, false
	}
	// for slot field: 15495
	if len(vsp) == 1 {
		return []int{begin}, true
	}
	// for slot field: 0-1902
	end, err := strconv.Atoi(vsp[1])
	if err != nil {
		return nil, false
	}
	if end < begin {
		return nil, false
	}
	slots := []int{}
	for i := begin; i <= end; i++ {
		slots = append(slots, i)
	}
	//sort.IntSlice(slots).Sort()
	return slots, true
}

// Addr is a getter of attribute addr.
func (n *Node) Addr() string {
	return n.addr
}

// Role is a getter of attribute role.
func (n *Node) Role() string {
	return n.role
}

// SlaveOf is a getter of attribute slaveOf.
func (n *Node) SlaveOf() string {
	return n.slaveOf
}

// Flags is a getter of attribute flags.
func (n *Node) Flags() []string {
	return n.flags
}

// Slots is a getter of attribute slots.
func (n *Node) Slots() []int {
	return n.slots
}
