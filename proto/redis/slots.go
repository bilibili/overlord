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
	ErrAbsentField   = errors.New("node fields is absent")
	ErrEmptyNodeLine = errors.New("empty line of cluster nodes")
	ErrBadReplyType  = errors.New("bad reply type")
)
var (
	roleMaster = "master"
	roleSlave  = "slave"
)

// Slots is the container of all slots.
type Slots interface {
	GetSlots() []string
	GetSlaveSlots() [][]string
}

// Nodes is the container caontains Nodes.
type Nodes interface {
	GetNodes() []Node
	GetNodeByAddr(addr string) (Node, bool)
}

// NodeSlots is the export interface of CLUSTER NODES.
type NodeSlots interface {
	Slots
	Nodes
}

// ParseSlots must be call as "CLSUTER NODES" response
func ParseSlots(data []byte) (NodeSlots, error) {
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
	nodes := make(map[string]Node)
	slots := make([]string, slotsCount)
	slaveSlots := make([][]string, slotsCount)
	masterIDMap := make(map[string]Node)
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
	return &nodeSlots{nodes: nodes, slots: slots, slaveSlots: slaveSlots}, nil
}

type nodeSlots struct {
	nodes      map[string]Node
	slaveSlots [][]string
	slots      []string
}

func (ns *nodeSlots) GetSlots() []string {
	return ns.slots
}

func (ns *nodeSlots) GetSlaveSlots() [][]string {
	return ns.slaveSlots
}

func (ns *nodeSlots) GetNodes() []Node {
	nodes := make([]Node, len(ns.nodes))
	idx := 0
	for _, val := range ns.nodes {
		nodes[idx] = val
		idx++
	}
	return nodes
}

func (ns *nodeSlots) GetNodeByAddr(addr string) (Node, bool) {
	if n, ok := ns.nodes[addr]; ok {
		return n, true
	}
	return nil, false
}

type node struct {
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

func parseNode(line string) (*node, error) {
	if len(strings.TrimSpace(line)) == 0 {
		return nil, ErrEmptyNodeLine
	}
	fields := strings.Fields(line)
	if len(fields) < 8 {
		return nil, ErrAbsentField
	}
	n := &node{}
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
func (n *node) setID(val string) {
	n.ID = strings.TrimSpace(val)
}
func (n *node) setAddr(val string) {
	trimed := strings.TrimSpace(val)
	// adaptor with 4.x
	splited := strings.Split(trimed, "@")
	n.addr = splited[0]
	if len(splited) == 2 {
		asp := strings.Split(n.addr, ":")
		n.gossipAddr = asp[0] + splited[1]
	}
}
func (n *node) setFlags(val string) {
	flags := strings.Split(val, ",")
	n.flags = flags
	if strings.Contains(val, roleMaster) {
		n.role = roleMaster
	} else if strings.Contains(val, "slave") {
		n.role = roleSlave
	}
}
func (n *node) setSlaveOf(val string) {
	n.slaveOf = val
}
func (n *node) setPingSent(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.pingSent = 0
	}
	n.pingSent = ival
}
func (n *node) setPongRecv(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.pongRecv = 0
	}
	n.pongRecv = ival
}
func (n *node) setConfigEpoch(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.configEpoch = 0
	}
	n.configEpoch = ival
}
func (n *node) setLinkState(val string) {
	n.linkState = val
}
func (n *node) setSlots(vals ...string) {
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
	vsp := strings.SplitN(val, "-", 2)
	begin, err := strconv.Atoi(vsp[0])
	if err != nil {
		return nil, false
	}
	if len(vsp) == 1 {
		return []int{begin}, true
	}
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
func (n *node) Addr() string {
	return n.addr
}
func (n *node) Role() string {
	return n.role
}
func (n *node) SlaveOf() string {
	return n.slaveOf
}
func (n *node) Flags() []string {
	return n.flags
}
func (n *node) Slots() []int {
	return n.slots
}

// Node is the interface of single redis node.
type Node interface {
	Addr() string
	Role() string
	SlaveOf() string
	Flags() []string
	Slots() []int
}
