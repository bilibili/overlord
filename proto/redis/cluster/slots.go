package cluster

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
)
var (
	roleMaster = "master"
	roleSlave  = "slave"
)

// parseSlots must be call as "CLSUTER NODES" response.
func parseSlots(data []byte) (*nodeSlots, error) {
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
	nodes := make(map[string]*node)
	slots := make([]string, slotsCount)
	slaveSlots := make([][]string, slotsCount)
	masterIDMap := make(map[string]*node)
	// full fill master slots
	for _, line := range lines {
		node, err := parseNode(line)
		if err != nil {
			return nil, err
		}
		nodes[node.addr] = node
		subSlots := node.slots
		if node.role != roleMaster {
			continue
		}
		masterIDMap[node.ID] = node
		for _, slot := range subSlots {
			slots[slot] = node.addr
		}
	}
	// full fill slave slots
	for _, node := range nodes {
		if node.role != roleSlave {
			continue
		}
		if mn, ok := masterIDMap[node.slaveOf]; ok {
			for _, slot := range mn.slots {
				if slaveSlots[slot] == nil {
					slaveSlots[slot] = []string{}
				}
				slaveSlots[slot] = append(slaveSlots[slot], node.addr)
			}
		}
	}
	return &nodeSlots{nodes: nodes, slots: slots, slaveSlots: slaveSlots}, nil
}

// nodeSlots is the slots map collections.
type nodeSlots struct {
	nodes      map[string]*node
	slaveSlots [][]string
	slots      []string
}

// getMasters return all the Masters address.
func (ns *nodeSlots) getMasters() []string {
	masters := make([]string, 0)
	for _, node := range ns.nodes {
		if node.role == roleMaster {
			masters = append(masters, node.addr)
		}
	}
	return masters
}

// node is a struct for each CLUSTER NODES response line.
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
