package cluster

import (
	errs "errors"
	"overlord/pkg/log"
	"strconv"
	"strings"
)

const slotsCount = 16384

// errors
var (
	ErrAbsentField   = errs.New("Node fields is absent")
	ErrEmptyNodeLine = errs.New("empty line of cluster nodes")
	ErrParseNodeSlot = errs.New("error when parse nodes slots")
)

var (
	roleMyself = "myself"
	roleMaster = "master"
	roleSlave  = "slave"
)

// parseSlots must be call as "CLSUTER NODES" response.
//
// CLUSTER NODES response
// 6b22f87b78cdb181f7b9b1e0298da177606394f7 172.17.0.2:7003@17003 slave 8f02f3135c65482ac00f217df0edb6b9702691f8 0 1532770704000 4 connected
// dff2f7b0fbda82c72d426eeb9616d9d6455bb4ff 172.17.0.2:7004@17004 slave 828c400ea2b55c43e5af67af94bec4943b7b3d93 0 1532770704538 5 connected
// b1798ba2171a4bd765846ddb5d5bdc9f3ca6fdf3 172.17.0.2:7000@17000 master - 0 1532770705458 1 connected 0-5460
// db2dd7d6fbd2a03f16f6ab61d0576edc9c3b04e2 172.17.0.2:7005@17005 slave b1798ba2171a4bd765846ddb5d5bdc9f3ca6fdf3 0 1532770704437 6 connected
// 828c400ea2b55c43e5af67af94bec4943b7b3d93 172.17.0.2:7002@17002 master - 0 1532770704000 3 connected 10923-16383
// 8f02f3135c65482ac00f217df0edb6b9702691f8 172.17.0.2:7001@17001 myself,master - 0 1532770703000 2 connected 5461-10922
func parseSlots(data []byte) (*nodeSlots, error) {
	tmpLines := strings.Split(string(data), "\n")
	lines := make([]string, 0)
	for _, tl := range tmpLines {
		if len(strings.TrimSpace(tl)) != 0 {
			lines = append(lines, tl)
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
		if node.role != roleMaster {
			continue
		}
		subSlots := node.slots
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
	// check slot=>addr
	for slot, addr := range slots {
		if node, ok := nodes[addr]; addr == "" || !ok || node == nil {
			log.Errorf("parse failed slot:%d miss addr, original:%s", slot, string(data))
			return nil, ErrParseNodeSlot
		}
	}
	return &nodeSlots{nodes: nodes, slots: slots, slaveSlots: slaveSlots}, nil
}

// nodeSlots is the slots map collections.
type nodeSlots struct {
	nodes      map[string]*node
	slots      []string
	slaveSlots [][]string
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
	} else if strings.Contains(val, roleSlave) {
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

func (n *node) isNormal() bool {
	for _, f := range n.flags {
		if f != roleMaster && f != roleSlave && f != roleMyself {
			return false
		}
	}
	if n.linkState != "connected" {
		return false
	}
	return true
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
