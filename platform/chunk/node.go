package chunk

import (
	"fmt"
	"overlord/pkg/log"
	"strings"
)

// Node is the type for a cache node.
type Node struct {
	Name string
	Port int
	Role string

	// Node Run ID
	RunID   string
	SlaveOf string
	Slots   []Slot
}

// 0c415ea9a9244803d33f8ce97cf6f5b30f862904 127.0.0.1:7020@17020 master - 0 1540462008000 11 connected 13654-15018
const nodesConfLine = "%040s %s:%d@%d %s %s 0 0 0 connected %s\n"

// IntoConfLine will genenrate line for this node in nodes.conf
func (n *Node) IntoConfLine(myself bool) string {
	flags := n.Role
	if myself {
		flags = "myself," + flags
	}
	slots := make([]string, len(n.Slots))
	for i, s := range n.Slots {
		slots[i] = s.String()
	}
	slaveOf := n.SlaveOf
	if n.Role == RoleSlave {
		slaveOf = fmt.Sprintf("%040s", n.SlaveOf)
	}

	line := fmt.Sprintf(nodesConfLine,
		n.RunID, n.Name, n.Port, n.Port+10000,
		flags, slaveOf, strings.Join(slots, " "))
	log.Infof("generated nodes.conf line as: %s", line)
	return line
}

func (n *Node) String() string {
	return fmt.Sprintf("Node<Name=%s, Port=%d, Role=%s>", n.Name, n.Port, n.Role)
}

// Addr get the node addr contians ip:port
func (n *Node) Addr() string {
	return fmt.Sprintf("%s:%d", n.Name, n.Port)
}

const epochSet = "vars currentEpoch 0 lastVoteEpoch 0"

// GenNodesConfFile will gennerate nodes.conf file content
func GenNodesConfFile(name string, port int, chunks []*Chunk) string {
	var sb strings.Builder
	for _, chunk := range chunks {
		for _, node := range chunk.Nodes {
			myself := node.Name == name && node.Port == port
			_, _ = sb.WriteString(node.IntoConfLine(myself))
		}
	}
	_, _ = sb.WriteString(epochSet)
	return sb.String()
}
