package myredis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"overlord/lib/chunk"
	"overlord/lib/log"
	"strconv"
	"strings"
)

// errors
var (
	ErrNoNode    = errors.New("node not exists in cluster")
	ErrTrashNode = errors.New("trash nodes")
)

// New create client with singleton node.
func New() *Client {
	return &Client{cluster: &cluster{make(map[string]*node)}}
}

// NewWithChunks create new Client
func NewWithChunks(chunks []*chunk.Chunk) *Client {
	return &Client{chunks: chunks, cluster: &cluster{make(map[string]*node)}}
}

// Client is the myredis manager client.
type Client struct {
	chunks  []*chunk.Chunk
	cluster *cluster
}

// SetChunks set the chunks as given
func (c *Client) SetChunks(chunks []*chunk.Chunk) {
	c.chunks = chunks
}

// Close will close the whole client
func (c *Client) Close() error {
	var err error
	for _, node := range c.cluster.addrMap {
		innerErr := node.Close()
		if err == nil {
			err = innerErr
		}
	}
	return err
}

// Info get the info of client
func (c *Client) Info(node string) (map[string]string, error) {
	n := c.cluster.getNode(node)
	cmd, err := n.execute("INFO ALL")
	if err != nil {
		return nil, err
	}
	info := make(map[string]string)
	lines := strings.Split(string(cmd.Reply.Data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "#") {
			continue
		}
		lsp := strings.Split(line, ":")
		info[lsp[0]] = lsp[1]
	}

	return info, nil
}

// Execute will trying to execute command to the given node.
func (c *Client) Execute(node, command string) (*Command, error) {
	n := c.cluster.getNode(node)
	if n == nil {
		return nil, ErrNoNode
	}

	cmd, err := n.execute(command)
	if err != nil {
		oldN := c.cluster.removeNode(node)
		if oldN != nil {
			_ = oldN.Close()
		}
	}

	return cmd, err
}

// IsConsistent will trying to to check if the cluster nodes info is consistent.
func (c *Client) IsConsistent() (bool, error) {
	var latest []string
	for _, cc := range c.chunks {
		for _, n := range cc.Nodes {
			node := c.cluster.getNode(n.Addr())
			command, err := node.execute("CLUSTER NODES")
			if err != nil {
				return false, err
			}

			if command.Reply.RType != RespBulk {
				return false, fmt.Errorf("get wrong reply of %s %s", n.Addr(), strconv.Quote(string(command.Reply.Data)))
			}

			slots, err := parseSlots(command.Reply.Data)
			if err != nil {
				return false, err
			}
			if latest == nil {
				latest = slots
				continue
			}

			for i := range latest {
				if latest[i] != slots[i] {
					return false, nil
				}
			}
		}
	}

	return true, nil
}

// BumpEpoch will bump epoch to each master
func (c *Client) BumpEpoch() error {
	for _, cc := range c.chunks {
		for _, node := range cc.Nodes {
			role := chunk.RoleMaster
			cmd, err := c.Execute(node.Addr(), "INFO REPLICATION")
			if err != nil {
				return err
			}
			content := string(cmd.Reply.Data)
			lsp := strings.Split(content, "\n")
			for _, line := range lsp {
				if strings.Contains(line, "role") {
					if !strings.Contains(line, chunk.RoleMaster) {
						role = chunk.RoleSlave
					}
					break
				}
			}

			if role == chunk.RoleMaster {
				_, err := c.Execute(node.Addr(), "CLUSTER BUMPEPOCH")
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// IsBalanced will create check if the cluster is balanced
func (c *Client) IsBalanced() (bool, error) {
	for _, cc := range c.chunks {
		for _, node := range cc.Nodes {
			if node.Role != chunk.RoleMaster {
				continue
			}

			cmd, err := c.Execute(node.Addr(), "INFO REPLICATION")
			if err != nil {
				return false, err
			}
			content := string(cmd.Reply.Data)
			lsp := strings.Split(content, "\n")
			for _, line := range lsp {
				if strings.Contains(line, "role") && !strings.Contains(line, chunk.RoleMaster) {
					log.Infof("check balance fail due on %s with lines %s", node.Addr(), content)
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// TryBalance will trying to balance the whole cluster.
func (c *Client) TryBalance() error {
	for _, cc := range c.chunks {
		for _, node := range cc.Nodes {
			if node.Role != chunk.RoleMaster {
				continue
			}

			cmd, err := c.Execute(node.Addr(), "CLUSTER FAILOVER FORCE")
			if err != nil {
				return err
			}
			if cmd.Reply.RType == RespError {
				if bytes.Contains(cmd.Reply.Data, []byte("You should send CLUSTER FAILOVER to a slave")) {
					continue
				}
				return fmt.Errorf("fail with redis error %s", strconv.Quote(string(cmd.Reply.Data)))
			}
		}
	}
	log.Infof("succeed balanced executed to %v", c.chunks)
	return nil
}

// Conn is the singleton connection to backend
type Conn struct {
	conn *node
}

// NewConn create new connection by given addr
func NewConn(addr string) *Conn {
	return &Conn{
		conn: newNode(addr),
	}
}

// Ping will execute ping command
func (c *Conn) Ping() (err error) {
	_, err = c.conn.execute("ping")
	return
}

// Close will close the given connection
func (c *Conn) Close() error {
	return c.conn.Close()
}

type cluster struct {
	addrMap map[string]*node
}

func (c *cluster) removeNode(name string) *node {
	n := c.addrMap[name]
	delete(c.addrMap, name)
	return n
}

func (c *cluster) getNode(name string) *node {
	// TODO: check and create new cluster
	if n, ok := c.addrMap[name]; ok {
		return n
	}

	n := newNode(name)
	c.addrMap[name] = n
	return n
}

// TODO :reconnect on err
func newNode(addr string) *node {
	conn, err := net.Dial("tcp", addr)
	n := &node{conn: conn, err: err}
	n.wr = bufio.NewReadWriter(bufio.NewReader(n.conn), bufio.NewWriter(n.conn))
	return n
}

type node struct {
	wr   *bufio.ReadWriter
	conn net.Conn
	err  error
}

func (n *node) Close() error {
	if n.conn == nil {
		return nil
	}
	return n.conn.Close()
}

func (n *node) execute(command string) (*Command, error) {
	if n.err != nil {
		return nil, n.err
	}

	cmd := NewCmd(command)
	n.err = cmd.execute(n)
	return cmd, n.err
}

func parseSlots(Data []byte) ([]string, error) {
	dataStr := string(Data)
	lines := strings.Split(dataStr, "\n")
	slots := make([]string, 16384)

	if len(lines) < 3 {
		log.Warnf("get trash node as dataStr %v", lines)
		return nil, ErrTrashNode
	}

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		if strings.Contains(line, chunk.RoleSlave) {
			continue
		}

		if strings.Contains(line, "fail") {
			continue
		}

		lsp := strings.Split(line, " ")
		if len(lsp) < 8 {
			continue
		}
		addr := lsp[1]
		if strings.Contains(addr, "@") {
			addrsp := strings.Split(addr, "@")
			addr = addrsp[0]
		}

		for _, item := range lsp[8:] {
			if strings.Contains(item, "-<-") {
				continue
			}

			if strings.Contains(item, "->-") {
				vsp := strings.SplitN(item, "->-", 2)
				slot, err := strconv.Atoi(strings.TrimLeft(vsp[0], "["))
				if err != nil {
					return nil, err
				}
				slots[slot] = addr
			}

			vsp := strings.SplitN(item, "-", 2)
			begin, err := strconv.Atoi(vsp[0])
			if err != nil {
				return nil, err
			}

			if len(vsp) == 1 {
				slots[begin] = addr
			}

			end, err := strconv.Atoi(vsp[1])
			if err != nil {
				return nil, err
			}
			for i := begin; i <= end; i++ {
				slots[i] = addr
			}
		}
	}
	return slots, nil
}
