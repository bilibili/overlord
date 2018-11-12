package myredis

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"net"
	"overlord/lib/chunk"
	"strconv"
	"strings"
)

// errors
var (
	ErrNoNode = errors.New("node not exists in cluster")
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
	lines := strings.Split(string(cmd.reply.data), "\n")
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
func (c *Client) Execute(node, command string) (*cmd, error) {
	n := c.cluster.getNode(node)
	if n == nil {
		return nil, ErrNoNode
	}

	return n.execute(command)
}

// IsConsistent will trying to to check if the cluster nodes info is consistent.
func (c *Client) IsConsistent() (bool, error) {
	var latest []byte
	var hash = md5.New()
	for _, cc := range c.chunks {
		for _, n := range cc.Nodes {
			node := c.cluster.getNode(n.Addr())
			command, err := node.execute("CLUSTER NODES")
			if err != nil {
				return false, err
			}

			if command.reply.rtype != respBulk {
				return false, fmt.Errorf("get wrong reply %v", *command.reply)
			}
			slots, err := parseSlots(command.reply.data)
			if err != nil {
				return false, err
			}
			hash.Reset()
			for _, addr := range slots {
				hash.Write([]byte(addr))
			}
			if latest == nil {
				latest = hash.Sum(nil)
				continue
			} else {
				val := hash.Sum(nil)
				if !bytes.Equal(latest, val) {
					return false, nil
				}
			}
		}
	}

	return true, nil
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
			content := string(cmd.reply.data)
			lsp := strings.Split(content, "\n")
			for _, line := range lsp {
				if strings.Contains(line, "role") {
					if !strings.HasSuffix(line, chunk.RoleMaster) {
						return false, nil
					}
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

			cmd, err := c.Execute(node.Addr(), "CLUSTER FAILOVER")
			if err != nil {
				return err
			}
			if cmd.reply.rtype == respError {
				if bytes.Contains(cmd.reply.data, []byte("You should send CLUSTER FAILOVER to a slave")) {
					continue
				}
				return fmt.Errorf("fail with redis error %s", strconv.Quote(string(cmd.reply.data)))
			}
		}
	}
	return nil
}

func (c *Client) Ping(node string) (err error) {
	n := c.cluster.getNode(node)
	_, err = n.execute("ping")
	return
}

type cluster struct {
	addrMap map[string]*node
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

func (n *node) execute(command string) (*cmd, error) {
	if n.err == nil {
		return nil, n.err
	}

	cmd := newCmd(command)
	n.err = cmd.execute(n)
	return cmd, n.err
}

func parseSlots(data []byte) ([]string, error) {
	dataStr := string(data)
	lines := strings.Split(dataStr, "\n")
	slots := make([]string, 16384)

	for _, line := range lines {
		if strings.Contains(line, chunk.RoleSlave) {
			continue
		}

		if strings.Contains(line, "fail") {
			continue
		}

		lsp := strings.Split(line, " ")
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
