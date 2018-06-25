package memcache

import (
	"bytes"
	"io"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/felixhao/overlord/lib/prom"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)
)

type nodeConn struct {
	cluster string
	addr    string
	conn    *libnet.Conn
	bw      *bufio.Writer
	br      *bufio.Reader
	closed  int32

	pinger *mcPinger
}

// NewNodeConn returns node conn.
func NewNodeConn(cluster, addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (nc proto.NodeConn) {
	conn := libnet.DialWithTimeout(addr, dialTimeout, readTimeout, writeTimeout)
	nc = &nodeConn{
		cluster: cluster,
		addr:    addr,
		conn:    conn,
		bw:      bufio.NewWriter(conn),
		br:      bufio.NewReader(conn, nil),
		pinger:  newMCPinger(conn.Dup()),
	}
	return
}

// Ping will send some special command by checking mc node is alive
func (n *nodeConn) Ping() (err error) {
	if n.Closed() {
		err = io.EOF
		return
	}
	err = n.pinger.Ping()
	return
}

// Write write request data into server node.
func (n *nodeConn) Write(m *proto.Message) (err error) {
	if n.Closed() {
		err = errors.Wrap(ErrClosed, "MC Handler handle Msg")
		return
	}
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.Wrap(ErrAssertMsg, "MC Handler handle assert MCMsg")
		return
	}
	_ = n.bw.WriteString(mcr.rTp.String())
	_ = n.bw.Write(spaceBytes)
	if mcr.rTp == RequestTypeGat || mcr.rTp == RequestTypeGats {
		_ = n.bw.Write(mcr.data) // NOTE: exp time
		_ = n.bw.Write(spaceBytes)
		_ = n.bw.Write(mcr.key)
		_ = n.bw.Write(crlfBytes)
	} else {
		_ = n.bw.Write(mcr.key)
		_ = n.bw.Write(mcr.data)
	}
	if err = n.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC Handler handle flush Msg bytes")
	}
	return
}

// Read reads response bytes from server node.
func (n *nodeConn) Read(m *proto.Message) (err error) {
	if n.Closed() {
		err = errors.Wrap(ErrClosed, "MC Handler handle Msg")
		return
	}
	// TODO: this read was only support read one key's result
	n.br.ResetBuffer(m.Buffer())
	// request
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.Wrap(ErrAssertMsg, "MC Handler handle assert MCMsg")
		return
	}
	bs, err := n.br.ReadUntil(delim)
	if err != nil {
		err = errors.Wrap(err, "MC Handler handle read response bytes")
		return
	}
	// n.br.Advance(-len(bs)) // NOTE: for response bytes
	mcr.data = bs
	if _, ok := withValueTypes[mcr.rTp]; !ok {
		return
	}
	if bytes.Equal(bs, endBytes) {
		prom.Miss(n.cluster, n.addr)
		return
	}
	prom.Hit(n.cluster, n.addr)
	// value length
	length, err := findLength(bs, mcr.rTp == RequestTypeGets || mcr.rTp == RequestTypeGats)
	if err != nil {
		err = errors.Wrap(err, "MC Handler while parse length")
		return
	}
	n.br.Advance(-len(bs))
	rlen := len(bs) + length + 2 + len(endBytes)
	if bs, err = n.br.ReadFull(rlen); err != nil {
		err = errors.Wrap(err, "MC Handler while reading length full data")
		return
	}
	// n.br.Advance(-rlen) // NOTE: for response bytes
	mcr.data = bs
	return
}

func (n *nodeConn) Close() error {
	if atomic.CompareAndSwapInt32(&n.closed, handlerOpening, handlerClosed) {
		_ = n.pinger.Close()
		n.pinger = nil
		err := n.conn.Close()
		return err
	}
	return nil
}

func (n *nodeConn) Closed() bool {
	return atomic.LoadInt32(&n.closed) == handlerClosed
}
