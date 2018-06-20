package memcache

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/felixhao/overlord/lib/stat"
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

	closed int32
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
	}
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
	n.bw.WriteString(mcr.rTp.String())
	n.bw.Write(spaceBytes)
	if mcr.rTp == RequestTypeGat || mcr.rTp == RequestTypeGats {
		n.bw.Write(mcr.data) // NOTE: exp time
		n.bw.Write(spaceBytes)
		n.bw.Write(mcr.key)
		n.bw.Write(crlfBytes)
	} else {
		n.bw.Write(mcr.key)
		n.bw.Write(mcr.data)
	}
	if err = n.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC Handler handle flush Msg bytes")
		return
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

	if bytes.Equal(bs, endBytes) {
		m.ResetBuffer(n.br.Buffer())
		return
	}

	if _, ok := withDataMsgTypes[mcr.rTp]; !ok {
		m.ResetBuffer(n.br.Buffer())
		m.Buffer().Advance(-len(bs))
		return
	}

	if bytes.Equal(bs, endBytes) {
		stat.Miss(n.cluster, n.addr)
		return
	}

	stat.Hit(n.cluster, n.addr)

	length, err := findLength(bs, mcr.rTp == RequestTypeGets || mcr.rTp == RequestTypeGats)
	fmt.Println("bs:", bs, "rtype:", mcr.rTp.String(), "length:", length, "err:", err)
	// fmt.Printf("bs len:%d bs:%v bs-str:%s length:%d error:%s\n", len(bs), bs, string(bs), length, err)
	if err != nil {
		err = errors.Wrap(err, "MC Handler while parse length")
		return
	}
	rlen := length + 2 + len(endBytes)
	if _, err = n.br.ReadFull(rlen); err != nil {
		err = errors.Wrap(err, "MC Handler while reading length full data")
		return
	}
	m.ResetBuffer(n.br.Buffer())
	m.Buffer().Advance(-len(bs))
	m.Buffer().Advance(-rlen)
	return
}

func (n *nodeConn) Close() error {
	if atomic.CompareAndSwapInt32(&n.closed, handlerOpening, handlerClosed) {
		return n.conn.Close()
	}
	return nil
}

func (n *nodeConn) Closed() bool {
	return atomic.LoadInt32(&n.closed) == handlerClosed
}
