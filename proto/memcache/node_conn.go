package memcache

import (
	"bytes"
	"sync/atomic"
	"time"

	"overlord/lib/bufio"
	libnet "overlord/lib/net"
	"overlord/proto"

	"github.com/pkg/errors"
)

const (
	opened = int32(0)
	closed = int32(1)

	nodeReadBufSize = 2 * 1024 * 1024 // NOTE: 2MB
)

type nodeConn struct {
	cluster string
	addr    string

	conn *libnet.Conn
	bw   *bufio.Writer
	br   *bufio.Reader

	state int32
}

// NewNodeConn returns node conn.
func NewNodeConn(cluster, addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (nc proto.NodeConn) {
	conn := libnet.DialWithTimeout(addr, dialTimeout, readTimeout, writeTimeout)
	nc = &nodeConn{
		cluster: cluster,
		addr:    addr,
		conn:    conn,
		bw:      bufio.NewWriter(conn),
		br:      bufio.NewReader(conn, bufio.Get(nodeReadBufSize)),
	}
	return
}

func (n *nodeConn) Write(m *proto.Message) (err error) {
	if n.Closed() {
		err = errors.WithStack(ErrClosed)
		return
	}
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.WithStack(ErrAssertReq)
		return
	}
	_ = n.bw.Write(mcr.rTp.Bytes())
	_ = n.bw.Write(spaceBytes)
	if mcr.rTp == RequestTypeGat || mcr.rTp == RequestTypeGats {
		_ = n.bw.Write(mcr.data) // NOTE: exp time
		_ = n.bw.Write(spaceBytes)
		_ = n.bw.Write(mcr.key)
		err = n.bw.Write(crlfBytes)
	} else {
		_ = n.bw.Write(mcr.key)
		err = n.bw.Write(mcr.data)
	}
	return
}

func (n *nodeConn) Flush() error {
	if n.Closed() {
		return errors.WithStack(ErrClosed)
	}
	return n.bw.Flush()
}

func (n *nodeConn) Read(m *proto.Message) (err error) {
	if n.Closed() {
		err = errors.WithStack(ErrClosed)
		return
	}
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.WithStack(ErrAssertReq)
		return
	}
REREAD:
	var bs []byte
	if bs, err = n.br.ReadLine(); err == bufio.ErrBufferFull {
		if err = n.br.Read(); err != nil {
			err = errors.WithStack(err)
			return
		}
		goto REREAD
	} else if err != nil {
		err = errors.WithStack(err)
		return
	}
	if _, ok := withValueTypes[mcr.rTp]; !ok || bytes.Equal(bs, endBytes) || bytes.Equal(bs, errorBytes) {
		mcr.data = mcr.data[:0]
		mcr.data = append(mcr.data, bs...)
		return
	}
	var length int
	if length, err = findLength(bs, mcr.rTp == RequestTypeGets || mcr.rTp == RequestTypeGats); err != nil {
		err = errors.WithStack(err)
		return
	}
	ds := length + 2 + len(endBytes)
REREADData:
	var data []byte
	if data, err = n.br.ReadExact(ds); err == bufio.ErrBufferFull {
		if err = n.br.Read(); err != nil {
			err = errors.WithStack(err)
			return
		}
		goto REREADData
	} else if err != nil {
		err = errors.WithStack(err)
		return
	}
	mcr.data = mcr.data[:0]
	mcr.data = append(mcr.data, bs...)
	mcr.data = append(mcr.data, data...)
	return
}

func (n *nodeConn) Close() error {
	if atomic.CompareAndSwapInt32(&n.state, opened, closed) {
		return n.conn.Close()
	}
	return nil
}

func (n *nodeConn) Closed() bool {
	return atomic.LoadInt32(&n.state) == closed
}
