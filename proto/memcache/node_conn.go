package memcache

import (
	"bytes"
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

	// h.br.ResetBuffer(req.Buf())
	// bs, err := h.br.ReadUntil(delim)
	// if err != nil {
	// 	err = errors.Wrap(err, "MC Handler handle read response bytes")
	// 	return
	// }

	// if _, ok := withDataMsgTypes[mcr.rTp]; ok {
	// 	data, ierr := h.readResponseData(bs)
	// 	err = ierr
	// 	req.Forward(len(data))
	// } else {
	// 	req.Forward(len(bs))
	// }
	return
}

// Read reads response bytes from server node.
func (n *nodeConn) Read(m *proto.Message) (err error) {
	if n.Closed() {
		err = errors.Wrap(ErrClosed, "MC Handler handle Msg")
		return
	}
	mcr, ok := m.Request().(*MCRequest)
	if !ok {
		err = errors.Wrap(ErrAssertMsg, "MC Handler handle assert MCMsg")
		return
	}
	n.br.ResetBuffer(m.Buffer())
	bs, err := n.br.ReadUntil(delim)
	if err != nil {
		err = errors.Wrap(err, "MC Handler handle read response bytes")
		return
	}
	if _, ok := withDataMsgTypes[mcr.rTp]; !ok {
		return
	}
	if bytes.Equal(bs, endBytes) {
		stat.Miss(n.cluster, n.addr)
		return
	}
	stat.Hit(n.cluster, n.addr)
	length, err := findLength(bs, mcr.rTp == RequestTypeGets || mcr.rTp == RequestTypeGats)
	if err != nil {
		err = errors.Wrap(err, "MC Handler while parse length")
		return
	}
	if _, err = n.br.ReadFull(length); err != nil {
		err = errors.Wrap(err, "MC Handler while reading length full data")
	}
	return
}

// func (n *nodeConn) readResponseData(bs []byte) (data []byte, err error) {
// 	if bytes.Equal(bs, endBytes) {
// 		stat.Miss(h.cluster, h.addr)
// 		err = nil
// 		return
// 	}

// 	stat.Hit(h.cluster, h.addr)
// 	c := bytes.Count(bs, spaceBytes)
// 	if c < 3 {
// 		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes split")
// 		return
// 	}

// 	i := bytes.IndexByte(bs, spaceByte) + 1 // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
// 	i = i + bytes.IndexByte(bs[i:], spaceByte) + 1
// 	i = i + bytes.IndexByte(bs[i:], spaceByte) + 1
// 	var high int

// 	if len(bs[i:]) < 2 { // check if bytes length is null
// 		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes check")
// 		return
// 	}

// 	if c == 3 {
// 		// GET/GAT
// 		high = len(bs) - 2
// 	} else {
// 		// GETS/GATS
// 		high = i + bytes.IndexByte(bs[i:], spaceByte)
// 	}

// 	var size int64
// 	if size, err = conv.Btoi(bs[i:high]); err != nil {
// 		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes length")
// 		return
// 	}
// 	if data, err = h.br.ReReadFull(int(size+2), len(bs)); err != nil {
// 		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes data")
// 		return
// 	}
// 	return
// }

func (n *nodeConn) Close() error {
	if atomic.CompareAndSwapInt32(&n.closed, handlerOpening, handlerClosed) {
		return n.conn.Close()
	}
	return nil
}

func (n *nodeConn) Closed() bool {
	return atomic.LoadInt32(&n.closed) == handlerClosed
}
