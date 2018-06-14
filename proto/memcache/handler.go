package memcache

import (
	"bytes"
	"net"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
	"github.com/felixhao/overlord/lib/conv"
	"github.com/felixhao/overlord/lib/net2"
	"github.com/felixhao/overlord/lib/pool"
	"github.com/felixhao/overlord/lib/stat"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)

	handlerWriteBufferSize = 8 * 1024   // NOTE: write command, so relatively small
	handlerReadBufferSize  = 128 * 1024 // NOTE: read data, so relatively large
)

var (
	errMissRequest = errors.New("missing request")
)

type handler struct {
	cluster string
	addr    string
	conn    net.Conn
	br      *bufio.Reader
	bw      *bufio.Writer
	bss     [][]byte

	closed int32
}

// Dial returns pool Dial func.
func Dial(cluster, addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (dial func() (pool.Conn, error)) {
	dial = func() (pool.Conn, error) {
		conn, err := net2.DialWithTimeout(addr, dialTimeout, readTimeout, writeTimeout)
		if err != nil {
			return nil, err
		}
		h := &handler{
			cluster: cluster,
			addr:    addr,
			conn:    conn,
			bw:      bufio.NewWriterSize(conn, handlerWriteBufferSize),
			br:      bufio.NewReaderSize(conn, handlerReadBufferSize),
			bss:     make([][]byte, 3), // NOTE: like: 'VALUE a_11 0 0 3\r\naaa\r\nEND\r\n'
		}
		return h, nil
	}
	return
}

// Handle call server node by Msg and read response returned.
func (h *handler) Handle(req *proto.Msg) (err error) {
	if h.Closed() {
		err = errors.Wrap(ErrClosed, "MC Handler handle Msg")
		return
	}
	mcr, ok := req.Proto().(*MCMsg)
	if !ok {
		err = errors.Wrap(ErrAssertMsg, "MC Handler handle assert MCMsg")
		return
	}

	h.bw.WriteString(mcr.rTp.String())
	h.bw.WriteByte(spaceByte)
	if mcr.rTp == MsgTypeGat || mcr.rTp == MsgTypeGats {
		h.bw.Write(mcr.data) // NOTE: exptime
		h.bw.WriteByte(spaceByte)
		h.bw.Write(mcr.key)
		h.bw.Write(crlfBytes)
	} else {
		h.bw.Write(mcr.key)
		h.bw.Write(mcr.data)
	}
	if err = h.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC Handler handle flush Msg bytes")
		return
	}

	bss := make([][]byte, 2)

	// TODO: reset bytes buffer to reuse the bytes
	h.br.ResetBuffer(req.Buf())
	bs, err := h.br.ReadUntil(delim)
	if err != nil {
		err = errors.Wrap(err, "MC Handler handle read response bytes")
		return
	}
	if _, ok := retrievalRequestTypes[mcr.rTp]; ok {
		bss[0], err = h.readResponseData(bs)
		if err == errMissRequest {
			err = nil
		} else if err != nil {
			return
		}
	} else {
		bss[0] = bs
	}
	mcr.resp = bss
	return
}

func (h *handler) readResponseData(bs []byte) (data []byte, err error) {
	if bytes.Equal(bs, endBytes) {
		stat.Miss(h.cluster, h.addr)
		err = errMissRequest
		return
	}

	stat.Hit(h.cluster, h.addr)
	c := bytes.Count(bs, spaceBytes)
	if c < 3 {
		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes split")
		return
	}

	i := bytes.IndexByte(bs, spaceByte) + 1 // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
	i = i + bytes.IndexByte(bs[i:], spaceByte) + 1
	i = i + bytes.IndexByte(bs[i:], spaceByte) + 1
	var high int

	if len(bs[i:]) < 2 { // check if bytes length is null
		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes check")
		return
	}

	if c == 3 {
		// GET/GAT
		high = len(bs) - 2
	} else {
		// GETS/GATS
		high = i + bytes.IndexByte(bs[i:], spaceByte)
	}

	var size int64
	if size, err = conv.Btoi(bs[i:high]); err != nil {
		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes length")
		return
	}
	if data, err = h.br.ReReadFull(int(size), len(bs)); err != nil {
		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes data")
		return
	}
	if data, err = h.br.ReReadUntilBytes(endBytes, len(data)); err != nil {
		err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes end bytes")
	}
	return
}

func (h *handler) Close() error {
	if atomic.CompareAndSwapInt32(&h.closed, handlerOpening, handlerClosed) {
		return h.conn.Close()
	}
	return nil
}

func (h *handler) Closed() bool {
	return atomic.LoadInt32(&h.closed) == handlerClosed
}
