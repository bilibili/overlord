package redis

import (
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/proto"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)

	handlerWriteBufferSize = 8 * 1024   // NOTE: write command, so relatively small
	handlerReadBufferSize  = 128 * 1024 // NOTE: read data, so relatively large
)

// errors
var (
	ErrReuseClosedConn = errors.New("resuse closed connection")
	ErrBadRequest      = errors.New("bad request")
)

type handler struct {
	cluster string
	addr    string

	conn net.Conn

	buf *buffer

	readTimeout   time.Duration
	writerTimeout time.Duration

	closed int32
}

func (h *handler) Handle(req *proto.Request) (resp *proto.Response, err error) {
	if h.Closed() {
		err = ErrReuseClosedConn
		return
	}

	_, ok := req.Proto().(*RRequest)
	if !ok {
		err = ErrBadRequest
		return
	}

	return nil, nil
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
