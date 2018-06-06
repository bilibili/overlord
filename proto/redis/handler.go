package redis

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/pool"
	"github.com/felixhao/overlord/proto"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)
)

// errors
var (
	ErrReuseClosedConn = errors.New("resuse closed connection")
	ErrBadRequest      = errors.New("bad request")
)

type handler struct {
	cluster string
	addr    string

	conn *connection

	buf *buffer

	closed int32
}

// Dial will create a dial factory function to create new connection of redis
func Dial(cluster, addr string, dialTimeout, readerTimeout, writerTimeout time.Duration) (dial func() (pool.Conn, error)) {
	dial = func() (pool.Conn, error) {
		conn, err := dialWithTimeout(addr, dialTimeout, readerTimeout, writerTimeout)
		if err != nil {
			return nil, err
		}
		h := &handler{
			cluster: cluster,
			addr:    addr,
			conn:    conn,
			buf:     newBuffer(conn, conn),
			closed:  handlerOpening,
		}
		return h, nil
	}
	return
}

func (h *handler) Handle(req *proto.Request) (*proto.Response, error) {
	if h.Closed() {
		return nil, ErrReuseClosedConn
	}

	rr, ok := req.Proto().(*RRequest)
	if !ok {
		return nil, ErrBadRequest
	}

	err := h.buf.encodeResp(rr.respObj)
	if err != nil {
		return nil, err
	}

	robj, err := h.buf.decodeRespObj()
	if err != nil {
		return nil, err
	}
	response := &proto.Response{Type: proto.CacheTypeRedis}
	protoResponse := newRResponse(getCmdType(req.Cmd()), robj)
	response.WithProto(protoResponse)
	return response, nil
}

func (h *handler) Close() error {
	if atomic.CompareAndSwapInt32(&h.closed, handlerOpening, handlerClosed) {
		// ignore error
		_ = h.buf.Flush()
		return h.conn.Close()
	}
	return nil
}

func (h *handler) Closed() bool {
	return atomic.LoadInt32(&h.closed) == handlerClosed
}
