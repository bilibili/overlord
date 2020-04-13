package cluster

import (
	"bytes"
	errs "errors"
	"overlord/pkg/conv"
	libnet "overlord/pkg/net"
	"overlord/proxy/proto"
	"overlord/proxy/proto/redis"

	"github.com/pkg/errors"
)

var (
	cmdClusterBytes = []byte("7\r\nCLUSTER")
	cmdNodesBytes   = []byte("5\r\nNODES")
	cmdSlotsBytes   = []byte("5\r\nSLOTS")
	notSupportBytes = []byte("-Error: command not support\r\n")
)

// errors
var (
	ErrInvalidArgument = errs.New("cluster command with wrong argument")
)

type proxyConn struct {
	c  *cluster
	pc proto.ProxyConn
}

// NewProxyConn creates new redis cluster Encoder and Decoder.
func NewProxyConn(conn *libnet.Conn, fer proto.Forwarder) proto.ProxyConn {
	var c *cluster
	if fer != nil {
		c = fer.(*cluster)
	}
	r := &proxyConn{
		c:  c,
		pc: redis.NewProxyConn(conn, false),
	}
	return r
}

func (pc *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	return pc.pc.Decode(msgs)
}

func (pc *proxyConn) Encode(m *proto.Message) (err error) {
	if !m.IsBatch() {
		req := m.Request().(*redis.Request)
		if !req.IsSupport() && !req.IsCtl() {
			resp := req.RESP()
			arr := resp.Array()
			if bytes.Equal(arr[0].Data(), cmdClusterBytes) {
				if len(arr) == 2 {
					// CLUSTER COMMANDS
					conv.UpdateToUpper(arr[1].Data()) // NOTE: when arr[0] is CLUSTER, upper arr[1]
					pcc := pc.pc.(*redis.ProxyConn)
					if bytes.Equal(arr[1].Data(), cmdNodesBytes) {
						// CLUSTER NODES
						err = pcc.Bw().Write(pc.c.fakeNodesBytes)
						return
					} else if bytes.Equal(arr[1].Data(), cmdSlotsBytes) {
						// CLUSTER SLOTS
						err = pcc.Bw().Write(pc.c.fakeSlotsBytes)
						return
					}
					err = pcc.Bw().Write(notSupportBytes)
					return
				}
				err = errors.WithStack(ErrInvalidArgument)
				return
			}
		}
	}
	return pc.pc.Encode(m)
}

func (pc *proxyConn) Flush() (err error) {
	return pc.pc.Flush()
}
