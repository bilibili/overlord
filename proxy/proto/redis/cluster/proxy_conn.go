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
	ErrInvalidArgument   = errs.New("cluster command with wrong argument")
	ErrClusteTypeChanged = errs.New("Cannot handle request when changing backend cluster mode")
)

type proxyConn struct {
	pc proto.ProxyConn
}

// NewProxyConn creates new redis cluster Encoder and Decoder.
func NewProxyConn(conn *libnet.Conn) proto.ProxyConn {
	r := &proxyConn{
		pc: redis.NewProxyConn(conn),
	}
	return r
}

func (pc *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	return pc.pc.Decode(msgs)
}

func (pc *proxyConn) Encode(m *proto.Message, forwarder proto.Forwarder) (err error) {
	if !m.IsBatch() {
		req := m.Request().(*redis.Request)
		if !req.IsSupport() && !req.IsCtl() {
			resp := req.RESP()
			arr := resp.Array()
			if bytes.Equal(arr[0].Data(), cmdClusterBytes) {
				if len(arr) == 2 && forwarder != nil {
					var cls, ok = forwarder.(*cluster)
					if !ok {
						err = errors.WithStack(ErrClusteTypeChanged)
						return
					}
					// CLUSTER COMMANDS
					conv.UpdateToUpper(arr[1].Data()) // NOTE: when arr[0] is CLUSTER, upper arr[1]
					pcc := pc.pc.(*redis.ProxyConn)
					if bytes.Equal(arr[1].Data(), cmdNodesBytes) {
						// CLUSTER NODES
						err = pcc.Bw().Write(cls.fakeNodesBytes)
						return
					} else if bytes.Equal(arr[1].Data(), cmdSlotsBytes) {
						// CLUSTER SLOTS
						err = pcc.Bw().Write(cls.fakeSlotsBytes)
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
	return pc.pc.Encode(m, forwarder)
}

func (pc *proxyConn) Flush() (err error) {
	return pc.pc.Flush()
}
