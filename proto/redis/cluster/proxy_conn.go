package cluster

import (
	"bytes"

	"overlord/lib/conv"
	libnet "overlord/lib/net"
	"overlord/proto"
	"overlord/proto/redis"

	"github.com/pkg/errors"
)

var (
	cmdClusterBytes = []byte("7\r\nCLUSTER")
	cmdNodesBytes   = []byte("5\r\nNODES")
	cmdSlotsBytes   = []byte("5\r\nSLOTS")
	notSupportBytes = []byte("-Error: command not support\r\n")
)

type proxyConn struct {
	pc proto.ProxyConn
	c *cluster
}

// NewProxyConn creates new redis cluster Encoder and Decoder.
func NewProxyConn(conn *libnet.Conn, executer proto.Executor) proto.ProxyConn {
	var c *cluster
	if executer != nil {
		c = executer.(*cluster)
	}

	r := &proxyConn{
		pc: redis.NewProxyConn(conn),
		c: c,
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
			if len(arr) == 2 && bytes.Equal(arr[0].Data(), cmdClusterBytes) {
				conv.UpdateToUpper(arr[1].Data()) // NOTE: when arr[0] is CLUSTER, upper arr[1]
				pcc := pc.pc.(*redis.ProxyConn)
				if bytes.Equal(arr[1].Data(), cmdNodesBytes) {
					err = pcc.Bw().Write(pc.c.fakeNodesBytes)
					return
				} else if bytes.Equal(arr[1].Data(), cmdSlotsBytes) {
					err = pcc.Bw().Write(pc.c.fakeSlotsBytes)
					return
				}
				err = pcc.Bw().Write(notSupportBytes)
				return
			}
		}
	}
	return pc.pc.Encode(m)
}

func (pc *proxyConn) Flush() (err error) {
	if err = pc.pc.Flush(); err != nil {
		err = errors.Wrap(err, "Redis Cluster ProxyConn flush response")
	}
	return
}
