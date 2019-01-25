package pc

import (
	"overlord/proxy/proto"
)

// MigrateProxyConn is the struct for migrate conn mocked for input channel
type MigrateProxyConn struct {
	C chan []*proto.Message
}

// NewProxyConn is used for redis migrate
func NewProxyConn() proto.ProxyConn {
	m := &MigrateProxyConn{
		C: make(chan []*proto.Message, 1024),
	}
	return m
}

// Decode impl proto.ProxyConn
func (mpc *MigrateProxyConn) Decode(messages []*proto.Message) ([]*proto.Message, error) {
	select {
	case msgs := <-mpc.C:
		lenOfM := len(msgs)
		for i := range msgs {
			messages[i] = msgs[i]
			msgs[i] = nil
		}
		return messages[:lenOfM], nil
	default:
		return messages[:0], nil
	}
}

// Decode impl proto.ProxyConn
func (mpc *MigrateProxyConn) Encode(msg *proto.Message) error {
	return nil
}

// FLush impl proto.ProxyConn
func (mpc *MigrateProxyConn) Flush() error {
	return nil
}
