package proto

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockNodeConn struct {
	closed bool
}

func (n *mockNodeConn) Write(*Message) error {
	return nil
}
func (n *mockNodeConn) Read(*Message) error {
	return nil
}
func (n *mockNodeConn) Flush() error {
	return nil
}
func (n *mockNodeConn) Close() error {
	n.closed = true
	return nil
}

func TestPipe(t *testing.T) {
	nc := &mockNodeConn{}
	ncp := NewNodeConnPipe(2, func() NodeConn {
		return nc
	})
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		m := getMsg()
		m.WithWaitGroup(wg)
		ncp.Push(m)
	}
	wg.Wait()
	ncp.Close()
	assert.True(t, nc.closed)
}
