package proto

import (
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockNodeConn struct {
	closed bool
}

func (n *mockNodeConn) Write(*Message) error { return nil }
func (n *mockNodeConn) Read(*Message) error  { return nil }
func (n *mockNodeConn) Flush() error         { return nil }
func (n *mockNodeConn) Close() error {
	n.closed = true
	return nil
}

type mockRequest struct{}

func (*mockRequest) CmdString() string { return "" }
func (*mockRequest) Cmd() []byte       { return nil }
func (*mockRequest) Key() []byte {
	bs := make([]byte, 8)
	rand.Read(bs)
	return bs
}
func (*mockRequest) Put() {}

func TestPipe(t *testing.T) {
	nc1 := &mockNodeConn{}
	ncp1 := NewNodeConnPipe(1, func() NodeConn {
		return nc1
	})
	nc2 := &mockNodeConn{}
	ncp2 := NewNodeConnPipe(2, func() NodeConn {
		return nc2
	})
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		m := getMsg()
		m.WithRequest(&mockRequest{})
		m.WithWaitGroup(wg)
		ncp1.Push(m)
		ncp2.Push(m)
	}
	wg.Wait()
	ncp1.Close()
	ncp2.Close()
	time.Sleep(10 * time.Millisecond)
	assert.True(t, nc1.closed)
	assert.True(t, nc2.closed)
}
