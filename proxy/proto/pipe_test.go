package proto

import (
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockNodeConn struct {
	closed     bool
	count, num int
	err        error
}

func (n *mockNodeConn) Addr() string {
	return "mock"
}

func (n *mockNodeConn) Cluster() string {
	return "mock"
}

func (n *mockNodeConn) Write(*Message) error { return nil }
func (n *mockNodeConn) Read(*Message) error {
	if n.count == n.num {
		return n.err
	}
	n.count++
	return nil
}
func (n *mockNodeConn) Flush() error { return nil }
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
func (*mockRequest) Put()                   {}
func (*mockRequest) Slowlog() *SlowlogEntry { return nil }

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

	const whenErrNum = 3
	nc3 := &mockNodeConn{}
	nc3.num = whenErrNum
	nc3.err = errors.New("some error")
	ncp3 := NewNodeConnPipe(1, func() NodeConn {
		return nc3
	})
	wg = &sync.WaitGroup{}
	var msgs []*Message
	for i := 0; i < 10; i++ {
		m := getMsg()
		m.WithRequest(&mockRequest{})
		m.WithWaitGroup(wg)
		ncp3.Push(m)
		msgs = append(msgs, m)
	}
	wg.Wait()
	ncp3.Close()
	time.Sleep(100 * time.Millisecond)
	for _, msg := range msgs {
		assert.EqualError(t, msg.Err(), "some error")
	}
}
