package proto

import (
	"sync/atomic"
)

const (
	opened = int32(0)
	closed = int32(1)

	pipeMaxCount = 128
)

// NodeConnPipe multi MsgPipe for node conns.
type NodeConnPipe struct {
	input chan *Message
	mps   []*msgPipe
	errCh chan error

	state int32
}

// NewNodeConnPipe new NodeConnPipe.
func NewNodeConnPipe(conns int32, newNc func() NodeConn) (ncp *NodeConnPipe) {
	ncp = &NodeConnPipe{
		input: make(chan *Message, pipeMaxCount*128),
		mps:   make([]*msgPipe, conns),
		errCh: make(chan error, 1),
	}
	for i := 0; i < len(ncp.mps); i++ {
		ncp.mps[i] = newMsgPipe(ncp.input, newNc, ncp.errCh)
	}
	return
}

// Push push message into input chan.
func (ncp *NodeConnPipe) Push(m *Message) {
	if atomic.LoadInt32(&ncp.state) == opened {
		m.Add()
		ncp.input <- m
	}
}

// ErrorEvent return error chan.
func (ncp *NodeConnPipe) ErrorEvent() <-chan error {
	return ncp.errCh
}

// Close close pipe.
func (ncp *NodeConnPipe) Close() {
	if atomic.CompareAndSwapInt32(&ncp.state, opened, closed) {
		close(ncp.errCh)
		close(ncp.input)
		for _, mp := range ncp.mps {
			mp.close()
		}
	}
}

// msgPipe message pipeline.
type msgPipe struct {
	nc    atomic.Value
	newNc func() NodeConn
	input <-chan *Message

	batch   [pipeMaxCount]*Message
	count   int
	forward chan *Message

	errCh chan<- error

	closed chan struct{}
}

// newMsgPipe new msgPipe and return.
func newMsgPipe(input <-chan *Message, newNc func() NodeConn, errCh chan<- error) (mp *msgPipe) {
	mp = &msgPipe{
		newNc:   newNc,
		input:   input,
		forward: make(chan *Message, pipeMaxCount*2),
		errCh:   errCh,
		closed:  make(chan struct{}, 1),
	}
	mp.nc.Store(newNc())
	go mp.pipeWrite()
	go mp.pipeRead()
	return
}

func (mp *msgPipe) pipeWrite() {
	var (
		m  *Message
		ok bool
		nc = mp.nc.Load().(NodeConn)
	)
	for {
		for {
			if m == nil {
				select {
				case m, ok = <-mp.input:
					if !ok {
						mp.closed <- struct{}{}
						return
					}
				default:
				}
				if m == nil {
					break
				}
			}
			mp.batch[mp.count] = m
			mp.count++
			if err := nc.Write(m); err != nil {
				m.WithError(err)
			}
			m = nil
			if mp.count >= pipeMaxCount {
				break
			}
		}
		if mp.count > 0 {
			if err := nc.Flush(); err != nil {
				for i := 0; i < mp.count; i++ {
					mp.batch[i].WithError(err)
					mp.batch[i].Done()
				}
				mp.count = 0
				nc = mp.reNewNc()
				select {
				case mp.errCh <- err: // NOTE: action
				default:
				}
				continue
			}
			for i := 0; i < mp.count; i++ {
				mp.forward <- mp.batch[i]
			}
			mp.count = 0
			continue
		}
		m = <-mp.input // NOTE: avoid infinite loop
	}
}

func (mp *msgPipe) pipeRead() {
	var nc = mp.nc.Load().(NodeConn)
	for {
		m, ok := <-mp.forward
		if !ok {
			mp.closed <- struct{}{}
			return
		}
		if m == nil {
			nc.Close()
			nc = mp.nc.Load().(NodeConn)
			continue
		}
		if m.Err() != nil {
			continue
		}
		if err := nc.Read(m); err != nil {
			m.WithError(err)
		}
		m.Done()
	}
}

func (mp *msgPipe) reNewNc() NodeConn {
	mp.nc.Store(mp.newNc())
	mp.forward <- nil
	return mp.nc.Load().(NodeConn)
}

func (mp *msgPipe) close() {
	<-mp.closed
	close(mp.forward)
	<-mp.closed
	nc := mp.nc.Load().(NodeConn)
	nc.Close()
}
