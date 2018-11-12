package proto

import (
	"sync"
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
	l     sync.RWMutex

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
	ncp.l.RLock()
	if ncp.state == opened {
		m.Add()
		ncp.input <- m
	}
	ncp.l.RUnlock()
}

// ErrorEvent return error chan.
func (ncp *NodeConnPipe) ErrorEvent() <-chan error {
	return ncp.errCh
}

// Close close pipe.
func (ncp *NodeConnPipe) Close() {
	close(ncp.errCh)
	ncp.l.Lock()
	ncp.state = closed
	close(ncp.input)
	ncp.l.Unlock()
}

// msgPipe message pipeline.
type msgPipe struct {
	nc    atomic.Value
	newNc func() NodeConn
	input <-chan *Message

	batch [pipeMaxCount]*Message
	count int

	errCh chan<- error
}

// newMsgPipe new msgPipe and return.
func newMsgPipe(input <-chan *Message, newNc func() NodeConn, errCh chan<- error) (mp *msgPipe) {
	mp = &msgPipe{
		newNc: newNc,
		input: input,
		errCh: errCh,
	}
	mp.nc.Store(newNc())
	go mp.pipe()
	return
}

func (mp *msgPipe) pipe() {
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
						nc.Close()
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
			if werr := nc.Write(m); werr != nil {
				m.WithError(werr)
			}
			m = nil
			if mp.count >= pipeMaxCount {
				break
			}
		}
		if mp.count > 0 {
			if ferr := nc.Flush(); ferr != nil {
				for i := 0; i < mp.count; i++ {
					mp.batch[i].WithError(ferr)
					mp.batch[i].Done()
				}
				mp.count = 0
				nc = mp.reNewNc(nc, ferr)
				continue
			}
			var rerr error
			for i := 0; i < mp.count; i++ {
				if rerr = nc.Read(mp.batch[i]); rerr != nil {
					mp.batch[i].WithError(rerr)
				}
				mp.batch[i].Done()
			}
			mp.count = 0
			if rerr != nil {
				nc = mp.reNewNc(nc, rerr)
			}
			continue
		}
		m, ok = <-mp.input // NOTE: avoid infinite loop
		if !ok {
			nc.Close()
			return
		}
	}
}

func (mp *msgPipe) reNewNc(nc NodeConn, err error) NodeConn {
	if err != nil {
		select {
		case mp.errCh <- err: // NOTE: action
		default:
		}
	}
	nc.Close()
	mp.nc.Store(mp.newNc())
	return mp.nc.Load().(NodeConn)
}
