package proto

import (
	"errors"
	"sync"
	"sync/atomic"

	"overlord/pkg/hashkit"
)

const (
	opened = int32(0)
	closed = int32(1)

	pipeMaxCount = 128
)

var (
	errInputChanFull = errors.New("node pipe input chan is full")
)

// NodeConnPipe multi MsgPipe for node conns.
type NodeConnPipe struct {
	conns int32
	mps   []*msgPipe
	chans []*pipeChan
	l     sync.RWMutex

	errCh chan error

	state int32
}

// NewNodeConnPipe new NodeConnPipe.
func NewNodeConnPipe(conns int32, newNc func() NodeConn) (ncp *NodeConnPipe) {
	if conns <= 0 {
		panic("the number of connections cannot be zero")
	}
	ncp = &NodeConnPipe{
		conns: conns,
		mps:   make([]*msgPipe, conns),
		chans: make([]*pipeChan, conns),
		errCh: make(chan error, 1),
	}
	for i := int32(0); i < ncp.conns; i++ {
		ncp.chans[i] = newPipeChan()
		ncp.mps[i] = newMsgPipe(ncp.chans[i], newNc, ncp.errCh)
	}
	return
}

// Push push message into input chan.
func (ncp *NodeConnPipe) Push(m *Message) {
	m.Add()
	var ok bool
	ncp.l.RLock()
	if ncp.state == opened {
		if ncp.conns == 1 {
			ok = ncp.chans[0].push(m)
		} else {
			req := m.Request()
			if req != nil {
				crc := int32(hashkit.Crc16(req.Key()))
				ok = ncp.chans[crc%ncp.conns].push(m)
			} else {
				// NOTE: impossible!!!
			}
		}
	}
	ncp.l.RUnlock()
	if !ok {
		m.WithError(errInputChanFull)
		m.Done()
	}
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
	for _, ch := range ncp.chans {
		ch.close()
	}
	ncp.l.Unlock()
}

// msgPipe message pipeline.
type msgPipe struct {
	nc    atomic.Value
	newNc func() NodeConn
	input *pipeChan

	errCh chan<- error
}

func newMsgPipe(input *pipeChan, newNc func() NodeConn, errCh chan<- error) (mp *msgPipe) {
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
		nc = mp.nc.Load().(NodeConn)
	)
	for {
		ms, ok := mp.input.popAll()
		if !ok {
			nc.Close()
			return
		}
		if len(ms) == 0 {
			continue
		}
		var werr error
		for _, m := range ms {
			if werr == nil {
				werr = nc.Write(m)
			} // NOTE: no else!!!
			if werr != nil {
				m.WithError(werr)
				m.Done()
			}
		}
		if werr != nil {
			nc = mp.reNewNc(nc, werr)
			continue
		}
		if ferr := nc.Flush(); ferr != nil {
			for i := 0; i < len(ms); i++ {
				ms[i].WithError(ferr)
				ms[i].Done()
			}
			nc = mp.reNewNc(nc, ferr)
			continue
		}
		var rerr error
		for i := 0; i < len(ms); i++ {
			if rerr == nil {
				rerr = nc.Read(ms[i])
			} // NOTE: no else!!!
			if rerr != nil {
				ms[i].WithError(rerr)
			}
			ms[i].Done()
		}
		if rerr != nil {
			nc = mp.reNewNc(nc, rerr)
			continue
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

type pipeChan struct {
	lock sync.Mutex
	cond *sync.Cond

	data  []*Message
	buff  []*Message
	count int

	waits  int
	closed bool
}

func newPipeChan() *pipeChan {
	pc := &pipeChan{
		data: make([]*Message, 0, pipeMaxCount),
		buff: make([]*Message, 0, pipeMaxCount),
	}
	pc.cond = sync.NewCond(&pc.lock)
	return pc
}

// push push message into slice.
// NOTE: multi wirte!!!
func (pc *pipeChan) push(m *Message) (ok bool) {
	pc.lock.Lock()
	if pc.closed {
		pc.lock.Unlock()
		return
	}
	if pc.waits != 0 {
		pc.cond.Signal()
	}
	// NOTE: discard if the buff is too large!!!
	if len(pc.buff) <= pipeMaxCount*pipeMaxCount {
		pc.buff = append(pc.buff, m)
		pc.count++
		ok = true
	}
	pc.lock.Unlock()
	return
}

// popAll pop all message from slice.
// NOTE: only one read!!!
func (pc *pipeChan) popAll() (ms []*Message, ok bool) {
	pc.lock.Lock()
	if !pc.closed && pc.count == 0 {
		pc.waits++
		pc.cond.Wait()
		pc.waits--
	}
	if pc.closed {
		pc.lock.Unlock()
		return
	}
	if pc.count <= pipeMaxCount {
		pc.data = pc.data[0:pc.count]
		copy(pc.data, pc.buff[0:pc.count])
	} else {
		pc.data = pc.data[:0]
		for i := 0; i < pc.count; i++ {
			pc.data = append(pc.data, pc.buff[i])
		}
	}
	ms = pc.data
	pc.buff = pc.buff[:0]
	pc.count = 0
	pc.lock.Unlock()
	ok = true
	return
}

func (pc *pipeChan) close() {
	pc.lock.Lock()
	pc.closed = true
	pc.cond.Broadcast()
	pc.lock.Unlock()
}
