package proto

import (
	"errors"
	"overlord/pkg/prom"
	"sync"
	"sync/atomic"
	"time"

	perr "github.com/pkg/errors"

	"overlord/pkg/hashkit"
)

const (
	opened = int32(0)
	closed = int32(1)

	pipeMaxCount = 512
)

var (
	errPipeChanFull = errors.New("pipe chan is full")
)

// NodeConnPipe multi MsgPipe for node conns.
type NodeConnPipe struct {
	conns  int32
	inputs []chan *Message
	mps    []*msgPipe
	l      sync.RWMutex

	errCh chan error

	state int32
}

// NewNodeConnPipe new NodeConnPipe.
func NewNodeConnPipe(conns int32, newNc func() NodeConn) (ncp *NodeConnPipe) {
	if conns <= 0 {
		panic("the number of connections cannot be zero")
	}
	ncp = &NodeConnPipe{
		conns:  conns,
		inputs: make([]chan *Message, conns),
		mps:    make([]*msgPipe, conns),
		errCh:  make(chan error, 1),
	}
	for i := int32(0); i < ncp.conns; i++ {
		ncp.inputs[i] = make(chan *Message, pipeMaxCount*pipeMaxCount)
		ncp.mps[i] = newMsgPipe(ncp.inputs[i], newNc, ncp.errCh)
	}
	return
}

// Push push message into input chan.
func (ncp *NodeConnPipe) Push(m *Message) {
	m.Add()
	var input chan *Message
	ncp.l.RLock()
	if ncp.state == opened {
		if ncp.conns == 1 {
			input = ncp.inputs[0]
		} else {
			req := m.Request()
			if req != nil {
				crc := int32(hashkit.Crc16(req.Key()))
				input = ncp.inputs[crc%ncp.conns]
			} else {
				// NOTE: impossible!!!
			}
		}
	}
	ncp.l.RUnlock()
	if input != nil {
		input <- m
		return
	}
	m.WithError(errPipeChanFull)
	m.Done()
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
	for _, input := range ncp.inputs {
		close(input)
	}
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
		nc  = mp.nc.Load().(NodeConn)
		m   *Message
		ok  bool
		err error
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
			m.MarkWrite()
			err = nc.Write(m)
			m = nil
			if err != nil {
				goto MEND
			}
			if mp.count >= pipeMaxCount {
				break
			}
		}
		if err == nil && mp.count > 0 {
			if err = nc.Flush(); err != nil {
				goto MEND
			}
			for i := 0; i < mp.count; i++ {
				if err == nil {
					err = nc.Read(mp.batch[i])
					mp.batch[i].MarkRead()
				} else {
					goto MEND
				}
			}
		}
	MEND:
		for i := 0; i < mp.count; i++ {
			msg := mp.batch[i]
			msg.WithError(err) // NOTE: maybe err is nil
			if prom.On {
				cmd := msg.Request().CmdString()
				duration := msg.RemoteDur()
				msg.Done()
				if err != nil {
					prom.ErrIncr(nc.Cluster(), nc.Addr(), cmd, perr.Cause(err).Error())
				} else {
					prom.HandleTime(nc.Cluster(), nc.Addr(), cmd, int64(duration/time.Microsecond))
				}
			} else {
				msg.Done()
			}
		}
		mp.count = 0
		if err != nil {
			nc = mp.reNewNc(nc, err)
			err = nil
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
