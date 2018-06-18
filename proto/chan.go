package proto

import "sync"

const defaultMsgChanBuffer = 128

// MsgChan is queue be used process Msg.
type MsgChan struct {
	lock sync.Mutex
	cond *sync.Cond

	data []*Message
	buff []*Message

	waits  int
	closed bool
}

// NewMsgChan new Msg chan, defalut buffer 128.
func NewMsgChan() *MsgChan {
	return NewMsgChanBuffer(0)
}

// NewMsgChanBuffer new Msg chan with buffer.
func NewMsgChanBuffer(n int) *MsgChan {
	if n <= 0 {
		n = defaultMsgChanBuffer
	}
	ch := &MsgChan{
		buff: make([]*Message, n),
	}
	ch.cond = sync.NewCond(&ch.lock)
	return ch
}

// PushBack push Msg back queue.
func (c *MsgChan) PushBack(r *Message) int {
	c.lock.Lock()
	if c.closed {
		c.lock.Unlock()
		return 0
	}
	c.data = append(c.data, r)
	n := len(c.data)
	if c.waits != 0 {
		c.cond.Signal()
	}
	c.lock.Unlock()
	return n
}

// PopFront pop front from queue.
func (c *MsgChan) PopFront() (*Message, bool) {
	c.lock.Lock()
	for len(c.data) == 0 {
		if c.closed {
			c.lock.Unlock()
			return nil, false
		}
		c.data = c.buff[:0]
		c.waits++
		c.cond.Wait()
		c.waits--
	}
	r := c.data[0]
	c.data[0], c.data = nil, c.data[1:]
	c.lock.Unlock()
	return r, true
}

// Buffered returns buffer.
func (c *MsgChan) Buffered() int {
	c.lock.Lock()
	n := len(c.data)
	c.lock.Unlock()
	return n
}

// Close close Msg chan.
func (c *MsgChan) Close() {
	c.lock.Lock()
	if !c.closed {
		c.closed = true
		c.cond.Broadcast()
	}
	c.lock.Unlock()
}

// Closed return closed.
func (c *MsgChan) Closed() bool {
	c.lock.Lock()
	b := c.closed
	c.lock.Unlock()
	return b
}
