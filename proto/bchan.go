package proto

import (
	"sync/atomic"
)

// BatchChan is the channel pool using round-robin access to blance load.
type BatchChan struct {
	idx int32
	cnt int32
	chs []chan *MsgBatch
}

// NewBatchChan will create new BatchChan with given number of channel.
func NewBatchChan(n int32) *BatchChan {
	chs := make([]chan *MsgBatch, n)
	for i := int32(0); i < n; i++ {
		chs[i] = make(chan *MsgBatch, 1024)
	}
	return &BatchChan{cnt: n, chs: chs}
}

// Push will push the `MsgBatch` object into the special channel.
func (c *BatchChan) Push(m *MsgBatch) {
	i := atomic.AddInt32(&c.idx, 1)
	c.chs[i%c.cnt] <- m
}

// GetCh will return the MsgBatch with the given index.
func (c *BatchChan) GetCh(i int) chan *MsgBatch {
	return c.chs[i%int(c.cnt)]
}
