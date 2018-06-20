package bufio

import (
	"sort"
	"sync"
)

const (
	maxBufferSize     = 32 * 1024 * 1024 // 32MB
	defaultBufferSize = 1024
	growFactor        = 2
)

var (
	sizes []int
	pools []*sync.Pool
)

func init() {
	sizes = make([]int, maxBufferSize/defaultBufferSize/growFactor)
	threshold := defaultBufferSize
	for threshold <= maxBufferSize {
		sizes = append(sizes, threshold)
		threshold *= growFactor
	}
	// init poll
	pools = make([]*sync.Pool, len(sizes))
	for idx := range pools {
		initBufPool(idx)
	}
}

func initBufPool(idx int) {
	pools[idx] = &sync.Pool{
		New: func() interface{} {
			return &Buffer{
				buf: make([]byte, sizes[idx]),
			}
		},
	}
}

// Buffer buffer.
type Buffer struct {
	buf  []byte
	r, w int
}

func (b *Buffer) grow() {
	nb := make([]byte, len(b.buf)*growFactor)
	copy(nb, b.buf[:b.w])
	b.buf = nb
}

func (b *Buffer) len() int {
	return len(b.buf)
}

// Advance the rpos
func (b *Buffer) Advance(n int) {
	b.r += n
	// TODO: remove check
	if b.r < 0 {
		panic("fail to advance")
	}
}

// func (b *Buffer) Buf() []byte {
// 	return b.buf
// }

func (b *Buffer) buffered() int {
	return b.w - b.r
}

// Reset reset buffer.
func (b *Buffer) Reset() {
	b.r, b.w = 0, 0
}

// Bytes return unread bytes.
func (b *Buffer) Bytes() []byte {
	return b.buf[b.r:b.w]
}

// Get the data buffer
func Get(size int) *Buffer {
	if size <= defaultBufferSize {
		size = defaultBufferSize
	}
	i := sort.SearchInts(sizes, size)
	b := pools[i].Get().(*Buffer)
	b.Reset()
	return b
}

// Put the data into global pool
func Put(b *Buffer) {
	i := sort.SearchInts(sizes, b.len())
	pools[i].Put(b)
}
