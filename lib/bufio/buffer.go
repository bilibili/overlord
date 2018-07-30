package bufio

import (
	"sort"
	"sync"
)

const (
	// 512MB: max redis RDB object is 512MB due :https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
	// so the max RESP object length is 512MB
	// max memcache buffer length is not longer than 512MB
	maxBufferSize     = 512 * 1024 * 1024
	defaultBufferSize = 512
	growFactor        = 2
)

var (
	sizes []int
	pools []*sync.Pool
)

func init() {
	sizes = make([]int, 0)
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
			return NewBuffer(sizes[idx])
		},
	}
}

// Buffer buffer.
type Buffer struct {
	buf  []byte
	r, w int
}

// NewBuffer new buffer.
func NewBuffer(size int) *Buffer {
	return &Buffer{buf: make([]byte, size)}
}

// Bytes return the bytes readed
func (b *Buffer) Bytes() []byte {
	return b.buf[b.r:b.w]
}

func (b *Buffer) grow() {
	nb := make([]byte, len(b.buf)*growFactor)
	copy(nb, b.buf[:b.w])
	b.buf = nb
	// NOTE: old buf cannot put into pool, maybe some slice point mem. Wait GC!!!
}

func (b *Buffer) len() int {
	return len(b.buf)
}

// Advance the rpos
func (b *Buffer) Advance(n int) {
	b.r += n
}

func (b *Buffer) shrink() {
	if b.r == 0 {
		return
	}
	copy(b.buf, b.buf[b.r:b.w])
	b.w -= b.r
	b.r = 0
}

func (b *Buffer) buffered() int {
	return b.w - b.r
}

// Reset reset buffer.
func (b *Buffer) Reset() {
	// b.buf = b.buf[:0]
	// b.buf = b.buf[:cap(b.buf)]
	b.r, b.w = 0, 0
}

// Get the data buffer
func Get(size int) *Buffer {
	if size <= defaultBufferSize {
		size = defaultBufferSize
	}
	i := sort.SearchInts(sizes, size)
	if i >= len(pools) {
		return NewBuffer(size)
	}
	b := pools[i].Get().(*Buffer)
	b.Reset()
	return b
}

// Put the data into global pool
func Put(b *Buffer) {
	i := sort.SearchInts(sizes, b.len())
	if i < len(pools) {
		pools[i].Put(b)
	}
}
