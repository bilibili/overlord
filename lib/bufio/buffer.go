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
	// remove check
	// if b.r < 0 {
	// 	panic("fail to advance")
	// }
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
		b := &Buffer{buf: make([]byte, size)}
		return b
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
