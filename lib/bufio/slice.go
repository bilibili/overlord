package bufio

import "sync"

// SliceAlloc make big memory then slice.
type SliceAlloc struct {
	buf      []byte
	tinyPool *sync.Pool
	bigBytes *sync.Pool
}

// NewSliceAlloc will create new slice for reusing
func NewSliceAlloc() *SliceAlloc {
	// bytes Pool is now avaliable
	sa := &SliceAlloc{
		tinyPool: &sync.Pool{
			New: func() interface{} {
				b := buffer(make([]byte, 8192))
				return &b
			},
		},

		bigBytes: &sync.Pool{
			New: func() interface{} {
				b := buffer(make([]byte, 16*1024))
				return &b
			},
		},
	}

	b := sa.tinyPool.Get().(*buffer)
	sa.buf = []byte(*b)
	return sa
}

type buffer []byte

// Put is a proxy func of tinyPool
func (d *SliceAlloc) Put(data []byte) {
	b := buffer(data)
	if len(data) >= 512 {
		d.bigBytes.Put(&b)
	} else {
		d.tinyPool.Put(&b)
	}
}

// Make make bytes slice from buf.
func (d *SliceAlloc) Make(n int) (ss []byte) {
	switch {
	case n == 0:
		return []byte{}
	case n >= 512:
		b := d.bigBytes.Get().(*buffer)
		ss = []byte(*b)
		if len(ss) < n {
			// create new one bigger
			ss = make([]byte, n)
		}
		return ss
	default:
		if len(d.buf) < n {
			b := d.tinyPool.Get().(*buffer)
			d.buf = []byte(*b)
		}
		ss, d.buf = d.buf[:n:n], d.buf[n:]
		return ss
	}
}
