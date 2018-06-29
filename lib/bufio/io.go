package bufio

import (
	"bufio"
	"bytes"
	"io"
	"net"

	libnet "github.com/felixhao/overlord/lib/net"
)

const (
	maxBuffered = 64
)

// ErrProxy
var (
	ErrBufferFull = bufio.ErrBufferFull
)

// Reader implements buffering for an io.Reader object.
type Reader struct {
	rd  io.Reader
	b   *Buffer
	err error
}

// NewReader returns a new Reader whose buffer has the default size.
func NewReader(rd io.Reader, b *Buffer) *Reader {
	return &Reader{rd: rd, b: b}
}

func (r *Reader) fill() error {
	n, err := r.rd.Read(r.b.buf[r.b.w:])
	r.b.w += n
	if err != nil {
		r.err = err
		return err
	} else if n == 0 {
		return io.ErrNoProgress
	}
	return nil
}

// Advance proxy to buffer advance
func (r *Reader) Advance(n int) {
	r.b.Advance(n)
}

// Buffer will return the reference of local buffer
func (r *Reader) Buffer() *Buffer {
	return r.b
}

// Read will trying to read until the buffer is full
func (r *Reader) Read() error {
	if r.err != nil {
		return r.err
	}

	if r.b.buffered() == r.b.len() {
		r.b.grow()
	}

	if r.b.w == r.b.len() {
		r.b.shrink()
	}

	if err := r.fill(); err != io.EOF {
		return err
	}
	return nil
}

// ReadSlice will read until the delim or return ErrBufferFull.
// It never contains any I/O operation
func (r *Reader) ReadSlice(delim byte) (data []byte, err error) {
	idx := bytes.IndexByte(r.b.buf[r.b.r:r.b.w], delim)
	if idx == -1 {
		data = nil
		err = ErrBufferFull
		return
	}
	data = r.b.buf[r.b.r : r.b.r+idx+1]
	r.b.r += idx + 1
	return
}

// ReadExact will read n size bytes or return ErrBufferFull.
// It never contains any I/O operation
func (r *Reader) ReadExact(n int) (data []byte, err error) {
	if r.b.buffered() < n {
		err = ErrBufferFull
		return
	}
	data = r.b.buf[r.b.r : r.b.r+n]
	r.b.r += n
	return
}

// ResetBuffer reset buf.
func (r *Reader) ResetBuffer(b *Buffer) {
	if b == nil {
		r.b = b
		return
	}
	b.Reset()
	var n int
	if r.b != nil {
		if r.b.buffered() > 0 {
			for b.len() < r.b.buffered() {
				b.grow()
			}
			n = copy(b.buf[b.w:], r.b.buf[r.b.r:r.b.w])
			b.w += n
		}
	}
	r.err = nil
	r.b = b
	r.b.r = b.r
	r.b.w = b.w
}

// ReadUntil reads until the first occurrence of delim in the input,
// returning a slice pointing at the bytes in the buffer.
// The bytes stop being valid at the next read.
// If ReadUntil encounters an error before finding a delimiter,
// it returns all the data in the buffer and the error itself (often io.EOF).
// ReadUntil returns err != nil if and only if line does not end in delim.
func (r *Reader) ReadUntil(delim byte) ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	for {
		var index = bytes.IndexByte(r.b.buf[r.b.r:r.b.w], delim)
		if index >= 0 {
			limit := r.b.r + index + 1
			slice := r.b.buf[r.b.r:limit]
			r.b.r = limit
			return slice, nil
		}
		if r.b.w >= r.b.len() {
			r.b.grow()
		}
		err := r.fill()
		if err == io.EOF && r.b.buffered() > 0 {
			data := r.b.buf[r.b.r:r.b.w]
			r.b.r = r.b.w
			return data, nil
		} else if err != nil {
			r.err = err
			return nil, err
		}
	}
}

// ReadFull reads exactly n bytes from r into buf.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading some but not all the bytes,
// ReadFull returns ErrUnexpectedEOF.
// On return, n == len(buf) if and only if err == nil.
func (r *Reader) ReadFull(n int) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	if r.err != nil {
		return nil, r.err
	}
	for {
		if r.b.buffered() >= n {
			bs := r.b.buf[r.b.r : r.b.r+n]
			r.b.r += n
			return bs, nil
		}
		maxCanRead := r.b.len() - r.b.w + r.b.buffered()
		if maxCanRead < n {
			r.b.grow()
		}
		err := r.fill()
		if err == io.EOF && r.b.buffered() > 0 {
			data := r.b.buf[r.b.r:r.b.w]
			r.b.r = r.b.w
			return data, nil
		} else if err != nil {
			r.err = err
			return nil, err
		}
	}
}

// Writer implements buffering for an io.Writer object.
// If an error occurs writing to a Writer, no more data will be
// accepted and all subsequent writes, and Flush, will return the error.
// After all data has been written, the client should call the
// Flush method to guarantee all data has been forwarded to
// the underlying io.Writer.
type Writer struct {
	wr     *libnet.Conn
	bufsp  net.Buffers
	bufs   [][]byte
	cursor int

	err error
}

// NewWriter returns a new Writer whose buffer has the default size.
func NewWriter(wr *libnet.Conn) *Writer {
	return &Writer{wr: wr, bufs: make([][]byte, maxBuffered)}
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.bufs) == 0 {
		return nil
	}
	// fmt.Printf("buffers:%v\n", w.bufs)
	//	nbufs := net.Buffers(w.bufs[:w.cursor])
	//	_, err := w.wr.Writev(&w.bufs)
	w.bufsp = net.Buffers(w.bufs[:w.cursor])
	_, err := w.wr.Writev(&w.bufsp)
	if err != nil {
		w.err = err
	}
	w.cursor = 0
	return w.err
}

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (w *Writer) Write(p []byte) (err error) {
	if w.err != nil {
		return w.err
	}
	if p == nil {
		return nil
	}

	if len(w.bufs) == maxBuffered {
		w.Flush()
	}
	//fmt.Printf("len %p %v %v %v", w.bufs, len(w.bufs), cap(w.bufs), w.cursor)
	w.bufs[w.cursor] = p
	w.cursor = (w.cursor + 1) % maxBuffered
	return nil
}

// WriteString writes a string.
// It returns the number of bytes written.
// If the count is less than len(s), it also returns an error explaining
// why the write is short.
func (w *Writer) WriteString(s string) (err error) {
	return w.Write([]byte(s))
}
