package bufio

import (
	"bufio"
	"bytes"
	"io"
	"net"

	libnet "overlord/pkg/net"
)

var (
	// ErrBufferFull err buffer full
	ErrBufferFull = bufio.ErrBufferFull
)

var (
	crlfBytes = []byte("\r\n")
)

// Reader implements buffering for an io.Reader object.
type Reader struct {
	rd  io.Reader
	b   *Buffer
	err error

	// for usage counter
	rbytes int
}

// NewReader returns a new Reader whose buffer has the default size.
func NewReader(rd io.Reader, b *Buffer) *Reader {
	return &Reader{rd: rd, b: b}
}

func (r *Reader) fill() error {
	n, err := r.rd.Read(r.b.buf[r.b.w:])
	r.rbytes += n
	r.b.w += n
	if err != nil {
		r.err = err
		return err
	} else if n == 0 {
		return io.ErrNoProgress
	}
	return nil
}

// GetReadedSize is the struct which for usage count
func (r *Reader) GetReadedSize() int {
	return r.rbytes
}

// Advance proxy to buffer advance
func (r *Reader) Advance(n int) {
	r.b.Advance(n)
}

// Mark return buf read pos.
func (r *Reader) Mark() int {
	return r.b.r
}

// AdvanceTo reset buffer read pos.
func (r *Reader) AdvanceTo(mark int) {
	r.Advance(mark - r.b.r)
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

// ReadLine will read until meet the first crlf bytes.
func (r *Reader) ReadLine() (line []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	idx := bytes.Index(r.b.buf[r.b.r:r.b.w], crlfBytes)
	if idx == -1 {
		line = nil
		err = ErrBufferFull
		return
	}
	line = r.b.buf[r.b.r : r.b.r+idx+2]
	r.b.r += idx + 2
	return
}

// ReadSlice will read until the delim or return ErrBufferFull.
// It never contains any I/O operation
func (r *Reader) ReadSlice(delim byte) (data []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
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
	if r.err != nil {
		return nil, r.err
	}
	if r.b.buffered() < n {
		err = ErrBufferFull
		return
	}
	data = r.b.buf[r.b.r : r.b.r+n]
	r.b.r += n
	return
}

const (
	maxWritevSize = 1024
)

// Writer implements buffering for an io.Writer object.
// If an error occurs writing to a Writer, no more data will be
// accepted and all subsequent writes, and Flush, will return the error.
// After all data has been written, the client should call the
// Flush method to guarantee all data has been forwarded to
// the underlying io.Writer.
type Writer struct {
	wr    *libnet.Conn
	bufsp net.Buffers
	bufs  [][]byte
	cnt   int

	err error
}

// NewWriter returns a new Writer whose buffer has the default size.
func NewWriter(wr *libnet.Conn) *Writer {
	return &Writer{wr: wr, bufs: make([][]byte, 0, maxWritevSize)}
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.bufs) == 0 {
		return nil
	}
	w.bufsp = net.Buffers(w.bufs[:w.cnt])
	_, err := w.wr.Writev(&w.bufsp)
	if err != nil {
		w.err = err
	}
	w.bufs = w.bufs[:0]
	w.cnt = 0
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
	w.bufs = append(w.bufs, p)
	w.cnt++
	if len(w.bufs) == maxWritevSize {
		err = w.Flush()
	}
	return
}
