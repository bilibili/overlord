package bufio

import (
	"bufio"
	"bytes"
	"io"
	"net"
)

// Reader implements buffering for an io.Reader object.
type Reader struct {
	err error
	buf []byte

	rd   io.Reader
	rpos int
	wpos int
}

// NewReader returns a new Reader whose buffer has the default size.
func NewReader(rd io.Reader) *Reader {
	return NewReaderSize(rd, defaultBufferSize)
}

// NewReaderSize returns a new Reader whose buffer has at least the specified
// size. If the argument io.Reader is already a Reader with large enough
// size, it returns the underlying Reader.
func NewReaderSize(rd io.Reader, size int) *Reader {
	if size <= 0 {
		size = defaultBufferSize
	}
	return &Reader{rd: rd, buf: Get(size)}
}

// ResetBuffer bytes buffer
func (b *Reader) ResetBuffer(buf []byte) {
	b.rpos = 0
	b.wpos = 0
	b.buf = buf
}

// Buffer will return the slice of real data
func (b *Reader) Buffer() []byte {
	return b.buf
}

func (b *Reader) grow() {
	b.growTo(growFactor * len(b.buf))
}

func (b *Reader) growTo(size int) {
	if len(b.buf) > size {
		return
	}

	// TODO(wayslog): recycle using of global bytes pool
	buf := Get(size)
	copy(buf, b.buf[b.rpos:b.wpos])

	// Release it first
	Put(b.buf)
	b.buf = buf
	b.wpos = b.wpos - b.rpos
	b.rpos = 0
}

func (b *Reader) fill() error {
	if b.err != nil {
		return b.err
	}
	if b.rpos > 0 {
		n := copy(b.buf, b.buf[b.rpos:b.wpos])
		b.rpos = 0
		b.wpos = n
	}
	n, err := b.rd.Read(b.buf[b.wpos:])
	if err != nil {
		b.err = err
	} else if n == 0 {
		b.err = io.ErrNoProgress
	} else {
		b.wpos += n
	}
	return b.err
}

func (b *Reader) buffered() int {
	return b.wpos - b.rpos
}

// Read reads data into p.
// NOTICE: We don't recomand to use it any more.
func (b *Reader) Read(p []byte) (int, error) {
	if b.err != nil || len(p) == 0 {
		return 0, b.err
	}
	if b.buffered() == 0 {
		if len(p) >= len(b.buf) {
			n, err := b.rd.Read(p)
			if err != nil {
				b.err = err
			}
			return n, b.err
		}
		if b.fill() != nil {
			return 0, b.err
		}
	}
	n := copy(p, b.buf[b.rpos:b.wpos])
	b.rpos += n
	return n, nil
}

// Bytes will return the reference of the read data array.
func (b *Reader) Bytes() []byte {
	return b.buf[b.rpos:b.wpos]
}

// Advance will trying to advance the rpos
func (b *Reader) Advance(size int) {
	if b.rpos+size > b.wpos {
		b.rpos = b.wpos
	} else {
		b.rpos += size
	}
}

// ReadAll will trying to read bytes data to fill buffer,
// if b.buffered() == len(b.buf), the buffer will grow
// ReadAll will never grow b.rpos, but can reduce it by
// using fill or grow.
func (b *Reader) ReadAll() error {
	if b.err != nil {
		return b.err
	}
	if b.buffered() == len(b.buf) {
		b.grow()
	}
	return b.fill()
}

// ReadByte reads and returns a single byte.
// If no byte is available, returns an error.
func (b *Reader) ReadByte() (byte, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.buffered() == 0 {
		if b.fill() != nil {
			return 0, b.err
		}
	}
	c := b.buf[b.rpos]
	b.rpos++
	return c, nil
}

// Peek some bytes and don't have any I/O or buffer operation
// if buffered is not long enough to slice the n bytes,
// error will be return as bufio.ErrBufferFull
func (b *Reader) Peek(n int) ([]byte, error) {
	if b.buffered() < n {
		return b.buf[b.rpos:b.wpos], bufio.ErrBufferFull
	}
	return b.buf[b.rpos : b.rpos+n], nil
}

// ReadUntil reads until the first delim occur,
// returning a slice pointing at the bytes in the reader's buffer.
// if ReadUntil encounters an error before finding a delimiter,
// it returns all the data in the buffer and the err itself(offten io.EOF).
// ReadUntil will grow local buffer to read until the buffer is large enough
// to contains all the bytes from begin until the occurrence of the delim.
// So that this function will never return ErrBufferFull.
// Notice, the data will be overwritten by the next I/O operation.
func (b *Reader) ReadUntil(delim byte) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	for {
		var index = bytes.IndexByte(b.buf[b.rpos:b.wpos], delim)
		if index != -1 {
			limit := b.rpos + index + 1
			buf := b.buf[b.rpos:limit]
			b.rpos = limit
			return buf, nil
		}

		if b.buffered() == len(b.buf) {
			b.grow()
		}

		if b.fill() != nil {
			return b.buf, b.err
		}
	}
}

// ReReadUntilBytes must be called after last Read operation, so that we can have enough
// buffer size to backoff, it will read until the first occurrence
// of delims.
func (b *Reader) ReReadUntilBytes(delims []byte, backoff int) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	b.rpos -= backoff

	for {
		var index = bytes.Index(b.buf[b.rpos+backoff:b.wpos], delims)
		if index != -1 {
			limit := b.rpos + backoff + index + len(delims)
			slice := b.buf[b.rpos:limit]
			b.rpos = limit
			return slice, nil
		}

		if b.buffered() == len(b.buf) {
			b.grow()
		}

		if b.fill() != nil {
			return nil, b.err
		}
	}
}

// ReReadUntil must be called after last Read operation, so that we can have enough
// buffer size to backoff, it will read until the first occurrence
// of delim but contains backoff size bytes as a prefix
// of return datas.
// In redis protocol, you might look up `\r\n`, you can call:
//     buf, _ := b.ReadUntil('\n')
// But, if buf[len(buf)-2] != '\r'
// you can use the function in loop as follows:
//     buf, _ := b.ReReadUntil('\n', len(buf))
// until buf[len(buf)-2] == '\r'
func (b *Reader) ReReadUntil(delim byte, backoff int) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}

	b.rpos -= backoff
	for {
		var index = bytes.IndexByte(b.buf[b.rpos+backoff:b.wpos], delim)
		if index != -1 {
			limit := b.rpos + backoff + index + 1
			slice := b.buf[b.rpos:limit]
			b.rpos = limit
			return slice, nil
		}

		if b.buffered() == len(b.buf) {
			b.grow()
		}

		if b.fill() != nil {
			return nil, b.err
		}
	}
}

// ReReadFull must be called after last Read operation, so that we can
// have enough buffer size to backoff, it will read until the size is fill now.
func (b *Reader) ReReadFull(size int, backoff int) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}

	b.rpos -= backoff
	if len(b.buf) < size+backoff {
		b.growTo(size + backoff)
	}

	for {
		if b.buffered() >= size+backoff {
			buf := b.buf[b.rpos : b.rpos+size+backoff]
			b.rpos += size + backoff
			return buf, nil
		}

		if b.fill() == nil {
			return b.buf, b.err
		}
	}
}

// ReadFull reads exactly n bytes from r into buf.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading some but not all the bytes,
// ReadFull returns ErrUnexpectedEOF.
// On return, n == len(buf) if and only if err == nil.
func (b *Reader) ReadFull(n int) ([]byte, error) {
	if b.err != nil || n == 0 {
		return nil, b.err
	}

	if b.buffered() >= n {
		pos := b.rpos
		b.rpos += n
		return b.buf[pos:b.rpos], nil
	}

	if n > len(b.buf) {
		b.growTo(n + len(b.buf))
	}

	for b.buffered() < n {
		if b.fill() != nil {
			return b.buf, b.err
		}
	}
	buf := b.buf[b.rpos:b.wpos]
	b.rpos += n
	return buf, nil
}

// Writer implements buffering for an io.Writer object.
// If an error occurs writing to a Writer, no more data will be
// accepted and all subsequent writes, and Flush, will return the error.
// After all data has been written, the client should call the
// Flush method to guarantee all data has been forwarded to
// the underlying io.Writer.
type Writer struct {
	err error
	buf []byte

	wr   io.Writer
	wpos int
}

// NewWriter returns a new Writer whose buffer has the default size.
func NewWriter(wr io.Writer) *Writer {
	return NewWriterSize(wr, defaultBufferSize)
}

// NewWriterSize returns a new Writer whose buffer has at least the specified
// size. If the argument io.Writer is already a Writer with large enough
// size, it returns the underlying Writer.
func NewWriterSize(wr io.Writer, size int) *Writer {
	if size <= 0 {
		size = defaultBufferSize
	}
	return &Writer{wr: wr, buf: Get(size)}
}

// Flush writes any buffered data to the underlying io.Writer.
func (b *Writer) Flush() error {
	return b.flush()
}

func (b *Writer) flush() error {
	if b.err != nil {
		return b.err
	}
	if b.wpos == 0 {
		return nil
	}
	n, err := b.wr.Write(b.buf[:b.wpos])
	if err != nil {
		b.err = err
	} else if n < b.wpos {
		b.err = io.ErrShortWrite
	} else {
		b.wpos = 0
	}
	return b.err
}

func (b *Writer) available() int {
	return len(b.buf) - b.wpos
}

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (b *Writer) Write(p []byte) (nn int, err error) {
	for b.err == nil && len(p) > b.available() {
		var n int
		if b.wpos == 0 {
			n, b.err = b.wr.Write(p)
		} else {
			n = copy(b.buf[b.wpos:], p)
			b.wpos += n
			b.flush()
		}
		nn, p = nn+n, p[n:]
	}
	if b.err != nil || len(p) == 0 {
		return nn, b.err
	}
	n := copy(b.buf[b.wpos:], p)
	b.wpos += n
	return nn + n, nil
}

// WriteByte writes a single byte.
func (b *Writer) WriteByte(c byte) error {
	if b.err != nil {
		return b.err
	}
	if b.available() == 0 && b.flush() != nil {
		return b.err
	}
	b.buf[b.wpos] = c
	b.wpos++
	return nil
}

// WriteString writes a string.
// It returns the number of bytes written.
// If the count is less than len(s), it also returns an error explaining
// why the write is short.
func (b *Writer) WriteString(s string) (nn int, err error) {
	for b.err == nil && len(s) > b.available() {
		n := copy(b.buf[b.wpos:], s)
		b.wpos += n
		b.flush()
		nn, s = nn+n, s[n:]
	}
	if b.err != nil || len(s) == 0 {
		return nn, b.err
	}
	n := copy(b.buf[b.wpos:], s)
	b.wpos += n
	return nn + n, nil
}

// writeBuffers impl the net.buffersWriter to support writev
func (b *Writer) writeBuffers(buf *net.Buffers) (int64, error) {
	return buf.WriteTo(b.wr)
}

// Buffer will return the slice of real data
func (b *Writer) Buffer() []byte {
	return b.buf
}

// ResetBuffer may always be called after Flush to flush
// datas into connection.
func (b *Writer) ResetBuffer(buf []byte) {
	b.wpos = len(buf)
	b.buf = buf
}
