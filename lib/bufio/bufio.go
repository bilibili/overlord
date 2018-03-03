package bufio

import (
	"bufio"
	"bytes"
	"io"
)

const defaultBufferSize = 1024

// Reader implements buffering for an io.Reader object.
type Reader struct {
	err error
	buf []byte

	rd   io.Reader
	rpos int
	wpos int

	slice SliceAlloc
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
	return &Reader{rd: rd, buf: make([]byte, size)}
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
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying Reader,
// hence n may be less than len(p).
// At EOF, the count will be zero and err will be io.EOF.
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

// ReadSlice reads until the first occurrence of delim in the input,
// returning a slice pointing at the bytes in the buffer.
// The bytes stop being valid at the next read.
// If ReadSlice encounters an error before finding a delimiter,
// it returns all the data in the buffer and the error itself (often io.EOF).
// ReadSlice fails with error ErrBufferFull if the buffer fills without a delim.
// Because the data returned from ReadSlice will be overwritten
// by the next I/O operation, most clients should use ReadBytes instead.
// ReadSlice returns err != nil if and only if line does not end in delim.
func (b *Reader) ReadSlice(delim byte) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	for {
		var index = bytes.IndexByte(b.buf[b.rpos:b.wpos], delim)
		if index >= 0 {
			limit := b.rpos + index + 1
			slice := b.buf[b.rpos:limit]
			b.rpos = limit
			return slice, nil
		}
		if b.buffered() == len(b.buf) {
			b.rpos = b.wpos
			return b.buf, bufio.ErrBufferFull
		}
		if b.fill() != nil {
			return nil, b.err
		}
	}
}

// ReadBytes reads until the first occurrence of delim in the input,
// returning a slice containing the data up to and including the delimiter.
// If ReadBytes encounters an error before finding a delimiter,
// it returns the data read before the error and the error itself (often io.EOF).
// ReadBytes returns err != nil if and only if the returned data does not end in
// delim.
// For simple uses, a Scanner may be more convenient.
func (b *Reader) ReadBytes(delim byte) ([]byte, error) {
	var full [][]byte
	var last []byte
	var size int
	for last == nil {
		f, err := b.ReadSlice(delim)
		if err != nil {
			if err != bufio.ErrBufferFull {
				return nil, b.err
			}
			dup := b.slice.Make(len(f))
			copy(dup, f)
			full = append(full, dup)
		} else {
			last = f
		}
		size += len(f)
	}
	var n int
	var buf = b.slice.Make(size)
	for _, frag := range full {
		n += copy(buf[n:], frag)
	}
	copy(buf[n:], last)
	return buf, nil
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
	var buf = b.slice.Make(n)
	if _, err := io.ReadFull(b, buf); err != nil {
		return nil, err
	}
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
	return &Writer{wr: wr, buf: make([]byte, size)}
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
