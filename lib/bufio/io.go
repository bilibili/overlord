package bufio

import (
	"bytes"
	"io"
	"net"
)

// Reader implements buffering for an io.Reader object.
type Reader struct {
	rd io.Reader
	b  *Buffer
}

// NewReader returns a new Reader whose buffer has the default size.
func NewReader(rd io.Reader, b *Buffer) *Reader {
	return &Reader{rd: rd, b: b}
}

// NewReaderSize returns a new Reader whose buffer has at least the specified
// size. If the argument io.Reader is already a Reader with large enough
// size, it returns the underlying Reader.
// func NewReaderSize(rd io.Reader, size int) *Reader {
// 	if size <= 0 {
// 		size = defaultBufferSize
// 	}
// 	return &Reader{rd: rd, buf: make([]byte, size)}
// }

func (r *Reader) fill() error {
	// if r.err != nil {
	// 	return r.err
	// }
	// if r.rpos > 0 {
	// 	n := copy(r.buf, r.buf[r.rpos:r.wpos])
	// 	r.rpos = 0
	// 	r.wpos = n
	// }
	n, err := r.rd.Read(r.b.buf[r.b.w:])
	if err != nil {
		return err
	} else if n == 0 {
		return io.ErrNoProgress
	} else {
		r.b.w += n
	}
	return nil
}

// func (r *Reader) buffered() int {
// 	return r.b.w - r.b.r
// }

// ResetBuffer reset buf.
func (r *Reader) ResetBuffer(b *Buffer) {
	b.Reset()
	n := 0
	if r.b != nil {
		if r.b.buffered() > 0 {
			n = copy(b.buf, r.b.buf[r.b.r:r.b.w])
		}
		Put(r.b)
	}
	r.b = b
	r.b.w = n
}

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying Reader,
// hence n may be less than len(p).
// At EOF, the count will be zero and err will be io.EOF.
// func (r *Reader) Read(p []byte) (int, error) {
// 	if len(p) == 0 {
// 		return 0, nil
// 	}
// 	if r.buffered() == 0 {
// 		if len(p) >= len(b.buf) {
// 			n, err := b.rd.Read(p)
// 			if err != nil {
// 				b.err = err
// 			}
// 			return n, b.err
// 		}
// 		if b.fill() != nil {
// 			return 0, b.err
// 		}
// 	}
// 	n := copy(p, b.buf[b.rpos:b.wpos])
// 	b.rpos += n
// 	return n, nil
// }

// ReadByte reads and returns a single byte.
// If no byte is available, returns an error.
// func (b *Reader) ReadByte() (byte, error) {
// 	if b.err != nil {
// 		return 0, b.err
// 	}
// 	if b.buffered() == 0 {
// 		if b.fill() != nil {
// 			return 0, b.err
// 		}
// 	}
// 	c := b.buf[b.rpos]
// 	b.rpos++
// 	return c, nil
// }

// ReadUntil reads until the first occurrence of delim in the input,
// returning a slice pointing at the bytes in the buffer.
// The bytes stop being valid at the next read.
// If ReadUntil encounters an error before finding a delimiter,
// it returns all the data in the buffer and the error itself (often io.EOF).
// ReadUntil returns err != nil if and only if line does not end in delim.
func (r *Reader) ReadUntil(delim byte) ([]byte, error) {
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
		if err := r.fill(); err != nil {
			return nil, err
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
// func (b *Reader) ReadBytes(delim byte) ([]byte, error) {
// 	var full [][]byte
// 	var last []byte
// 	var size int
// 	for last == nil {
// 		f, err := b.ReadSlice(delim)
// 		if err != nil {
// 			if err != bufio.ErrBufferFull {
// 				return nil, b.err
// 			}
// 			dup := b.slice.Make(len(f))
// 			copy(dup, f)
// 			full = append(full, dup)
// 		} else {
// 			last = f
// 		}
// 		size += len(f)
// 	}
// 	var n int
// 	var buf = b.slice.Make(size)
// 	for _, frag := range full {
// 		n += copy(buf[n:], frag)
// 	}
// 	copy(buf[n:], last)
// 	return buf, nil
// }

// ReadFull reads exactly n bytes from r into buf.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading some but not all the bytes,
// ReadFull returns ErrUnexpectedEOF.
// On return, n == len(buf) if and only if err == nil.
func (r *Reader) ReadFull(n int) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	for {
		if r.b.buffered() >= n {
			bs := r.b.buf[r.b.r : r.b.r+n]
			r.b.r += n
			return bs, nil
		}
		if r.b.len()-r.b.w < n-r.b.buffered() {
			r.b.grow()
		}
		if err := r.fill(); err != nil {
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
	wr   io.Writer
	bufs net.Buffers

	err error
}

// NewWriter returns a new Writer whose buffer has the default size.
func NewWriter(wr io.Writer) *Writer {
	return &Writer{wr: wr, bufs: net.Buffers(make([][]byte, 0, 128))}
}

// NewWriterSize returns a new Writer whose buffer has at least the specified
// size. If the argument io.Writer is already a Writer with large enough
// size, it returns the underlying Writer.
// func NewWriterSize(wr io.Writer, size int) *Writer {
// 	if size <= 0 {
// 		size = defaultBufferSize
// 	}
// 	return &Writer{wr: wr, buf: make([]byte, size)}
// }

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.bufs) == 0 {
		return nil
	}
	_, err := w.bufs.WriteTo(w.wr)
	if err != nil {
		w.err = err
	}
	w.bufs = w.bufs[0:0]
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
	if len(w.bufs) == 10 {
		w.Flush()
	}
	w.bufs = append(w.bufs, p)
	return nil
}

// WriteString writes a string.
// It returns the number of bytes written.
// If the count is less than len(s), it also returns an error explaining
// why the write is short.
func (w *Writer) WriteString(s string) (err error) {
	return w.Write([]byte(s))
}
