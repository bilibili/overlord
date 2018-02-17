package bufio_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/felixhao/overlord/lib/bufio"
)

func newReader(n int, input string) *bufio.Reader {
	return bufio.NewReaderSize(strings.NewReader(input), n)
}

func TestRead(t *testing.T) {
	var b bytes.Buffer
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&b, "hello world %d", i)
	}
	var input = b.String()
	for n := 1; n < len(input); n++ {
		r := newReader(n, input)
		b := make([]byte, len(input))
		_, err := io.ReadFull(r, b)
		if err != nil {
			t.Fatalf("read error:%v", err)
		}
		if string(b) != input {
			t.Fatalf("input(%s) not equal read(%s)", input, b)
		}
	}
}

func TestReadByte(t *testing.T) {
	var input = "hello world"
	for n := 1; n < len(input); n++ {
		r := newReader(n, input)
		var s string
		for i := 0; i < len(input); i++ {
			b, err := r.ReadByte()
			if err != nil {
				t.Fatalf("read byte error:%v", err)
			}
			if b != input[i] {
				t.Fatalf("input byte(%v) not equal read byte(%v)", input[i], b)
			}
			s += string(b)
		}
		if s != input {
			t.Fatalf("input(%s) not integral(%s)", input, s)
		}
	}
}

func TestReadBytes(t *testing.T) {
	var b bytes.Buffer
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&b, "hello world %d ", i)
	}
	var input = b.String()
	for n := 1; n < len(input); n++ {
		r := newReader(n, input)
		var s string
		for i := 0; i < 30; i++ {
			b, err := r.ReadBytes(' ')
			if err != nil {
				t.Fatalf("read bytes error:%v", err)
			}
			s += string(b)
		}
		if s != input {
			t.Fatalf("input(%s) not integral(%s)", input, s)
		}
	}
}

func TestReadFull(t *testing.T) {
	var b bytes.Buffer
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&b, "hello world %d ", i)
	}
	var input = b.String()
	for n := 1; n < len(input); n++ {
		r := newReader(n, input)
		b, err := r.ReadFull(len(input))
		if err != nil {
			t.Fatalf("read full bytes error:%v", err)
		}
		if string(b) != input {
			t.Fatalf("input(%s) not integral(%s)", input, string(b))
		}
	}
}

func newWriter(n int, b *bytes.Buffer) *bufio.Writer {
	return bufio.NewWriterSize(b, n)
}

func TestWrite(t *testing.T) {
	for n := 1; n < 20; n++ {
		var input string
		var b bytes.Buffer
		var w = newWriter(n, &b)
		for i := 0; i < 10; i++ {
			s := fmt.Sprintf("hello world %d", i)
			_, err := w.Write([]byte(s))
			if err != nil {
				t.Fatalf("write bytes error:%v", err)
			}
			input += s
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("write flush error:%v", err)
		}
		if b.String() != input {
			t.Fatalf("input(%s) not integral(%s)", input, b.String())
		}
	}
}

func TestWriteBytes(t *testing.T) {
	var input = "hello world!!"
	for n := 1; n < 20; n++ {
		var b bytes.Buffer
		var w = newWriter(n, &b)
		for i := 0; i < len(input); i++ {
			err := w.WriteByte(input[i])
			if err != nil {
				t.Fatalf("write byte error:%v", err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("write flush error:%v", err)
		}
		if b.String() != input {
			t.Fatalf("input(%s) not integral(%s)", input, b.String())
		}
	}
}

func TestWriteString(t *testing.T) {
	for n := 1; n < 20; n++ {
		var input string
		var b bytes.Buffer
		var w = newWriter(n, &b)
		for i := 0; i < 10; i++ {
			s := fmt.Sprintf("hello world %d", i)
			_, err := w.WriteString(s)
			if err != nil {
				t.Fatalf("write string error:%v", err)
			}
			input += s
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("write flush error:%v", err)
		}
		if b.String() != input {
			t.Fatalf("input(%s) not integral(%s)", input, b.String())
		}
	}
}
