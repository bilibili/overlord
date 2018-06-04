package redis

import (
	"bufio"
	"errors"
	"io"
)

// errors
var (
	ErrMissMatchResponseType = errors.New("response type miss match, except: CacheTypeRedis")
	ErrNotSupportRESPType    = errors.New("-not support RESP type (+ ,- ,: ,$ ,* )")
)

var (
	nullBytes = []byte("-1\r\n")
)

type buffer struct {
	br *bufio.Reader
	bw *bufio.Writer
}

func newBuffer(rd io.Reader, w io.Writer) *buffer {
	return &buffer{
		br: bufio.NewReader(rd),
		bw: bufio.NewWriter(w),
	}
}
