package memcache

import (
	"io"
	"net"
	"strings"

	"github.com/felixhao/overlord/lib/bufio"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

const (
	errorPrefix       = "ERROR"
	clientErrorPrefix = "CLIENT_ERROR "
	serverErrorPrefix = "SERVER_ERROR "

	encoderBufferSize = 64 * 1024 // NOTE: keep writing data into client, so relatively large
)

type encoder struct {
	bw *bufio.Writer
}

// NewEncoder new a memcache encoder.
func NewEncoder(w io.Writer) proto.Encoder {
	e := &encoder{
		bw: bufio.NewWriterSize(w, encoderBufferSize),
	}
	return e
}

// Encode encode response and write into writer.
func (e *encoder) Encode(msg *proto.Msg) (err error) {
	err = msg.Err()
	if err != nil {
		se := errors.Cause(err).Error()
		if !strings.HasPrefix(se, errorPrefix) && !strings.HasPrefix(se, clientErrorPrefix) && !strings.HasPrefix(se, serverErrorPrefix) { // NOTE: the mc error protocol
			e.bw.WriteString(serverErrorPrefix)
		}
		e.bw.WriteString(se)
		e.bw.Write(crlfBytes)
	} else {
		msg.Resp.WriteTo(e.bw)
	}
	if fe := e.bw.Flush(); fe != nil {
		err = errors.Wrap(fe, "MC Encoder encode response flush bytes")
	}
	releaseBuffers(&msg.Resp)
	bufio.Put(msg.Buf())
	return
}

func releaseBuffers(b *net.Buffers) {
	d := [][]byte(*b)
	for _, v := range d {
		if v != nil {
			bufio.Put(v)
		}
	}
}
