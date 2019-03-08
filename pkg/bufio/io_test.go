package bufio

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"overlord/pkg/mockconn"
	libnet "overlord/pkg/net"

	"github.com/stretchr/testify/assert"
)

const fbyte = byte('f')

func _genData() []byte {
	bts := bytes.Repeat([]byte("abcde"), 3*100)
	bts[len(bts)-1] = fbyte
	return bts
}

func TestReaderAdvance(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	_ = b.Read()
	_, _ = b.ReadExact(5)
	b.Advance(5)

	buf := b.Buffer()
	assert.NotNil(t, buf)

	assert.Len(t, buf.Bytes(), 502)
	b.Advance(-10)
	assert.Len(t, buf.Bytes(), 512)

	_, _ = b.ReadExact(10)
	m := b.Mark()
	assert.Equal(t, 10, m)

	b.AdvanceTo(5)
	m = b.Mark()
	assert.Equal(t, 5, m)
	assert.Equal(t, 5, b.Buffer().r)
}

func TestReaderRead(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	err := b.Read()
	assert.NoError(t, err)

	b.err = errors.New("some error")
	err = b.Read()
	assert.EqualError(t, err, "some error")
}

func TestReaderReadSlice(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	_ = b.Read()
	data, err := b.ReadSlice('c')
	assert.NoError(t, err)
	assert.Len(t, data, 3)

	_, err = b.ReadSlice('\n')
	assert.Equal(t, ErrBufferFull, err)
}

func TestReaderReadLine(t *testing.T) {
	b := NewReader(bytes.NewBuffer([]byte("abcd\r\nabc")), Get(defaultBufferSize))
	_ = b.Read()
	data, err := b.ReadLine()
	assert.NoError(t, err)
	assert.Len(t, data, 6)
}

func TestReaderReadExact(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	_ = b.Read()
	data, err := b.ReadExact(5)
	assert.NoError(t, err)
	assert.Len(t, data, 5)

	_, err = b.ReadExact(5 * 3 * 100)
	assert.Equal(t, ErrBufferFull, err)
}

func TestWriterWriteOk(t *testing.T) {
	data := "Bilibili 干杯 - ( ゜- ゜)つロ"
	conn := libnet.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
	w := NewWriter(conn)

	err := w.Flush()
	assert.NoError(t, err)

	err = w.Write([]byte(data))
	assert.NoError(t, err)

	err = w.Write([]byte(data))
	assert.NoError(t, err)

	err = w.Flush()
	assert.NoError(t, err)

	w.err = errors.New("some error")
	err = w.Write([]byte(data))
	assert.EqualError(t, err, "some error")
	err = w.Flush()
	assert.EqualError(t, err, "some error")
}
