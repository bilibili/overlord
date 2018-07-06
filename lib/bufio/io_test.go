package bufio

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

const fbyte = byte('f')

func _genData() []byte {
	bts := bytes.Repeat([]byte("abcde"), 3*100)
	bts[len(bts)-1] = fbyte
	return bts
}

func TestReaderRead(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	err := b.Read()
	assert.NoError(t, err)
}

func TestReaderReadUntil(t *testing.T) {
	bts := _genData()
	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	data, err := b.ReadUntil(fbyte)
	assert.NoError(t, err)
	assert.Len(t, data, 5*3*100)
}

func TestReaderReadSlice(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	b.Read()
	data, err := b.ReadSlice('c')
	assert.NoError(t, err)
	assert.Len(t, data, 3)
}

func TestReaderReadFull(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	data, err := b.ReadFull(1200)
	assert.NoError(t, err)
	assert.Len(t, data, 1200)
}

func TestReaderReadExact(t *testing.T) {
	bts := _genData()

	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))
	b.Read()
	data, err := b.ReadExact(5)
	assert.NoError(t, err)
	assert.Len(t, data, 5)
}

func TestReaderResetBuffer(t *testing.T) {
	bts := _genData()
	b := NewReader(bytes.NewBuffer(bts), Get(defaultBufferSize))

	_, err := b.ReadFull(1200)
	assert.NoError(t, err)

	b.ResetBuffer(Get(defaultBufferSize))
	data, err := b.ReadFull(300)
	assert.NoError(t, err)
	assert.Len(t, data, 300)

	_, err = b.ReadFull(300)
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)
}

// func TestWriterWriteOk(t *testing.T) {
// 	data := "Bilibili 干杯 - ( ゜- ゜)つロ"
// 	buf := bytes.NewBuffer([]byte{})
// 	w := NewWriter(buf)
// 	err := w.Write([]byte(data))
// 	assert.NoError(t, err)

// 	err = w.Write([]byte(data))
// 	assert.NoError(t, err)

// 	err = w.WriteString(data)
// 	assert.NoError(t, err)

// 	err = w.Flush()
// 	assert.NoError(t, err)
// }
