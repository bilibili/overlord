package bufio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferGrowOk(t *testing.T) {
	b := Get(defaultBufferSize)
	b.grow()
	assert.Equal(t, 0, b.r)
	assert.Equal(t, 0, b.w)
	assert.Len(t, b.buf, defaultBufferSize*2)
	assert.Equal(t, len(b.buf), b.len())
}

func TestBuffer(t *testing.T) {
	b := Get(defaultBufferSize)
	assert.Len(t, b.buf, defaultBufferSize)
	assert.Len(t, b.Bytes(), 0)
	b.w = 1
	assert.Len(t, b.Bytes(), 1)
	b.Reset()
	assert.Len(t, b.Bytes(), 0)
}

func TestGetOk(t *testing.T) {
	b := Get(defaultBufferSize)
	assert.Len(t, b.buf, defaultBufferSize)

	b = Get(maxBufferSize)
	assert.Len(t, b.buf, maxBufferSize)

	b = Get(maxBufferSize + 1)
	assert.Len(t, b.buf, maxBufferSize+1)
}

func TestBufferAdvance(t *testing.T) {
	b := Get(defaultBufferSize)
	b.r += 100
	b.Advance(-10)
	assert.Equal(t, 90, b.r)
}
