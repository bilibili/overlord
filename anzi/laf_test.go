package anzi

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLzfDecompressOk(t *testing.T) {
	data := []byte{1, 97, 97, 224, 246, 0, 1, 97, 97}
	ulen := int64(259)

	output := lzfDecompress(data, ulen)
	expected := strings.Repeat("a", int(ulen))
	assert.Equal(t, expected, string(output))
}

func TestLzfDecompressNoData(t *testing.T) {
	output := lzfDecompress([]byte{}, 0)
	assert.Len(t, output, 0)
}
