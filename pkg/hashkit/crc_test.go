package hashkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCrcCheckOk(t *testing.T) {
	assert.Equal(t, uint16(0x31C3), Crc16([]byte("123456789")))
	assert.Equal(t, uint16(21847), Crc16([]byte{83, 153, 134, 118, 229, 214, 244, 75, 140, 37, 215, 215}))
}
