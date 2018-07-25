package hashkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRingOk(t *testing.T) {
	ring := NewRing("redis_cluster", "crc16")
	assert.NotNil(t, ring)

	ring = NewRing("ketama", "fnv1a_64")
	assert.NotNil(t, ring)
}
