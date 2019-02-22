package hashkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllHashMethods(t *testing.T) {
	key := []byte("0123456789")
	assert.Equal(t, uint(3227819096), hashCrc16(key), "crc16")
	assert.Equal(t, uint(9860), hashCrc32(key), "crc32")
	assert.Equal(t, uint(2793719750), hashCrc32a(key), "crc32a")
	assert.Equal(t, uint(610147960), hashMD5(key), "md5")
	assert.Equal(t, uint(2336436402), hashFnv1a64(key), "fnv1a64")
	assert.Equal(t, uint(1576209164), hashFnv164(key), "fnv164")
	assert.Equal(t, uint(4185952242), hashFnv1a32(key), "fnv1a32")
	assert.Equal(t, uint(1737638188), hashFnv132(key), "fnv132")
	assert.Equal(t, uint(2264676836), hashHsieh(key), "hsieh")
	assert.Equal(t, uint(1957635836), hashMurmur(key), "murmur")
	assert.Equal(t, uint(2451084222), hashOneOnTime(key), "hash one on time")
}
