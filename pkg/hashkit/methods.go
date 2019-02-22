package hashkit

import (
	"crypto/md5"
)

func hashOneOnTime(key []byte) uint {
	var value uint32
	for _, c := range key {
		val := uint32(c)
		value += val
		value += value << 10
		value ^= value >> 6
	}

	value += value << 3
	value ^= value >> 11
	value += value << 15
	return uint(value)
}

func hashMD5(key []byte) uint {
	m := md5.New()
	m.Write(key)
	results := m.Sum(nil)
	return uint(
		(uint32(results[3]&0xFF) << 24) |
			(uint32(results[2]&0xFF) << 16) |
			(uint32(results[1]&0xFF) << 8) |
			(uint32(results[0]) & 0xFF))
}
