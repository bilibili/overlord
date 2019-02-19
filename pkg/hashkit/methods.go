package hashkit

import (
	"crypto/md5"
)

func hashOneOnTime(key []byte) uint {
	var val uint32
	for _, c := range key {
		uc := uint32(c)
		val += uc
		val += uc << 3
		val += uc << 10
		val ^= (uc >> 6)
	}

	val += val << 3
	val ^= val >> 11
	val += val << 15
	return uint(val)
}

func hashMD5(key []byte) uint {
	m := md5.New()
	results := m.Sum(key)
	return uint(((uint32)(results[3]&0xFF) << 24) | ((uint32)(results[2]&0xFF) << 16) | ((uint32)(results[1]&0xFF) << 8) | (uint32(results[0]) & 0xFF))
}
