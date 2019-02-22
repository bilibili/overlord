package hashkit

import "github.com/aviddiviner/go-murmur"

func hashMurmur(key []byte) uint {
	var uklen = uint32(len(key))
	var seed = 0xdeadbeef * uklen
	return uint(murmur.MurmurHash2(key, seed))
}
