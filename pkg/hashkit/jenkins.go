package hashkit

func hashJenkins(key []byte) uint {
	// this may be error when meet little endian
	var hash uint32
	for _, b := range key {
		hash += uint32(b)
		hash += (hash << 10)
		hash ^= (hash >> 6)
	}

	hash += (hash << 3)
	hash ^= (hash >> 11)
	hash += (hash << 15)

	return uint(hash)
}
