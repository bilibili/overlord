package hashkit

func get16bits(key []byte) uint32 {
	var uval = uint32(key[0])
	uval += uint32(key[1]) << 8
	return uval
}

func hashHsieh(key []byte) uint {
	if len(key) == 0 {
		return uint(0)
	}

	var (
		hash uint32
		tmp  uint32
		klen = len(key)
		rem  = klen & 3
	)

	klen >>= 2

	// main loop
	for ; klen > 0; klen-- {
		hash += get16bits(key)
		tmp = (get16bits(key[2:]) << 11) ^ hash
		hash = (hash << 16) ^ tmp
		key = key[4:]
		hash += hash >> 11
	}

	// deal with rem
	switch rem {
	case 3:
		hash += get16bits(key)
		hash ^= hash << 16
		hash ^= uint32(key[2]) << 18
		hash += hash >> 11
	case 2:
		hash += get16bits(key)
		hash ^= hash << 11
		hash += hash >> 17
	case 1:
		hash += uint32(key[0])
		hash ^= hash << 10
		hash += hash >> 1
	}

	// Force "avalanching" of final 127 bits
	hash ^= hash << 3
	hash += hash >> 5
	hash ^= hash << 4
	hash += hash >> 17
	hash ^= hash << 25
	hash += hash >> 6

	return uint(hash)
}
