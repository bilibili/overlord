package hashkit

const (
	prime64  = 1099511628211
	offset64 = 14695981039346656037

	prime64tw  = 1099511628211 & 0x0000ffff
	offset64tw = 14695981039346656037 & 0xffffffff

	prime32  = 16777619
	offset32 = 2166136261
)

func hashFnv1a64(key []byte) uint {
	hash := uint32(offset64tw)
	for _, c := range key {
		hash ^= uint32(c)
		hash *= uint32(prime64tw)
	}

	return uint(hash)
}

func hashFnv164(key []byte) uint {
	var hash uint64 = offset64
	for _, c := range key {
		hash *= prime64
		hash ^= uint64(c)
	}
	return uint(uint32(hash))
}

func hashFnv1a32(key []byte) uint {
	var hash uint32 = offset32
	for _, c := range key {
		hash ^= uint32(c)
		hash *= prime32
	}
	return uint(hash)
}

func hashFnv132(key []byte) (value uint) {
	var hash uint32 = offset32
	for _, c := range key {
		hash *= prime32
		hash ^= uint32(c)
	}
	return uint(hash)
}
