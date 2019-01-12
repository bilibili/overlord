package hashkit

// constants defines
const (
	HashMethodFnv1a = "fnv1a_64"
)

// NewRing will create new and need init method.
func NewRing(des, method string) *HashRing {
	var hash func([]byte) uint
	switch method {
	case HashMethodFnv1a:
		fallthrough
	default:
		hash = fnv1a64
	}
	return newRingWithHash(hash)
}
