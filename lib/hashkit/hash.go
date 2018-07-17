package hashkit

// constants defines
const (
	HashMethodFnv1a = "fnv1a_64"
)

// Ring is a abstraction
type Ring interface {
	// AddNode will add node into this ring
	// if the ring is ketma hash args contains only one.
	AddNode(node string, args ...int)

	// DelNode will remove node.
	DelNode(node string)

	// UpdateSlot will update single one slot due to redis move/ask
	UpdateSlot(node string, slot int)

	GetNode(key []byte) (string, bool)

	Init(masters []string, slots ...[]int)
}

// NewRing will create new and need init method.
func NewRing(des, method string) Ring {
	if des == "redis_cluster" {
		return newRedisClusterRing()
	}

	var hash func([]byte) uint
	switch method {
	case HashMethodFnv1a:
		fallthrough
	default:
		hash = NewFnv1a64().fnv1a64
	}
	return newRingWithHash(hash)
}
