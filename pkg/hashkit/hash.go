package hashkit

// constants defines
const (
	HashMethodFnv1a64 = "fnv1a_64"
	HashMethodFnv1a32 = "fnv1a_32"
	HashMethodFnv164  = "fnv1_64"
	HashMethodFnv132  = "fnv1_32"

	HashMethodCRC16  = "crc16"
	HashMethodCRC32  = "crc32"
	HashMethodCRC32a = "crc32a"

	HashMethodMD5       = "md5"
	HashMethodOneOnTime = "one_on_time"
	HashMethodHsieh     = "hsieh"
	HashMethodMurmur    = "murmur"
)

// NewRing will create new and need init method.
func NewRing(des, method string) *HashRing {

	var hash func([]byte) uint
	switch method {

	case HashMethodFnv1a64: // fnv family
		hash = hashFnv1a64
	case HashMethodFnv164:
		hash = hashFnv164
	case HashMethodFnv1a32:
		hash = hashFnv1a32
	case HashMethodFnv132:
		hash = hashFnv132

	case HashMethodCRC32a: // crc family
		hash = hashCrc32a
	case HashMethodCRC32:
		hash = hashCrc32
	case HashMethodCRC16:
		hash = hashCrc16

	case HashMethodMD5: // others
		hash = hashMD5
	case HashMethodOneOnTime:
		hash = hashOneOnTime
	case HashMethodHsieh:
		hash = hashHsieh
	case HashMethodMurmur:
		hash = hashMurmur
	default:
		hash = hashFnv1a64
	}
	return newRingWithHash(hash)
}
