package hashkit

import (
	"hash"
)

// Fnv define fnv1 hash.
type Fnv struct {
}

// NewFnv1a64 return fnv with fnv1a64.
func NewFnv1a64() *Fnv {
	h := &Fnv{}
	return h
}

func (f *Fnv) fnv1a64(key []byte) (value uint) {
	var hs = new(sum64a)
	*hs = offset64
	hs.Write(key)
	value = uint(hs.Sum64())
	return
}

type (
	sum64a uint64
)

const (
	prime64  = 1099511628211
	offset64 = 14695981039346656037
)

// New64a new fnv1a64 same as twemproxy.
func New64a() hash.Hash64 {
	var s sum64a = offset64
	return &s
}

func (s *sum64a) Reset()         { *s = offset64 }
func (s *sum64a) Sum64() uint64  { return uint64(*s) }
func (s *sum64a) Size() int      { return 8 }
func (s *sum64a) BlockSize() int { return 1 }

func (s *sum64a) Sum(in []byte) []byte {
	v := uint64(*s)
	return append(in, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (s *sum64a) Write(data []byte) (int, error) {
	hash := uint32(*s)
	for _, c := range data {
		hash ^= uint32(c)
		hash *= uint32(prime64 & 0x0000ffff)
	}
	*s = sum64a(hash)
	return len(data), nil
}
