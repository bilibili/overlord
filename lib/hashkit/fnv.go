package hashkit

import (
	"hash"
	"hash/fnv"
	"sync"
)

// Fnv define fnv1 hash.
type Fnv struct {
	pool sync.Pool
}

// NewFnv1a32 return fnv with fnv1a32.
func NewFnv1a32() *Fnv {
	h := &Fnv{}
	h.pool.New = func() interface{} { return fnv.New32a() }
	return h
}

func (f *Fnv) fnv1a32(key []byte) (value uint) {
	hs := f.pool.Get().(hash.Hash32)
	hs.Write(key)
	value = uint(hs.Sum32())
	hs.Reset()
	f.pool.Put(hs)
	return
}
