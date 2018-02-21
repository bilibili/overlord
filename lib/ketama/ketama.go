package ketama

import (
	"crypto/sha1"
	"hash"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

type nodeHash struct {
	node string
	hash uint
}

type tickArray struct {
	nodes  []nodeHash
	length int
}

func (p *tickArray) Len() int           { return p.length }
func (p *tickArray) Less(i, j int) bool { return p.nodes[i].hash < p.nodes[j].hash }
func (p *tickArray) Swap(i, j int)      { p.nodes[i], p.nodes[j] = p.nodes[j], p.nodes[i] }
func (p *tickArray) Sort()              { sort.Sort(p) }

// HashRing ketama hash ring.
type HashRing struct {
	spots int
	ticks atomic.Value
	pool  sync.Pool
}

// NewRing new a hash ring.
// Default hash: sha1
func NewRing(n int) (h *HashRing) {
	h = new(HashRing)
	h.spots = n
	h.pool.New = func() interface{} { return sha1.New() }
	return
}

// NewRingWithHash new a hash ring with a hash func.
// func NewRingWithHash(n int, hash func() hash.Hash) (h *HashRing) {
// 	h = new(HashRing)
// 	h.spots = n
// 	h.hash = hash
// 	return
// }

// Init init hash ring with nodes.
func (h *HashRing) Init(nodes []string, spots []int) {
	if len(nodes) != len(spots) {
		panic("nodes length not equal spots length")
	}
	var ticks []nodeHash
	hash := h.hash()
	for i := range nodes {
		tSpots := h.spots * spots[i]
		for j := 1; j <= tSpots; j++ {
			hash.Write([]byte(nodes[i] + ":" + strconv.Itoa(j)))
			hashBytes := hash.Sum(nil)
			n := &nodeHash{
				node: nodes[i],
				hash: uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24,
			}
			ticks = append(ticks, *n)
			hash.Reset()
		}
	}
	h.pool.Put(hash)
	ts := &tickArray{nodes: ticks, length: len(ticks)}
	ts.Sort()
	h.ticks.Store(ts)
}

// AddNode a new node to the hash ring.
// n: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
func (h *HashRing) AddNode(node string, spot int) {
	tmpTs := &tickArray{}
	if ts, ok := h.ticks.Load().(*tickArray); ok {
		for i := range ts.nodes {
			if ts.nodes[i].node == node {
				continue
			}
			tmpTs.nodes = append(tmpTs.nodes, ts.nodes[i])
		}
	}
	hash := h.hash()
	for i := 1; i <= h.spots*spot; i++ {
		hash.Write([]byte(node + ":" + strconv.Itoa(i)))
		hashBytes := hash.Sum(nil)
		n := &nodeHash{
			node: node,
			hash: uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24,
		}
		tmpTs.nodes = append(tmpTs.nodes, *n)
		hash.Reset()
	}
	h.pool.Put(hash)
	tmpTs.length = len(tmpTs.nodes)
	tmpTs.Sort()
	h.ticks.Store(tmpTs)
}

// DelNode delete a node from the hash ring.
// n: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
func (h *HashRing) DelNode(n string) {
	ts, ok := h.ticks.Load().(*tickArray)
	if !ok {
		panic("hash ring has not been initialized")
	}
	tmpTs := &tickArray{}
	for i := range ts.nodes {
		if ts.nodes[i].node == n {
			continue
		}
		tmpTs.nodes = append(tmpTs.nodes, ts.nodes[i])
	}
	tmpTs.length = len(tmpTs.nodes)
	tmpTs.Sort()
	h.ticks.Store(tmpTs)
}

// Hash returns result node.
func (h *HashRing) Hash(bs []byte) (string, bool) {
	ts, ok := h.ticks.Load().(*tickArray)
	if !ok || ts.length == 0 {
		return "", false
	}
	hash := h.hash()
	hash.Write(bs)
	hashBytes := hash.Sum(nil)
	hash.Reset()
	h.pool.Put(hash)
	v := uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24
	i := sort.Search(ts.length, func(i int) bool { return ts.nodes[i].hash >= v })
	if i == ts.length {
		i = 0
	}
	return ts.nodes[i].node, true
}

func (h *HashRing) hash() hash.Hash {
	return h.pool.Get().(hash.Hash)
}
