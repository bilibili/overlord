package hashkit

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync/atomic"
)

const (
	_pointsPerServer = 160
	_maxHostLen      = 64
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
	hash  func(string) uint
}

// NewRing new a hash ring.
// Default hash: sha1
func NewRing(n int) (h *HashRing) {
	h = new(HashRing)
	h.spots = n
	h.hash = NewFnv1a32().fnv1a32
	return
}

// NewRingWithHash new a hash ring with a hash func.
func NewRingWithHash(n int, hash func(string) uint) (h *HashRing) {
	h = NewRing(n)
	h.hash = hash
	return
}

// Init init hash ring with nodes.
func (h *HashRing) Init(nodes []string, spots []int) {
	if len(nodes) != len(spots) {
		panic("nodes length not equal spots length")
	}
	var (
		ticks          []nodeHash
		svrn           = len(nodes)
		totalw         int
		pointerCnt     int
		pointerPerSvr  int
		pointerPerHash = 4
	)
	for _, sp := range spots {
		totalw += sp
	}
	for idx, node := range nodes {
		pct := float64(spots[idx]) / float64(totalw)
		pointerPerSvr = int((pct*_pointsPerServer/4*float64(svrn) + 0.0000000001) * 4)
		for pidx := 1; pidx <= pointerPerSvr/pointerPerHash; pidx++ {
			host := fmt.Sprintf("%s-%d", node, pidx-1)
			if len(host) > _maxHostLen {
				host = host[:_maxHostLen]
			}
			for x := 0; x < pointerPerHash; x++ {
				value := h.ketamaHash(host, len(host), x)
				n := &nodeHash{
					node: node,
					hash: value,
				}
				ticks = append(ticks, *n)
			}
		}
		pointerCnt += pointerPerSvr
	}
	ts := &tickArray{nodes: ticks, length: len(ticks)}
	ts.Sort()
	h.ticks.Store(ts)
	// var ticks []nodeHash
	// hash := h.hash()
	// for i := range nodes {
	// 	tSpots := h.spots * spots[i]
	// 	for j := 1; j <= tSpots; j++ {
	// 		hash.Write([]byte(nodes[i] + ":" + strconv.Itoa(j)))
	// 		hashBytes := hash.Sum(nil)
	// 		n := &nodeHash{
	// 			node: nodes[i],
	// 			hash: uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24,
	// 		}
	// 		ticks = append(ticks, *n)
	// 		hash.Reset()
	// 	}
	// }
	// h.pool.Put(hash)
	// ts := &tickArray{nodes: ticks, length: len(ticks)}
	// ts.Sort()
	// h.ticks.Store(ts)
}

func (h *HashRing) ketamaHash(key string, kl, alignment int) (v uint) {
	hs := md5.New()
	hs.Write([]byte(key))
	bs := hs.Sum(nil)
	hs.Reset()
	v = (uint(bs[3+alignment*4]&0xFF) << 24) | (uint(bs[2+alignment*4]&0xFF) << 16) | (uint(bs[1+alignment*4]&0xFF) << 8) | (uint(bs[0+alignment*4] & 0xFF))
	return v
}

// AddNode a new node to the hash ring.
// n: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
// func (h *HashRing) AddNode(node string, spot int) {
// 	tmpTs := &tickArray{}
// 	if ts, ok := h.ticks.Load().(*tickArray); ok {
// 		for i := range ts.nodes {
// 			if ts.nodes[i].node == node {
// 				continue
// 			}
// 			tmpTs.nodes = append(tmpTs.nodes, ts.nodes[i])
// 		}
// 	}
// 	hash := h.hash()
// 	for i := 1; i <= h.spots*spot; i++ {
// 		hash.Write([]byte(node + ":" + strconv.Itoa(i)))
// 		hashBytes := hash.Sum(nil)
// 		n := &nodeHash{
// 			node: node,
// 			hash: uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24,
// 		}
// 		tmpTs.nodes = append(tmpTs.nodes, *n)
// 		hash.Reset()
// 	}
// 	h.pool.Put(hash)
// 	tmpTs.length = len(tmpTs.nodes)
// 	tmpTs.Sort()
// 	h.ticks.Store(tmpTs)
// }

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
func (h *HashRing) Hash(key string) (string, bool) {
	ts, ok := h.ticks.Load().(*tickArray)
	if !ok || ts.length == 0 {
		return "", false
	}
	value := h.hash(key)
	i := sort.Search(ts.length, func(i int) bool { return ts.nodes[i].hash >= value })
	if i == ts.length {
		i = 0
	}
	return ts.nodes[i].node, true
}
