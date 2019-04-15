package hashkit

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"overlord/pkg/log"
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
	nodes []string
	spots []int
	ticks atomic.Value
	lock  sync.Mutex
	hash  func([]byte) uint
}

// Ketama new a hash ring with ketama consistency.
// Default hash: fnv1a64
func Ketama() (h *HashRing) {
	h = new(HashRing)
	h.hash = hashFnv1a64
	return
}

// newRingWithHash new a hash ring with a hash func.
func newRingWithHash(hash func([]byte) uint) (h *HashRing) {
	h = Ketama()
	h.hash = hash
	return
}

// Init init ring.
func (h *HashRing) Init(nodes []string, spots []int) {
	h.lock.Lock()
	h.init(nodes, spots)
	h.lock.Unlock()
}

// Init init hash ring with nodes.
func (h *HashRing) init(nodes []string, spots []int) {
	if len(nodes) != len(spots) {
		panic("nodes length not equal spots length")
	}
	h.nodes = nodes
	h.spots = spots
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
}

func (h *HashRing) ketamaHash(key string, kl, alignment int) (v uint) {
	hs := md5.New()
	_, _ = hs.Write([]byte(key))
	bs := hs.Sum(nil)
	hs.Reset()
	v = (uint(bs[3+alignment*4]&0xFF) << 24) | (uint(bs[2+alignment*4]&0xFF) << 16) | (uint(bs[1+alignment*4]&0xFF) << 8) | (uint(bs[0+alignment*4] & 0xFF))
	return v
}

// AddNode a new node to the hash ring.
// n: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
func (h *HashRing) AddNode(node string, spot int) {
	var (
		tmpNode []string
		tmpSpot []int
		exitst  bool
	)
	h.lock.Lock()
	defer h.lock.Unlock()
	for i, nd := range h.nodes {
		tmpNode = append(tmpNode, nd)
		if nd == node {
			exitst = true
			tmpSpot = append(tmpSpot, spot)
			log.Infof("add exist node %s update spot from %d to %d", nd, h.spots[i], spot)
		} else {
			tmpSpot = append(tmpSpot, h.spots[i])
		}
	}
	if !exitst {
		tmpNode = append(tmpNode, node)
		tmpSpot = append(tmpSpot, spot)
		log.Infof("add node %s spot %d", node, spot)
	}
	h.init(tmpNode, tmpSpot)
}

// DelNode delete a node from the hash ring.
// n: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
func (h *HashRing) DelNode(n string) {
	var (
		tmpNode []string
		tmpSpot []int
		del     bool
	)
	h.lock.Lock()
	defer h.lock.Unlock()
	for i, nd := range h.nodes {
		if nd != n {
			tmpNode = append(tmpNode, nd)
			tmpSpot = append(tmpSpot, h.spots[i])
		} else {
			del = true
			log.Info("ketama del node ", n)
		}
	}
	if del {
		h.init(tmpNode, tmpSpot)
	}
}

// GetNode returns result node by given key.
func (h *HashRing) GetNode(key []byte) (string, bool) {
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
