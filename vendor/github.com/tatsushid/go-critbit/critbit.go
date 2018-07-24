// Package critbit implements Crit-Bit tree for byte sequences.
//
// Crit-Bit tree [1] is fast, memory efficient and a variant of PATRICIA trie.
// This implementation can be used for byte sequences if it includes a null
// byte or not. This is based on [2] and extends it to support a null byte in a
// byte sequence.
//
//  [1]: http://cr.yp.to/critbit.html (definition)
//  [2]: https://github.com/agl/critbit (C implementation and document)
package critbit

import "bytes"

type nodeType int

const (
	internal nodeType = iota
	external
)

type node interface {
	kind() nodeType
}

type iNode struct {
	children [2]node
	pos      int
	other    uint8
}

func (n *iNode) kind() nodeType { return internal }

type eNode struct {
	key   []byte
	value interface{}
}

func (n *eNode) kind() nodeType { return external }

// Tree represents a critbit tree.
type Tree struct {
	root node
	size int
}

// New returns an empty tree.
func New() *Tree {
	return &Tree{}
}

// Len returns a number of elements in the tree.
func (t *Tree) Len() int {
	return t.size
}

func (t *Tree) direction(k []byte, pos int, other uint8) int {
	var c uint8
	if pos < len(k) {
		c = k[pos]
	} else if other == 0xff {
		return 0
	}
	return (1 + int(other|c)) >> 8
}

func (t *Tree) lookup(k []byte) (*eNode, *iNode) {
	if t.root == nil {
		return nil, nil
	}

	var top *iNode
	p := t.root
	for {
		switch n := p.(type) {
		case *eNode:
			return n, top
		case *iNode:
			if top == nil || n.pos < len(k) {
				top = n
			}
			p = n.children[t.direction(k, n.pos, n.other)]
		}
	}
}

// Get searches a given key from the tree. If the key exists in the tree, it
// returns its value and true. If not, it returns nil and false.
func (t *Tree) Get(k []byte) (interface{}, bool) {
	n, _ := t.lookup(k)
	if n != nil && bytes.Equal(k, n.key) {
		return n.value, true
	}
	return nil, false
}

func (t *Tree) findFirstDiffByte(k []byte, n *eNode) (pos int, other uint8, match bool) {
	var byt, b byte
	for pos = 0; pos < len(k); pos++ {
		b = k[pos]
		byt = 0
		if pos < len(n.key) {
			byt = n.key[pos]
		}
		if byt != b {
			return pos, byt ^ b, false
		}
	}
	if pos < len(n.key) {
		return pos, n.key[pos], false
	} else if pos == len(n.key) {
		return 0, 0, true
	}
	return pos - 1, 0, false
}

func (t *Tree) findInsertPos(k []byte, pos int, other uint8) (*node, node) {
	p := &t.root
	for {
		switch n := (*p).(type) {
		case *eNode:
			return p, n
		case *iNode:
			if n.pos > pos {
				return p, n
			}
			if n.pos == pos && n.other > other {
				return p, n
			}
			p = &n.children[t.direction(k, n.pos, n.other)]
		}
	}
}

// Insert adds or updates a given key to the tree and returns its previous
// value and if anything was set or not. If there is the key in the tree, it
// adds the key and the value to the tree and returns nil and true when it
// succeeded while if not, it updates the key's value and returns its previous
// value and true when it succeeded.
func (t *Tree) Insert(k []byte, v interface{}) (interface{}, bool) {
	key := append([]byte{}, k...)

	n, _ := t.lookup(k)
	if n == nil { // only happens when t.root is nil
		t.root = &eNode{key: key, value: v}
		t.size++
		return nil, true
	}

	pos, other, match := t.findFirstDiffByte(k, n)
	if match {
		orig := n.value
		n.value = v
		return orig, true
	}

	other |= other >> 1
	other |= other >> 2
	other |= other >> 4
	other = ^(other &^ (other >> 1))
	di := t.direction(n.key, pos, other)

	newn := &iNode{pos: pos, other: other}
	newn.children[1-di] = &eNode{key: key, value: v}

	p, child := t.findInsertPos(k, pos, other)
	newn.children[di] = child
	*p = newn

	t.size++
	return nil, true
}

func (t *Tree) findDeletePos(k []byte) (*node, *eNode, int) {
	if t.root == nil {
		return nil, nil, 0
	}

	var di int
	var q *node
	p := &t.root
	for {
		switch n := (*p).(type) {
		case *eNode:
			return q, n, di
		case *iNode:
			di = t.direction(k, n.pos, n.other)
			q = p
			p = &n.children[di]
		}
	}
}

// Delete removes a given key and its value from the tree. If it succeeded, it
// returns the key's previous value and true while if not, it returns nil and
// false. On an empty tree, it always fails.
func (t *Tree) Delete(k []byte) (interface{}, bool) {
	q, n, di := t.findDeletePos(k)
	if n == nil || !bytes.Equal(k, n.key) {
		return nil, false
	}
	t.size--
	if q == nil {
		t.root = nil
		return n.value, true
	}
	tmp := (*q).(*iNode)
	*q = tmp.children[1-di]
	return n.value, true
}

// Clear removes all elements in the tree. If it removes something, it returns
// true while the tree is empty and there is nothing to remove, it returns
// false.
func (t *Tree) Clear() bool {
	if t.root != nil {
		t.root = nil
		t.size = 0
		return true
	}
	return false
}

// Minimum searches a key from the tree in lexicographic order and returns the
// first one and its value. If it found such a key, it also returns true as the
// bool value while if not, it returns false as it.
func (t *Tree) Minimum() ([]byte, interface{}, bool) {
	if t.root == nil {
		return nil, nil, false
	}

	p := t.root
	for {
		switch n := p.(type) {
		case *eNode:
			return n.key, n.value, true
		case *iNode:
			p = n.children[0]
		}
	}
}

// Maximum searches a key from the tree in lexicographic order and returns the
// last one and its value. If it found such a key, it also returns true as the
// bool value while if not, it returns false as it.
func (t *Tree) Maximum() ([]byte, interface{}, bool) {
	if t.root == nil {
		return nil, nil, false
	}

	p := t.root
	for {
		switch n := p.(type) {
		case *eNode:
			return n.key, n.value, true
		case *iNode:
			p = n.children[1]
		}
	}
}

func (t *Tree) longestPrefix(p node, prefix []byte) ([]byte, interface{}, bool) {
	if p == nil {
		return nil, nil, false
	}
	var di int
	var c uint8
	switch n := p.(type) {
	case *eNode:
		if bytes.HasPrefix(prefix, n.key) {
			return n.key, n.value, true
		}
	case *iNode:
		c = 0
		if n.pos < len(prefix) {
			c = prefix[n.pos]
		}
		di = (1 + int(n.other|c)) >> 8

		if k, v, ok := t.longestPrefix(n.children[di], prefix); ok {
			return k, v, ok
		} else if di == 1 {
			return t.longestPrefix(n.children[0], prefix)
		}
	}
	return nil, nil, false
}

// LongestPrefix searches the longest key which is included in a given key and
// returns the found key and its value. For example, if there are "f", "fo",
// "foobar" in the tree and "foo" is given, it returns "fo". If it found such a
// key, it returns true as the bool value while if not, it returns false as it.
func (t *Tree) LongestPrefix(prefix []byte) ([]byte, interface{}, bool) {
	return t.longestPrefix(t.root, prefix)
}

// WalkFn is used at walking a tree. It receives a key and its value of each
// elements which a walk function gives. If it returns true, a walk function
// should be terminated at there.
type WalkFn func(k []byte, v interface{}) bool

func (t *Tree) walk(p node, fn WalkFn) bool {
	if p == nil {
		return false
	}
	switch n := p.(type) {
	case *eNode:
		return fn(n.key, n.value)
	case *iNode:
		for i := 0; i < 2; i++ {
			if t.walk(n.children[i], fn) {
				return true
			}
		}
	}
	return false
}

// Walk walks whole the tree and call a given function with each element's key
// and value. If the function returns true, the walk is terminated at there.
func (t *Tree) Walk(fn WalkFn) {
	t.walk(t.root, fn)
}

// WalkPrefix walks the tree under a given prefix and call a given function
// with each element's key and value. For example, the tree has "f", "fo",
// "foob", "foobar" and "foo" is given, it visits "foob" and "foobar" elements.
// If the function returns true, the walk is terminated at there.
func (t *Tree) WalkPrefix(prefix []byte, fn WalkFn) {
	n, top := t.lookup(prefix)
	if n == nil || !bytes.HasPrefix(n.key, prefix) {
		return
	}
	wrapper := func(k []byte, v interface{}) bool {
		if bytes.HasPrefix(k, prefix) {
			return fn(k, v)
		}
		return false
	}
	t.walk(top, wrapper)
}

func (t *Tree) walkPath(p node, path []byte, fn WalkFn) bool {
	if p == nil {
		return false
	}
	var di int
	switch n := p.(type) {
	case *eNode:
		if bytes.HasPrefix(path, n.key) {
			return fn(n.key, n.value)
		}
	case *iNode:
		di = t.direction(path, n.pos, n.other)
		if di == 1 {
			if t.walkPath(n.children[0], path, fn) {
				return true
			}
		}
		return t.walkPath(n.children[di], path, fn)
	}
	return false
}

// WalkPath walks the tree from the root up to a given key and call a given
// function with each element's key and value. For example, the tree has "f",
// "fo", "foob", "foobar" and "foo" is given, it visits "f" and "fo" elements.
// If the function returns true, the walk is terminated at there.
func (t *Tree) WalkPath(path []byte, fn WalkFn) {
	t.walkPath(t.root, path, fn)
}
