package hashkit

import (
	"testing"
)

func BenchmarkNewFnva64(b *testing.B) {
	fv := NewFnv1a64()
	keys := [][]byte{
		[]byte("asdfghjkl"),
		[]byte("zxcvbnmlkjhgfds"),
		[]byte("qwertyuiop"),
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, k := range keys {
				fv.fnv1a64(k)
			}
		}
	})

}
