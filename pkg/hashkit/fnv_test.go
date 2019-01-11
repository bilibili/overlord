package hashkit

import (
	"testing"
)

func TestFnv(t *testing.T) {
	f := New64a()
	bs := []byte("abc")
	fbs := f.Sum(bs)
	t.Logf("sum:%x", fbs)

	f.Reset()
	r := f.Sum64()
	if r != offset64 {
		t.Errorf("not equal:%d", r)
	}
	if f.BlockSize() != 1 {
		t.Errorf("blocksize:%d", f.BlockSize())
	}
	if f.Size() != 8 {
		t.Errorf("size:%d", f.Size())
	}
}
