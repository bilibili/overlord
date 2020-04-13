package slowlog

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync/atomic"
	"testing"
)

//in slowlog cursorInt32 init val is -1
var cursorInt32 int32

//in slowlog cursorUint32 init val is 0
var cursorUint32 uint32

func TestRecordWithCursorUint32(t *testing.T) {
	//now cursorUint32 is uint32's max val
	cursorUint32 = ^uint32(0)
	idxOk := true
	for i := 1; i < 10; i++ {
		if atomic.CompareAndSwapUint32(&cursorUint32, cursorUint32, cursorUint32+1) {
			idx := (cursorUint32 - 1) % slowlogMaxCount
			fmt.Fprintf(os.Stdout, "%d mod 1024 = %d\n", cursorUint32-1, idx)
			if idx < 0 {
				idxOk = false
				break
			}
		}
	}
	assert.True(t, idxOk)
}

func TestRecordWithCursorInt32(t *testing.T) {
	//now cursorInt32 is int32's max val
	cursorInt32 = int32(^uint32(0) >> 1)
	idxOk := true
	for i := 1; i < 10; i++ {
		if atomic.CompareAndSwapInt32(&cursorInt32, cursorInt32, cursorInt32+1) {
			idx := cursorInt32 % slowlogMaxCount
			if idx < 0 {
				fmt.Fprintf(os.Stdout, "%d mod 1024 = %d\n", cursorInt32, idx)
				idxOk = false
				break
			}
		}
	}
	assert.False(t, idxOk)
}
