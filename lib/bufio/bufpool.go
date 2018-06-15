package bufio

import (
	"sync"
)

const (
	maxBufferSize     = 32 * 1024 * 1024 // 32MB
	defaultBufferSize = 64
	growFactor        = 2
)

var sizeList []int

var bufPoolList []*sync.Pool

func init() {
	sizeList = make([]int, 0)
	threshold := defaultBufferSize
	for threshold <= maxBufferSize {
		sizeList = append(sizeList, threshold)
		threshold *= growFactor
	}

	bufPoolList = make([]*sync.Pool, len(sizeList))
	for idx := range bufPoolList {
		bufPoolList[idx] = &sync.Pool{
			New: func() interface{} {
				b := buffer(make([]byte, sizeList[idx]))
				return &b
			},
		}
	}

}

func searchExceptRange(n int) (low, top int) {
	low = 0
	if n < sizeList[low] {
		top = 0
		low = -1
		return
	}
	top = len(sizeList) - 1

	if n > sizeList[top] {
		top = -1
		low = -1
		return
	}

	for {
		middle := (low + top) / 2
		if sizeList[middle] > n {
			top = middle
			if low == top {
				top = low + 1
				return
			}

		} else if sizeList[middle] < n {
			low = middle
			if low == top {
				low = top - 1
				return
			}
		} else {
			low = middle
			top = middle
			return
		}
	}
}

type buffer []byte

// Get the data buffer
func Get(size int) []byte {
	_, top := searchExceptRange(size)
	b := bufPoolList[top].Get().(*buffer)
	return []byte(*b)
}

// Put the data into global pool
func Put(data []byte) {
	low, _ := searchExceptRange(len(data))
	b := buffer(data[:sizeList[low]])
	bufPoolList[low].Put(&b)
}
