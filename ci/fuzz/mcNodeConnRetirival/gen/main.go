package main

import (
	"github.com/dvyukov/go-fuzz/gen"
)

var zdata = []string{
	"VALUE a 0 2\r\nab\r\n",
}

func main() {
	for _, data := range zdata {
		gen.Emit([]byte(data), nil, true)
	}

	bodySize := 1048576
	head := []byte("VALUE a 1 0 1048576\r\n")
	tail := "\r\nEND\r\n"

	data := []byte{}
	for i := 0; i < 3; i++ {
		data = append(data, head...)
		data = append(data, make([]byte, bodySize)...)
		data = append(data, tail...)
	}

	gen.Emit(data, nil, true)
}
