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
}
