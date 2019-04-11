package main

import (
	"github.com/dvyukov/go-fuzz/gen"
)

var zdata = []string{
	`SET A 1 1 1\r\n1\r\n`,
	"GET A",
}

func main() {
	for _, data := range zdata {
		gen.Emit([]byte(data), nil, true)
	}
}
