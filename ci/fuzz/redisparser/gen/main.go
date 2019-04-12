package main

import (
	"github.com/dvyukov/go-fuzz/gen"
)

var zdata = []string{
	"*3\r\n$4\r\nMGET\r\n$4\r\nbaka\r\n$4\r\nkaba\r\n*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\nb\r\n$3\r\neee\r\n$5\r\n12345\r\n*3\r\n$4\r\nMGET\r\n$4\r\nenen\r\n$4\r\nnime\r\n*2\r\n$3\r\nGET\r\n$5\r\nabcde\r\n*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n",
	"*3\r\n$5\r\nSETNX\r\n$1\r\na\r\n$10\r\nabcdeabcde\r\n",
	"set a b\r\n",
	"get a\r\n",
}

func main() {
	for _, data := range zdata {
		gen.Emit([]byte(data), nil, true)
	}
}
