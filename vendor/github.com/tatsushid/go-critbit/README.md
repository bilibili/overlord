Crit-bit tree library written in Go
===================================

[![GoDoc](https://godoc.org/github.com/tatsushid/go-critbit?status.svg)][godoc]
[![Build Status](https://travis-ci.org/tatsushid/go-critbit.svg?branch=master)](https://travis-ci.org/tatsushid/go-critbit)
[![Go Report Card](https://goreportcard.com/badge/github.com/tatsushid/go-critbit)](https://goreportcard.com/report/github.com/tatsushid/go-critbit)

This is a [Crit-bit tree](http://cr.yp.to/critbit.html) implementation written in Go by referring [the C implementation and document](https://github.com/agl/critbit).

## Installation

Install and update by `go get -u github.com/tatsushid/go-critbit`.

## Usage

```go
package main

import (
	"github.com/tatsushid/go-critbit"
)

func main() {
	// create a tree
	tr := critbit.New()

	tr.Insert([]byte("foo"), 1)
	tr.Insert([]byte("bar"), 2)
	tr.Insert([]byte("foobar"), 3)

	// search exact key
	v, ok := tr.Get([]byte("bar"))
	if !ok {
		panic("should be ok")
	}
	if v.(int) != 2 {
		panic("should be 2")
	}

	// find the longest prefix of a given key
	k, _, _ := tr.LongestPrefix([]byte("foozip"))
	if string(k) != "foo" {
		panic("should be foo")
	}

	var sum int
	// walk the tree with a callback function
	tr.Walk(func(k []byte, v interface{}) bool {
		sum += v.(int)
		return false
	})
	if sum != 6 {
		panic("should be 6 = 1 + 2 + 3")
	}
}
```

Please see [GoDoc][godoc] for more APIs and details.

This also has Graphviz exporter sample. It is implemented as a part of tests.
To use it, please compile a test command by

```shellsession
go test -c
```

and run the generated command like

```shellsession
./critbit.test -random 10 -printdot > critbit.dot
```

You can see Graphviz image by opening the created file.

The command has following options

```
-add key
	add key to critbit tree. this can be used multiple times
-del key
	delete key from critbit tree. this can be used multiple times
-printdot
	print graphviz dot of critbit tree and exit
-random times
	insert keys chosen at random up to specified times
```

## Notes
- [go-radix](https://github.com/armon/go-radix) is widly used tree library and its API is well organized I think. I wrote this library to have a similar API to that and used some test data patterns to make sure it returns same results as that.
- To compare performance, I took the performance test data from [An Adaptive Radix Tree Implementation in Go](https://github.com/plar/go-adaptive-radix-tree). If you are interested in it, you can see it by running
  
  ```shellsession
  go test -bench . -benchmem
  ```

  in both cloned repositories. In my environment, this library is a bit slower but a little more memory efficient than that.
- This can handle a byte sequence with null bytes in it as same as [critbitgo](https://github.com/k-sone/critbitgo), the other Crit-bit library written in Go.

Thanks for all these libraries!

## License
This program is under MIT license. Please see the [LICENSE][license] file for details.

[godoc]: http://godoc.org/github.com/tatsushid/go-critbit
[license]: https://github.com/tatsushid/go-critbit/blob/master/LICENSE
