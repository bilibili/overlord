package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const psyncCmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"

func read(rd io.Reader, ping chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	buf := make([]byte, 1024)
	for {
		size, err := rd.Read(buf)
		if err != nil {
			panic(err)
		}
		if bytes.Contains(buf[:size], []byte("PING")) {
			ping <- struct{}{}
		}
		fmt.Println("get ", strconv.Quote(string(buf[:size])))
	}
}

func write(rd io.Writer, ping chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ping:
			// _, err := rd.Write([]byte("+PONG\r\n"))
			// if err != nil {
			// 	panic(err)
			// }
		case <- ticker.C:
			_, err := rd.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$4\r\n1640\r\n"))
			if err != nil {
				panic(err)
			}
		}
	}
}

func main() {

	sock, err := net.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}

	_, err = sock.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		panic(err)
	}
	pongbuf := make([]byte, 8)
	_, err = sock.Read(pongbuf)
	if err != nil {
		panic(err)
	}

	_, err = sock.Write([]byte(psyncCmd))
	if err != nil {
		panic(err)
	}


	c := make(chan struct{}, 10)
	var wg sync.WaitGroup
	wg.Add(2)

	go read(sock, c, &wg)
	go write(sock, c, &wg)

	wg.Wait()

}
