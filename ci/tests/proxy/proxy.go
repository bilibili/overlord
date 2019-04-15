package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ================== tools functions

func _createTCPProxy(t *testing.T, dist, origin int64) (cancel context.CancelFunc) {
	ctx := context.Background()
	var sub context.Context
	sub, cancel = context.WithCancel(ctx)
	listen, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", origin))
	if !assert.NoError(t, err) {
		return
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", dist))
	if !assert.NoError(t, err) {
		return
	}
	// defer conn.Close()

	go func() {
		for {
			select {
			case <-sub.Done():
				return
			default:
			}

			sock, err := listen.Accept()
			assert.NoError(t, err)

			forward := func(rd io.Reader, wr io.Writer) {
				for {
					_, err := io.Copy(wr, rd)
					if !assert.NoError(t, err) {
						return
					}
				}
			}

			go forward(sock, conn)
			go forward(conn, sock)
		}
	}()
	return
}

func _execute(t *testing.T) (bs []byte) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:21221", time.Second)
	if err != nil {
		t.Errorf("dial fail: %s", err)
		return
	}

	br := bufio.NewReader(conn)
	cmd := []byte("SET a_11 0 0 1\r\n1\r\n")
	conn.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err = conn.Write(cmd); err != nil {
		t.Errorf("conn write cmd:%s error:%v", cmd, err)
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if bs, err = br.ReadBytes('\n'); err != nil {
		t.Errorf("conn read cmd:%s error:%s resp:xxx%sxxx", cmd, err, bs)
		return
	}

	return
}
