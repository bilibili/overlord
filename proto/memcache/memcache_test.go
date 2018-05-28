package memcache_test

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/felixhao/overlord/lib/pool"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proto/memcache"
	"github.com/pkg/errors"
)

var (
	cmds = [][]byte{
		[]byte("SET a_11   0 0 1\r\n1\r\n"),
		[]byte("set a_22 0 123456 4\r\nhalo\r\n"),
		[]byte("set a_33 1 123456 3\r\ncao\r\n"),
		[]byte("cas a_11 0 0 3 39\r\ncao\r\n"),
		[]byte("  get   \r\n"),
		[]byte("get a_11 a_22 a_33\r\n"),
		[]byte("gets    a_22\r\n"),
		[]byte("gets a_11 a_22 a_33\r\n"),
		[]byte("delete a_11\r\n"),
		[]byte("incr a_11 1\r\n"),
		[]byte("decr a_11 1\r\n"),
		[]byte("touch a_11 123456\r\n"),
		//	[]byte("gat 123456 a_11\r\n"),
		//	[]byte("gats 123456 a_11\r\n"),
		//	[]byte("noexist a_11\r\n"),
	}
)

func mockProxyServer(t *testing.T, wg *sync.WaitGroup) {
	l, err := net.Listen("tcp", "127.0.0.1:11210")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		conn, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		handlerConn(t, wg, conn)
	}()
}

func handlerConn(t *testing.T, wg *sync.WaitGroup, conn net.Conn) {
	p := newPool(t) // NOTE: real memcache server
	d := memcache.NewDecoder(conn)
	e := memcache.NewEncoder(conn)
	for {
		req, err := d.Decode()
		wg.Done()
		if err != nil {
			if errors.Cause(err) == memcache.ErrError {
				t.Logf("%+v", err)
			} else {
				t.Errorf("%+v", err)
			}
			req := &proto.Request{Type: proto.CacheTypeMemcache}
			req.Process()
			req.DoneWithError(err)
			e.Encode(req.Resp)
			continue
		}
		req.Process()
		r := req.Proto().(*memcache.MCRequest)
		t.Logf("mcreq: %v", r)

		h, ok := p.Get().(proto.Handler)
		if !ok {
			t.Fatal("pool get conn is not proto Handler")
			continue
		}
		resp, err := h.Handle(req)
		p.Put(h.(pool.Conn), false)
		if err != nil {
			t.Errorf("%+v", err)
			req.DoneWithError(err)
			e.Encode(req.Resp)
			continue
		}
		req.Done(resp)
		if err = e.Encode(req.Resp); err != nil {
			t.Errorf("%+v", err)
		}
	}
}

func newPool(t *testing.T) *pool.Pool {
	dto := time.Duration(1000) * time.Millisecond
	rto := time.Duration(1000) * time.Millisecond
	wto := time.Duration(1000) * time.Millisecond
	dial := pool.PoolDial(memcache.Dial("", "192.168.99.100:32769", dto, rto, wto))
	act := pool.PoolActive(2)
	idle := pool.PoolIdle(1)
	idleTo := pool.PoolIdleTimeout(time.Duration(10) * time.Second)
	return pool.NewPool(dial, act, idle, idleTo)
}

func TestMemcache(t *testing.T) {
	wg := &sync.WaitGroup{}
	// start server
	mockProxyServer(t, wg)
	time.Sleep(time.Second)
	// dial
	conn, err := net.DialTimeout("tcp", "127.0.0.1:11210", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	for _, cmd := range cmds {
		wg.Add(1)
		conn.Write(cmd)
		// read
		bs, err := br.ReadSlice('\n')
		if err != nil {
			t.Errorf("cmd:%s, read error:%v", cmd, err)
			continue
		}
		if !bytes.Equal(bs, []byte("END\r\n")) && (bytes.HasPrefix(cmd, []byte("get")) || bytes.HasPrefix(cmd, []byte("gat"))) {
			var bs2 []byte
			for !bytes.Equal(bs2, []byte("END\r\n")) {
				if bs2, err = br.ReadSlice('\n'); err != nil {
					t.Errorf("cmd:%s, read error:%v", cmd, err)
					break
				}
				bs = append(bs, bs2...)
			}
		}
		// t.Logf("cmd:%s, read bytes(%s)", cmd, bs)
	}
	wg.Wait()
	t.Log("all commands handle success")
}
