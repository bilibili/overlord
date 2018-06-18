package memcache_test

import (
	"net"
	"sync"
	"testing"
	"time"

	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/felixhao/overlord/proto/memcache"
	"github.com/pkg/errors"
)

var (
	cmds = [][]byte{
		[]byte("SET a_11   0 0 1\r\n1\r\n"),
		[]byte("set a_22 0 123456 4\r\nhalo\r\n"),
		[]byte("set a_33 1 123456 3\r\ncao\r\n"),
		[]byte("cas a_11 0 0 3 39\r\ncao\r\n"),
		[]byte("  get  a \r\n"),
		[]byte("get a_11 a_22   a_33\r\n"),
		[]byte("gets    a_22\r\n"),
		[]byte("gets a_11 a_22 a_33\r\n"),
		[]byte("delete a_11\r\n"),
		[]byte("incr a_11   1\r\n"),
		[]byte("decr a_11 1\r\n"),
		[]byte("touch a_11 123456\r\n"),
		[]byte("gat 123456 a_11\r\n"),
		[]byte("gat 123456 a_11 a_22  a_33\r\n"),
		[]byte("gats 123456 a_11\r\n"),
		[]byte("noexist a_11\r\n"),
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
		c := libnet.NewConn(conn, time.Duration(1000)*time.Millisecond, time.Duration(1000)*time.Millisecond)
		handlerConn(t, wg, c)
	}()
}

func handlerConn(t *testing.T, wg *sync.WaitGroup, c *libnet.Conn) {
	pc := memcache.NewProxyConn(c)
	for {
		m, err := pc.Decode()
		// println(m.Buffer().String())
		// println(m.Cmd())
		// println(m.Key())
		wg.Done()
		if err != nil {
			println("wocao!err:", err)
			if errors.Cause(err) == memcache.ErrError {
				t.Logf("%+v", err)
			} else {
				t.Errorf("%+v", err)
			}
			// m := &proto.Message{Type: proto.CacheTypeMemcache}
			// m.Add()
			// m.DoneWithError(err)
			// pc.Encode(req.Resp)
			continue
		}
		// req.Process()
		// r := req.Proto().(*memcache.MCMsg)
		t.Logf("mcreq: %v", m)

		// resp, err := h.Handle(req)
		// p.Put(h.(pool.Conn), false)
		// if err != nil {
		// 	t.Errorf("%+v", err)
		// 	req.DoneWithError(err)
		// 	e.Encode(req.Resp)
		// 	continue
		// }
		// req.Done(resp)
		// if err = e.Encode(req.Resp); err != nil {
		// 	t.Errorf("%+v", err)
		// }
	}
}

func newConn(t *testing.T) *libnet.Conn {
	dto := time.Duration(1000) * time.Millisecond
	rto := time.Duration(1000) * time.Millisecond
	wto := time.Duration(1000) * time.Millisecond
	c, _ := libnet.DialWithTimeout("127.0.0.1:11210", dto, rto, wto)
	return c
}

func TestMemcache(t *testing.T) {
	wg := &sync.WaitGroup{}
	// start server
	mockProxyServer(t, wg)
	time.Sleep(time.Second)
	// dial
	conn := newConn(t)
	// br := bufio.NewReader(conn)
	for _, cmd := range cmds {
		wg.Add(1)
		conn.Write(cmd)
		// read
		// bs, err := br.ReadSlice('\n')
		// if err != nil {
		// 	t.Errorf("cmd:%s, read error:%v", cmd, err)
		// 	continue
		// }
		// if !bytes.Equal(bs, []byte("END\r\n")) && (bytes.HasPrefix(cmd, []byte("get")) || bytes.HasPrefix(cmd, []byte("gat"))) {
		// 	var bs2 []byte
		// 	for !bytes.Equal(bs2, []byte("END\r\n")) {
		// 		if bs2, err = br.ReadSlice('\n'); err != nil {
		// 			t.Errorf("cmd:%s, read error:%v", cmd, err)
		// 			break
		// 		}
		// 		bs = append(bs, bs2...)
		// 	}
		// }
		// t.Logf("cmd:%s, read bytes(%s)", cmd, bs)
	}
	wg.Wait()
	t.Log("all commands handle success")
}
