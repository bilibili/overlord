package proxy_test

import (
	"bufio"
	"bytes"
	"net"
	"testing"
	"time"

	libnet "github.com/felixhao/overlord/lib/net"
	"github.com/felixhao/overlord/proto"
	"github.com/felixhao/overlord/proxy"
	"github.com/stretchr/testify/assert"
)

var (
	ccs = []*proxy.ClusterConfig{
		&proxy.ClusterConfig{
			Name:             "test-cluster",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("memcache"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:21211",
			RedisAuth:        "",
			DialTimeout:      1000,
			ReadTimeout:      1000,
			NodeConnections:  10,
			WriteTimeout:     1000,
			// PoolActive:       50,
			// PoolIdle:         10,
			// PoolIdleTimeout:  100000,
			// PoolGetWait:      true,
			PingFailLimit: 3,
			PingAutoEject: false,
			Servers: []string{
				"127.0.0.1:11211:10",
				// "127.0.0.1:11212:10",
				// "127.0.0.1:11213:10",
			},
		},
	}

	cmds = [][]byte{
		[]byte("SET a_11 0 0 1\r\n1\r\n"),
		[]byte("get a_11\r\n"),
		[]byte("get a_11 a_22 a_33\r\n"),
		[]byte("set a_22 0 123456 4\r\nhalo\r\n"),
		[]byte("set a_33 1 123456 3\r\ncao\r\n"),
		[]byte("cas a_11 0 0 3 181\r\ncao\r\n"),
		[]byte("add a_44 0 0 3\r\naaa\r\n"),
		[]byte("replace a_44 0 0 3\r\nbbb\r\n"),
		[]byte("append a_44 0 0 3\r\nccc\r\n"),
		[]byte("prepend a_44 0 0 3\r\nddd\r\n"),
		[]byte("gets a_11\r\n"),
		[]byte("gets a_11 a_22 a_33\r\n"),
		[]byte("get a_44\r\n"),
		[]byte("delete a_11\r\n"),
		[]byte("incr a_11 1\r\n"),
		[]byte("decr a_11 1\r\n"),
		[]byte("touch a_11 123456\r\n"),
		[]byte("gat 123456 a_11\r\n"),
		[]byte("gat 123456 a_11 a_22 a_33\r\n"),
		[]byte("gats 123456 a_11\r\n"),
		[]byte("gats 123456 a_11 a_22 a_33\r\n"),
		[]byte("noexist a_11\r\n"),
	}
)

func init() {
	mockProxy()
	time.Sleep(1000 * time.Millisecond)
}

func mockProxy() {
	p, err := proxy.New(proxy.DefaultConfig())
	if err != nil {
		panic(err)
	}
	// serve
	go p.Serve(ccs)
}

func testCmd(t testing.TB, cmds ...[]byte) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:21211", time.Second)
	if err != nil {
		t.Errorf("net dial error:%v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	for _, cmd := range cmds {
		// t.Logf("\n\nexecute cmd %s", cmd)
		conn.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err = conn.Write(cmd); err != nil {
			t.Errorf("conn write cmd:%s error:%v", cmd, err)
		}
		conn.SetReadDeadline(time.Now().Add(time.Second))
		var bs []byte
		if bs, err = br.ReadBytes('\n'); err != nil {
			t.Errorf("conn read cmd:%s error:%s resp:xxx%sxxx", cmd, err, bs)
			continue
		}
		if bytes.HasPrefix(bs, []byte("ERROR")) || bytes.HasPrefix(bs, []byte("CLIENT_ERROR")) || bytes.HasPrefix(bs, []byte("SERVER_ERROR")) {
			t.Errorf("conn error:%s %s", bs, cmd)
			continue
		}
		if !bytes.Equal(bs, []byte("END\r\n")) && (bytes.HasPrefix(cmd, []byte("get")) || bytes.HasPrefix(cmd, []byte("gets")) || bytes.HasPrefix(cmd, []byte("gat")) || bytes.HasPrefix(cmd, []byte("gats"))) {
			var bs2 []byte
			for !bytes.Equal(bs2, []byte("END\r\n")) {
				conn.SetReadDeadline(time.Now().Add(time.Second))
				if bs2, err = br.ReadSlice('\n'); err != nil {
					t.Errorf("conn read cmd:%s error:%v", cmd, err)
					continue
				}
				bs = append(bs, bs2...)
			}
		}
		// t.Logf("read string:%s", bs)
	}
}

func TestProxyFull(t *testing.T) {
	for i := 0; i < 100; i++ {
		testCmd(t, cmds[0], cmds[1], cmds[2], cmds[10], cmds[11])
	}
}

func TestProxyWithAssert(t *testing.T) {
	ts := []struct {
		Name   string
		Cmd    string
		Line   int
		Except []string
	}{

		{Name: "SetLowerOneOk", Line: 1, Cmd: "set a_11 0 1024 1\r\n2\r\n", Except: []string{"STORED\r\n"}},
		{Name: "SetLowerTwoOk", Line: 1, Cmd: "set a_22 0 1024 4\r\nhalo\r\n", Except: []string{"STORED\r\n"}},
		{Name: "SetUpperOk", Line: 1, Cmd: "SET a_11 0 1024 1\r\n1\r\n", Except: []string{"STORED\r\n"}},

		{Name: "GetMultiMissOneOk", Line: 5, Cmd: "get a_11 a_22 a_33\r\n", Except: []string{"VALUE a_11 0 1\r\n1\r\n", "VALUE a_22 0 4\r\nhalo\r\n", "END\r\n"}},
		{Name: "GetsOneOk", Line: 3, Cmd: "gets a_11\r\n", Except: []string{"VALUE a_11 0 1 ", "\r\n1\r\nEND\r\n"}},
		{Name: "GetMultiCasMissOneOk", Line: 5, Cmd: "gets a_11 a_22 a_33\r\n", Except: []string{"VALUE a_11 0 1", "\r\n1\r\n", "VALUE a_22 0 4", "\r\nhalo\r\n", "END\r\n"}},
		{Name: "MultiCmdGetOk", Line: 6, Cmd: "gets a_11\r\ngets a_11\r\n", Except: []string{"VALUE a_11 0 1", "\r\n1\r\n", "END\r\n"}},
	}

	for i := 0; i < 100; i++ {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:21211", time.Second)
		if err != nil {
			t.Errorf("net dial error:%v", err)
		}
		nc := libnet.NewConn(conn, time.Second, time.Second)
		br := bufio.NewReader(nc)
		for _, tt := range ts {
			t.Run(tt.Name, func(t *testing.T) {
				size, err := conn.Write([]byte(tt.Cmd))
				assert.NoError(t, err)
				assert.Equal(t, len(tt.Cmd), size)

				buf := make([]byte, 0)
				for i := 0; i < tt.Line; i++ {
					data, err := br.ReadBytes('\n')
					assert.NoError(t, err)
					buf = append(buf, data...)
				}
				sb := string(buf)
				for _, except := range tt.Except {
					assert.Contains(t, sb, except, "CMD:%s", tt.Cmd)
				}
			})
		}
		nc.Close()
	}
}

func BenchmarkCmdSet(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			testCmd(b, cmds[0])
		}
	})
}

func BenchmarkCmdGet(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			testCmd(b, cmds[1])
		}
	})
}

func BenchmarkCmdMGet(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			testCmd(b, cmds[2])
		}
	})
}
