package proxy_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	libnet "overlord/lib/net"
	"overlord/proto"
	"overlord/proxy"

	"github.com/stretchr/testify/assert"
)

var (
	ccs = []*proxy.ClusterConfig{
		&proxy.ClusterConfig{
			Name:             "mc-cluster",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("memcache"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:21211",
			RedisAuth:        "",
			DialTimeout:      100,
			ReadTimeout:      100,
			NodeConnections:  10,
			WriteTimeout:     1000,
			PingFailLimit:    3,
			PingAutoEject:    false,
			Servers: []string{
				"127.0.0.1:11211:10",
				// "127.0.0.1:11212:10",
				// "127.0.0.1:11213:10",
			},
		},
		&proxy.ClusterConfig{
			Name:             "mcbin-cluster",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("memcache_binary"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:21212",
			RedisAuth:        "",
			DialTimeout:      100,
			ReadTimeout:      100,
			NodeConnections:  10,
			WriteTimeout:     1000,
			PingFailLimit:    3,
			PingAutoEject:    false,
			Servers: []string{
				"127.0.0.1:11211:10",
				// "127.0.0.1:11212:10",
				// "127.0.0.1:11213:10",
			},
		},

		&proxy.ClusterConfig{
			Name:             "redis-hash-cluster",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("redis"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:21213",
			RedisAuth:        "",
			DialTimeout:      100,
			ReadTimeout:      100,
			NodeConnections:  10,
			WriteTimeout:     1000,
			PingFailLimit:    3,
			PingAutoEject:    false,
			Servers: []string{
				"127.0.0.1:6379:10",
			},
		},

		&proxy.ClusterConfig{
			Name:            "redis-cluster-cluster",
			HashTag:         "{}",
			CacheType:       proto.CacheType("redis_cluster"),
			ListenProto:     "tcp",
			ListenAddr:      "127.0.0.1:21214",
			RedisAuth:       "",
			DialTimeout:     100,
			ReadTimeout:     100,
			NodeConnections: 10,
			WriteTimeout:    1000,
			PingFailLimit:   3,
			PingAutoEject:   false,
			FetchInterval:   1000,
			Servers: []string{
				"127.0.0.1:7000",
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

	cmdBins = [][]byte{
		[]byte{
			0x80,       // magic
			0x01,       // cmd
			0x00, 0x03, // key len
			0x08,       // extra len
			0x00,       // data type
			0x00, 0x00, // vbucket
			0x00, 0x00, 0x00, 0x10, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // extra: flags, expiration
			0x41, 0x42, 0x43, // key: ABC
			0x41, 0x42, 0x43, 0x44, 0x45, // value: ABCDE
		},
		[]byte{
			0x80,       // magic
			0x0c,       // cmd
			0x00, 0x03, // key len
			0x00,       // extra len
			0x00,       // data type
			0x00, 0x00, // vbucket
			0x00, 0x00, 0x00, 0x03, // body len
			0x00, 0x00, 0x00, 0x00, // opaque
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
			0x41, 0x42, 0x43, // key: ABC
		},
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
	}
}

func testCmdBin(t testing.TB, cmds ...[]byte) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:21212", time.Second)
	if err != nil {
		t.Errorf("net dial error:%v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	for _, cmd := range cmds {
		conn.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := conn.Write(cmd); err != nil {
			t.Errorf("conn write cmd:%s error:%v", cmd, err)
		}
		conn.SetReadDeadline(time.Now().Add(time.Second))
		bs := make([]byte, 24)
		if n, err := br.Read(bs); err != nil || n != 24 {
			t.Errorf("conn read cmd:%x error:%s resp:%x", cmd[1], err, bs)
			continue
		}
		if bytes.Equal(bs[6:8], []byte{0x00, 0x01}) {
			// key not found
			continue
		}
		if !bytes.Equal(bs[6:8], []byte{0x00, 0x00}) {
			t.Errorf("conn error:%s %s", bs, cmd)
			continue
		}
		bl := binary.BigEndian.Uint32(bs[8:12])
		if bl > 0 {
			body := make([]byte, bl)
			n, err := br.Read(body)
			if err != nil {
				t.Errorf("conn read body error: %v", err)
			} else if n != int(bl) {
				t.Errorf("conn read body size(%d) not equal(%d)", n, bl)
			}
		}
	}
}

func TestMCProxyFull(t *testing.T) {
	// for i := 0; i < 10; i++ {
	testCmd(t, cmds[0], cmds[1], cmds[2], cmds[10], cmds[11])
	testCmdBin(t, cmdBins[0], cmdBins[1])
	// }
}

func TestMCProxyWithAssert(t *testing.T) {
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

	for i := 0; i < 10; i++ {
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
				if len(tt.Except) == 1 {
					assert.Equal(t, sb, tt.Except[0], "CMD:%s", tt.Cmd)
				} else {
					for _, except := range tt.Except {
						assert.Contains(t, sb, except, "CMD:%s", tt.Cmd)
					}
				}
			})
		}
		nc.Close()
	}
}

func TestRedisClusterWithAssert(t *testing.T) {
	ts := []struct {
		Name   string
		Cmd    string
		Line   int
		Except []string
	}{

		{Name: "SetLowerOk", Line: 1, Cmd: "*3\r\n$3\r\nset\r\n$1\r\na\r\n$5\r\nval-a\r\n", Except: []string{"+OK\r\n"}},

		{Name: "MSetLowerOk", Line: 1,
			Cmd:    "*5\r\n$4\r\nmset\r\n$1\r\nb\r\n$5\r\nval-b\r\n$1\r\nc\r\n$4\r\n1024\r\n",
			Except: []string{"+OK\r\n"},
		},
		{Name: "MgetOk", Line: 7, Cmd: "*4\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", Except: []string{
			"*3\r\n$5\r\nval-a\r\n$5\r\nval-b\r\n$4\r\n1024\r\n"}},

		// {Name: "GetMultiMissOneOk", Line: 5, Cmd: "get a_11 a_22 a_33\r\n", Except: []string{"VALUE a_11 0 1\r\n1\r\n", "VALUE a_22 0 4\r\nhalo\r\n", "END\r\n"}},
		// {Name: "GetsOneOk", Line: 3, Cmd: "gets a_11\r\n", Except: []string{"VALUE a_11 0 1 ", "\r\n1\r\nEND\r\n"}},
		// {Name: "GetMultiCasMissOneOk", Line: 5, Cmd: "gets a_11 a_22 a_33\r\n", Except: []string{"VALUE a_11 0 1", "\r\n1\r\n", "VALUE a_22 0 4", "\r\nhalo\r\n", "END\r\n"}},
		// {Name: "MultiCmdGetOk", Line: 6, Cmd: "gets a_11\r\ngets a_11\r\n", Except: []string{"VALUE a_11 0 1", "\r\n1\r\n", "END\r\n"}},
	}

	for i := 0; i < 10; i++ {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:21214", time.Second)
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
				if len(tt.Except) == 1 {
					assert.Equal(t, sb, tt.Except[0], "CMD:%s", tt.Cmd)
				} else {
					for _, except := range tt.Except {
						assert.Contains(t, sb, except, "CMD:%s", tt.Cmd)
					}
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
			testCmdBin(b, cmdBins[0])
		}
	})
}

func BenchmarkCmdGet(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			testCmd(b, cmds[1])
			testCmdBin(b, cmdBins[1])
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
