package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	libnet "overlord/lib/net"
	"overlord/proto"

	"github.com/stretchr/testify/assert"
)

var (
	ccs = []*ClusterConfig{
		&ClusterConfig{
			Name:             "eject-cluster",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("memcache"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:22211",
			RedisAuth:        "",
			DialTimeout:      100,
			ReadTimeout:      100,
			NodeConnections:  10,
			WriteTimeout:     1000,
			PingFailLimit:    3,
			PingAutoEject:    true,
			Servers: []string{
				"127.0.0.1:11211:10 mc1",
				// "127.0.0.1:11213:10",
			},
		},
		&ClusterConfig{
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
				"127.0.0.1:11211:10 mc1",
				"127.0.0.1:11211:10 mc2",
				// "127.0.0.1:11213:10",
			},
		},
		&ClusterConfig{
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
		&ClusterConfig{
			Name:             "redis",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("redis"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:26379",
			RedisAuth:        "",
			DialTimeout:      100,
			ReadTimeout:      100,
			NodeConnections:  10,
			WriteTimeout:     1000,
			PingFailLimit:    3,
			PingAutoEject:    false,
			Servers: []string{
				"127.0.0.1:6379:10",
				// "127.0.0.1:11212:10",
				// "127.0.0.1:11213:10",
			},
		},
		&ClusterConfig{
			Name:             "redis-cluster",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("redis_cluster"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:27000",
			RedisAuth:        "",
			DialTimeout:      100,
			ReadTimeout:      100,
			NodeConnections:  10,
			WriteTimeout:     1000,
			PingFailLimit:    3,
			PingAutoEject:    false,
			Servers: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				// "127.0.0.1:11212:10",
				// "127.0.0.1:11213:10",
			},
		},
		&ClusterConfig{
			Name:             "no avaliable node ",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("redis"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:26380",
			RedisAuth:        "",
			DialTimeout:      100,
			ReadTimeout:      100,
			NodeConnections:  10,
			WriteTimeout:     1000,
			PingFailLimit:    3,
			PingAutoEject:    false,
			Servers:          []string{
				//"127.0.0.1:6379:10",
				// "127.0.0.1:11212:10",
				// "127.0.0.1:11213:10",
			},
		},
		&ClusterConfig{
			Name:             "reconn_test",
			HashMethod:       "sha1",
			HashDistribution: "ketama",
			HashTag:          "",
			CacheType:        proto.CacheType("memcache"),
			ListenProto:      "tcp",
			ListenAddr:       "127.0.0.1:21221",
			RedisAuth:        "",
			DialTimeout:      100,
			ReadTimeout:      100,
			NodeConnections:  1,
			WriteTimeout:     1000,
			PingFailLimit:    3,
			PingAutoEject:    false,
			Servers: []string{
				"127.0.0.1:21220:1",
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
	cmdRedis = [][]byte{
		[]byte("*5\r\n$4\r\nmset\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"),
		[]byte("*2\r\n$3\r\nget\r\n$1\r\na\r\n"),
		[]byte("*3\r\n$4\r\nmget\r\n$1\r\na\r\n$1\r\nb\r\n"),
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

var p *Proxy

func mockProxy() {
	var err error
	p, err = New(DefaultConfig())
	if err != nil {
		panic(err)
	}
	// serve
	go p.Serve(ccs)
}

func testCmdRedis(t testing.TB, cmds ...[]byte) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:26379", time.Second)
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
		if _, err = br.ReadBytes('\n'); err != nil {
			t.Errorf("conn read cmd:%s error:%s ", cmd, err)
			continue
		}
	}
}

func testCmdRedisCluster(t testing.TB, cmds ...[]byte) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:27000", time.Second)
	if err != nil {
		t.Errorf("net dial error:%v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	for _, cmd := range cmds {
		// t.Logf("\n\nexecute cmd %s", cmd)
		conn.SetWriteDeadline(time.Now().Add(time.Second))
		_, err = conn.Write(cmd)
		assert.NoError(t, err)
		conn.SetReadDeadline(time.Now().Add(time.Second))
		_, err = br.ReadBytes('\n')
		assert.NoError(t, err)
	}
}

func testCmdNotAvaliabeNode(t testing.TB, cmds ...[]byte) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:26380", time.Second)
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
		if _, err = br.ReadBytes('\n'); err != nil {
			t.Errorf("conn read cmd:%s error:%s ", cmd, err)
			continue
		}
	}
}

func _createTcpProxy(t *testing.T, dist, origin int64) (cancel context.CancelFunc) {
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
	cmd := cmds[0]
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

func TestReconnFeature(t *testing.T) {
	cancel := _createTcpProxy(t, 11211, 21220)
	defer cancel()
	// 1. try to execute with error, but click reconn process
	bs := _execute(t)
	assert.Equal(t, "SERVER_ERROR connection is closed\r\n", string(bs))
	// 2. try execute with ok
	bs = _execute(t)
	assert.Equal(t, "STORED\r\n", string(bs))
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

func TestProxyFull(t *testing.T) {
	// for i := 0; i < 10; i++ {
	testCmd(t, cmds[0], cmds[1], cmds[2], cmds[10], cmds[11])
	testCmdRedis(t, cmdRedis...)
	testCmdRedisCluster(t, cmdRedis...)
	testCmdNotAvaliabeNode(t, cmdRedis[1])
	testCmdBin(t, cmdBins[0], cmdBins[1])
	// }
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

func TestEject(t *testing.T) {
	ts := []struct {
		Name   string
		Cmd    string
		Line   int
		eject  bool
		Except []string
	}{

		{Name: "SetLowerOneOk", Line: 1, Cmd: "set a_11 0 1024 1\r\n2\r\n", eject: true, Except: []string{"SERVER_ERROR executor hash no hit node\r\n"}},
		{Name: "SetLowerTwoOk", Line: 1, Cmd: "set a_22 0 1024 4\r\nhalo\r\n", Except: []string{"STORED\r\n"}},
		{Name: "SetUpperOk", Line: 1, Cmd: "SET a_11 0 1024 1\r\n1\r\n", Except: []string{"STORED\r\n"}},

		{Name: "GetMultiMissOneOk", Line: 5, Cmd: "get a_11 a_22 a_33\r\n", Except: []string{"VALUE a_11 0 1\r\n1\r\n", "VALUE a_22 0 4\r\nhalo\r\n", "END\r\n"}},
		{Name: "GetsOneOk", Line: 3, Cmd: "gets a_11\r\n", Except: []string{"VALUE a_11 0 1 ", "\r\n1\r\nEND\r\n"}},
		{Name: "GetMultiCasMissOneOk", Line: 5, Cmd: "gets a_11 a_22 a_33\r\n", Except: []string{"VALUE a_11 0 1", "\r\n1\r\n", "VALUE a_22 0 4", "\r\nhalo\r\n", "END\r\n"}},
		{Name: "MultiCmdGetOk", Line: 6, Cmd: "gets a_11\r\ngets a_11\r\n", Except: []string{"VALUE a_11 0 1", "\r\n1\r\n", "END\r\n"}},
	}
	eject := ccs[0]
	exec := p.executors["eject-cluster"].(*defaultExecutor)
	mp := &mockPing{}
	ping := &pinger{ping: mp, cc: eject, weight: 10, alias: "mc1"}
	go exec.processPing(ping)

	for _, tt := range ts {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:22211", time.Second)
		if err != nil {
			t.Errorf("net dial error:%v", err)
		}
		nc := libnet.NewConn(conn, time.Second, time.Second)
		br := bufio.NewReader(nc)
		if tt.eject {
			mp.SetErr(&mockErr{})
			time.Sleep(time.Second * 3)
		} else {
			mp.SetErr(nil)
			time.Sleep(time.Second * 1)
		}
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
				assert.Equal(t, tt.Except[0], sb, "CMD:%s", tt.Cmd)
			} else {
				for _, except := range tt.Except {
					assert.Contains(t, sb, except, "CMD:%s", tt.Cmd)
				}
			}
		})
	}
}

type mockPing struct {
	err error
}

func (mp *mockPing) Ping() error {
	return mp.err
}

func (mp *mockPing) Close() error {
	return nil
}

func (mp *mockPing) SetErr(err error) {
	mp.err = err
}

type mockErr struct {
	err error
}

func (e *mockErr) Error() string {
	return "mock err"
}
func (e *mockErr) Timeout() bool {
	return true
}
func (e *mockErr) Temporary() bool {
	return true
}
