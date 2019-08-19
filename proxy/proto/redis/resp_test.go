package redis

import (
	"testing"
	"time"

	"overlord/pkg/bufio"
	"overlord/pkg/mockconn"
	libnet "overlord/pkg/net"

	"github.com/stretchr/testify/assert"
)

func TestRespDecode(t *testing.T) {
	ts := []struct {
		Name       string
		Bytes      []byte
		ExpectTp   byte
		ExpectLen  int
		ExpectData []byte
		ExpectArr  [][]byte
	}{
		{
			Name:       "ok",
			Bytes:      []byte("+OK\r\n"),
			ExpectTp:   respString,
			ExpectLen:  0,
			ExpectData: []byte("OK"),
		},
		{
			Name:       "error",
			Bytes:      []byte("-Error message\r\n"),
			ExpectTp:   respError,
			ExpectLen:  0,
			ExpectData: []byte("Error message"),
		},
		{
			Name:       "int",
			Bytes:      []byte(":1000\r\n"),
			ExpectTp:   respInt,
			ExpectLen:  0,
			ExpectData: []byte("1000"),
		},
		{
			Name:       "bulk",
			Bytes:      []byte("$6\r\nfoobar\r\n"),
			ExpectTp:   respBulk,
			ExpectLen:  0,
			ExpectData: []byte("6\r\nfoobar"),
		},
		{
			Name:       "array1",
			Bytes:      []byte("*2\r\n$3\r\nfoo\r\n$4\r\nbara\r\n"),
			ExpectTp:   respArray,
			ExpectLen:  2,
			ExpectData: []byte("2"),
			ExpectArr: [][]byte{
				[]byte("3\r\nfoo"),
				[]byte("4\r\nbara"),
			},
		},
		{
			Name:       "array2",
			Bytes:      []byte("*3\r\n:1\r\n:2\r\n:3\r\n"),
			ExpectTp:   respArray,
			ExpectLen:  3,
			ExpectData: []byte("3"),
			ExpectArr: [][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
			},
		},
		{
			Name:       "array3",
			Bytes:      []byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"),
			ExpectTp:   respArray,
			ExpectLen:  2,
			ExpectData: []byte("2"),
			ExpectArr: [][]byte{
				[]byte("3"),
				[]byte("2"),
			},
		},
	}
	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			conn := libnet.NewConn(mockconn.CreateConn(tt.Bytes, 1), time.Second, time.Second)
			r := &resp{}
			r.reset()
			br := bufio.NewReader(conn, bufio.Get(1024))
			br.Read()
			if err := r.decode(br); err != nil {
				t.Fatalf("decode error:%v", err)
			}
			assert.Equal(t, tt.ExpectTp, r.respType)
			assert.Equal(t, tt.ExpectLen, r.arraySize)
			assert.Equal(t, tt.ExpectData, r.data)
			if len(tt.ExpectArr) > 0 {
				for i, ea := range tt.ExpectArr {
					assert.Equal(t, ea, r.array[i].data)
				}
			}
		})
	}
}

func TestRespEncode(t *testing.T) {
	ts := []struct {
		Name   string
		Resp   *resp
		Expect []byte
	}{
		{
			Name: "ok",
			Resp: &resp{
				respType: respString,
				data:     []byte("OK"),
			},
			Expect: []byte("+OK\r\n"),
		},
		{
			Name: "error",
			Resp: &resp{
				respType: respError,
				data:     []byte("Error message"),
			},
			Expect: []byte("-Error message\r\n"),
		},
		{
			Name: "int",
			Resp: &resp{
				respType: respInt,
				data:     []byte("1000"),
			},
			Expect: []byte(":1000\r\n"),
		},
		{
			Name: "bulk",
			Resp: &resp{
				respType: respBulk,
				data:     []byte("6\r\nfoobar"),
			},
			Expect: []byte("$6\r\nfoobar\r\n"),
		},
		{
			Name: "array1",
			Resp: &resp{
				respType: respArray,
				data:     []byte("2"),
				array: []*resp{
					&resp{
						respType: respBulk,
						data:     []byte("3\r\nfoo"),
					},
					&resp{
						respType: respBulk,
						data:     []byte("4\r\nbara"),
					},
				},
				arraySize: 2,
			},
			Expect: []byte("*2\r\n$3\r\nfoo\r\n$4\r\nbara\r\n"),
		},
		{
			Name: "array2",
			Resp: &resp{
				respType: respArray,
				data:     []byte("3"),
				array: []*resp{
					&resp{
						respType: respInt,
						data:     []byte("1"),
					},
					&resp{
						respType: respInt,
						data:     []byte("2"),
					},
					&resp{
						respType: respInt,
						data:     []byte("3"),
					},
				},
				arraySize: 3,
			},
			Expect: []byte("*3\r\n:1\r\n:2\r\n:3\r\n"),
		},
		{
			Name: "array3",
			Resp: &resp{
				respType: respArray,
				data:     []byte("2"),
				array: []*resp{
					&resp{
						respType: respArray,
						data:     []byte("3"),
						array: []*resp{
							&resp{
								respType: respInt,
								data:     []byte("1"),
							},
							&resp{
								respType: respInt,
								data:     []byte("2"),
							},
							&resp{
								respType: respInt,
								data:     []byte("3"),
							},
						},
						arraySize: 3,
					},
					&resp{
						respType: respArray,
						data:     []byte("2"),
						array: []*resp{
							&resp{
								respType: respString,
								data:     []byte("Foo"),
							},
							&resp{
								respType: respError,
								data:     []byte("Bar"),
							},
						},
						arraySize: 2,
					},
				},
				arraySize: 2,
			},
			Expect: []byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"),
		},
	}
	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			conn := libnet.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
			bw := bufio.NewWriter(conn)

			err := tt.Resp.encode(bw)
			bw.Flush()
			assert.Nil(t, err)

			buf := make([]byte, 1024)
			n, err := conn.Conn.(*mockconn.MockConn).Wbuf.Read(buf)
			assert.Nil(t, err)
			assert.Equal(t, tt.Expect, buf[:n])
		})
	}
}

func TestRESPExportFunc(t *testing.T) {
	var r = &RESP{
		respType: respString,
		data:     []byte("abcde"),
	}
	assert.Equal(t, "abcde", string(r.Data()))
	assert.Equal(t, respString, r.Type())
	assert.Len(t, r.Array(), 0)
	conn := libnet.NewConn(mockconn.CreateConn([]byte("get a\r\n"), 1), time.Second, time.Second)
	br := bufio.NewReader(conn, bufio.Get(1024))
	br.Read()
	err := r.Decode(br)
	assert.NoError(t, err)
	conn = libnet.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
	bw := bufio.NewWriter(conn)
	err = r.encode(bw)
	assert.NoError(t, err)
}
