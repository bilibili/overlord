package redis

import (
	"errors"
	"testing"

	"overlord/lib/bufio"

	"github.com/stretchr/testify/assert"
)

var _errNone = errors.New("error unknonw")

func TestRespDecode(t *testing.T) {
	ts := []struct {
		Name              string
		Bytes             []byte
		ExpectTp          respType
		ExpectLen         int
		ExpectData        []byte
		ExpectArr         [][]byte
		ExpectDecodeError error
	}{
		{
			Name:       "StringOk",
			Bytes:      []byte("+OK\r\n"),
			ExpectTp:   respString,
			ExpectLen:  0,
			ExpectData: []byte("OK"),
		},
		{
			Name:       "ErrorOk",
			Bytes:      []byte("-Error message\r\n"),
			ExpectTp:   respError,
			ExpectLen:  0,
			ExpectData: []byte("Error message"),
		},
		{
			Name:       "IntOk",
			Bytes:      []byte(":1000\r\n"),
			ExpectTp:   respInt,
			ExpectLen:  0,
			ExpectData: []byte("1000"),
		},
		{
			Name:       "BulkOk",
			Bytes:      []byte("$6\r\nfoobar\r\n"),
			ExpectTp:   respBulk,
			ExpectLen:  0,
			ExpectData: []byte("6\r\nfoobar"),
		},
		{
			Name:              "BulkBadLenOk",
			Bytes:             []byte("$abdce\r\nfoobar\r\n"),
			ExpectDecodeError: _errNone,
		},
		{
			Name:       "BulkNullOk",
			Bytes:      []byte("$-1\r\n"),
			ExpectTp:   respBulk,
			ExpectLen:  0,
			ExpectData: nil,
		},
		{
			Name:       "ArrayWith2BulkOk",
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
			Name:       "ArrayNullOk",
			Bytes:      []byte("*-1\r\n"),
			ExpectTp:   respArray,
			ExpectLen:  0,
			ExpectData: nil,
		},
		{
			Name:              "ArrayBadLenError",
			Bytes:             []byte("*boynextdoor\r\n"),
			ExpectDecodeError: _errNone,
		},
		{
			Name:       "ArrayWtih3IntOk",
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
			Name:       "ArrayRecursiveOk",
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
			conn := _createConn(tt.Bytes)
			r := &resp{}
			r.reset()
			br := bufio.NewReader(conn, bufio.Get(1024))
			br.Read()
			if err := r.decode(br); err != nil {
				if tt.ExpectDecodeError == _errNone {
					return
				}
				if tt.ExpectDecodeError != nil {
					assert.Equal(t, tt.ExpectDecodeError, err)
					return
				} else {
					t.Fatalf("decode error:%v", err)
				}
			}
			assert.Equal(t, tt.ExpectTp, r.rTp)
			assert.Equal(t, tt.ExpectLen, r.arrayn)
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
			Name: "RespStringOk",
			Resp: &resp{
				rTp:  respString,
				data: []byte("OK"),
			},
			Expect: []byte("+OK\r\n"),
		},
		{
			Name: "RespErrorOk",
			Resp: &resp{
				rTp:  respError,
				data: []byte("Error message"),
			},
			Expect: []byte("-Error message\r\n"),
		},
		{
			Name: "RespIntOk",
			Resp: &resp{
				rTp:  respInt,
				data: []byte("1000"),
			},
			Expect: []byte(":1000\r\n"),
		},
		{
			Name: "BulkOk",
			Resp: &resp{
				rTp:  respBulk,
				data: []byte("6\r\nfoobar"),
			},
			Expect: []byte("$6\r\nfoobar\r\n"),
		},
		{
			Name: "NullBulkOk",
			Resp: &resp{
				rTp:  respBulk,
				data: []byte(""),
			},
			Expect: []byte("$-1\r\n"),
		},
		{
			Name: "RespArrayAsCommandOk",
			Resp: &resp{
				rTp:  respArray,
				data: []byte("2"),
				array: []*resp{
					&resp{
						rTp:  respBulk,
						data: []byte("3\r\nfoo"),
					},
					&resp{
						rTp:  respBulk,
						data: []byte("4\r\nbara"),
					},
				},
				arrayn: 2,
			},
			Expect: []byte("*2\r\n$3\r\nfoo\r\n$4\r\nbara\r\n"),
		},
		{
			Name: "RespArrayNullOk",
			Resp: &resp{
				rTp:    respArray,
				data:   []byte(""),
				array:  []*resp{},
				arrayn: 0,
			},
			Expect: []byte("*-1\r\n"),
		},
		{
			Name: "RespArrayWith3IntOk",
			Resp: &resp{
				rTp:  respArray,
				data: []byte("3"),
				array: []*resp{
					&resp{
						rTp:  respInt,
						data: []byte("1"),
					},
					&resp{
						rTp:  respInt,
						data: []byte("2"),
					},
					&resp{
						rTp:  respInt,
						data: []byte("3"),
					},
				},
				arrayn: 3,
			},
			Expect: []byte("*3\r\n:1\r\n:2\r\n:3\r\n"),
		},
		{
			Name: "RespArrayRecursiveOk",
			Resp: &resp{
				rTp:  respArray,
				data: []byte("2"),
				array: []*resp{
					&resp{
						rTp:  respArray,
						data: []byte("3"),
						array: []*resp{
							&resp{
								rTp:  respInt,
								data: []byte("1"),
							},
							&resp{
								rTp:  respInt,
								data: []byte("2"),
							},
							&resp{
								rTp:  respInt,
								data: []byte("3"),
							},
						},
						arrayn: 3,
					},
					&resp{
						rTp:  respArray,
						data: []byte("2"),
						array: []*resp{
							&resp{
								rTp:  respString,
								data: []byte("Foo"),
							},
							&resp{
								rTp:  respError,
								data: []byte("Bar"),
							},
						},
						arrayn: 2,
					},
				},
				arrayn: 2,
			},
			Expect: []byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"),
		},
	}
	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			conn := _createConn(nil)
			bw := bufio.NewWriter(conn)

			err := tt.Resp.encode(bw)
			bw.Flush()
			assert.Nil(t, err)

			buf := make([]byte, 1024)
			n, err := conn.Conn.(*mockConn).wbuf.Read(buf)
			assert.Nil(t, err)
			assert.Equal(t, tt.Expect, buf[:n])
		})
	}
}
