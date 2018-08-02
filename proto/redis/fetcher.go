package redis

import (
	"bytes"
	"overlord/lib/bufio"
	libnet "overlord/lib/net"
)

// Fetcher will execute `CLUSTER NODES` by the given address。
type Fetcher struct {
	conn *libnet.Conn
	bw   *bufio.Writer
	br   *bufio.Reader
}

var (
	cmdClusterNodesRawBytes = []byte("*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
)

// NewFetcher will create new Fetcher
func NewFetcher(conn *libnet.Conn) *Fetcher {
	// TODO: remove magic number
	f := &Fetcher{
		conn: conn,
		br:   bufio.NewReader(conn, bufio.Get(1024)),
		bw:   bufio.NewWriter(conn),
	}
	return f
}

// Fetch new CLUSTER NODES result
func (f *Fetcher) Fetch() (data []byte, err error) {
	if err = f.bw.Write(cmdClusterNodesRawBytes); err != nil {
		return
	}
	reply := &resp{}
	if err = reply.decode(f.br); err != nil {
		return
	}
	if reply.rTp != respBulk {
		err = ErrBadReplyType
		return
	}
	idx := bytes.Index(reply.data, crlfBytes)
	if idx == -1 {
		err = ErrBadRequest
		return
	}
	data = reply.data[idx+2:]
	return
}
