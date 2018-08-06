package redis

import (
	"bytes"
	"overlord/lib/bufio"
	libnet "overlord/lib/net"

	"github.com/pkg/errors"
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
		err = errors.Wrap(err, "while encode.")
		return
	}

	if err = f.bw.Flush(); err != nil {
		err = errors.Wrap(err, "while call writev")
		return
	}

	err = f.br.Read()
	if err != nil {
		err = errors.Wrap(err, "while call read syscall")
		return
	}

	reply := &resp{}
	if err = reply.decode(f.br); err != nil {
		err = errors.Wrap(err, "while decode")
		return
	}
	if reply.rTp != respBulk {
		err = ErrBadReplyType
		return
	}
	idx := bytes.Index(reply.data, crlfBytes)
	data = reply.data[idx+2:]
	return
}

// Close enable to close the conneciton of backend.
func (f *Fetcher) Close() error {
	return f.conn.Close()
}
