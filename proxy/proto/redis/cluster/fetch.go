package cluster

import (
	"bytes"
	errs "errors"

	"overlord/pkg/bufio"
	libnet "overlord/pkg/net"
	"overlord/proxy/proto/redis"

	"github.com/pkg/errors"
)

const (
	respFetch = '$'
)

var (
	crlfBytes = []byte("\r\n")
)

// fetcher will execute `CLUSTER NODES` by the given address。
type fetcher struct {
	conn *libnet.Conn
	bw   *bufio.Writer
	br   *bufio.Reader
}

var (
	cmdClusterNodesBytes = []byte("*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")

	// ErrBadReplyType error bad reply type
	ErrBadReplyType = errs.New("fetcher CLUSTER NODES bad reply type")
)

// newFetcher will create new fetcher
func newFetcher(conn *libnet.Conn) *fetcher {
	f := &fetcher{
		conn: conn,
		br:   bufio.NewReader(conn, bufio.Get(4096)),
		bw:   bufio.NewWriter(conn),
	}
	return f
}

// Fetch new CLUSTER NODES result
func (f *fetcher) fetch() (ns *nodeSlots, err error) {
	if err = f.bw.Write(cmdClusterNodesBytes); err != nil {
		err = errors.WithStack(err)
		return
	}
	if err = f.bw.Flush(); err != nil {
		err = errors.WithStack(err)
		return
	}
	var data []byte
	begin := f.br.Mark()
	for {
		err = f.br.Read()
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		reply := &redis.RESP{}
		if err = reply.Decode(f.br); err == bufio.ErrBufferFull {
			f.br.AdvanceTo(begin)
			continue
		} else if err != nil {
			err = errors.WithStack(err)
			return
		}
		if reply.Type() != respFetch {
			err = errors.WithStack(ErrBadReplyType)
			return
		}
		data = reply.Data()
		idx := bytes.Index(data, crlfBytes)
		data = data[idx+2:]
		break
	}
	return parseSlots(data)
}

// Close enable to close the conneciton of backend.
func (f *fetcher) Close() error {
	return f.conn.Close()
}
