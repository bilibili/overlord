package cluster

import (
	"overlord/lib/bufio"
	libnet "overlord/lib/net"
	"overlord/proto/redis"

	"github.com/pkg/errors"
)

// fetcher will execute `CLUSTER NODES` by the given address。
type fetcher struct {
	conn *libnet.Conn
	bw   *bufio.Writer
	br   *bufio.Reader
}

var (
	cmdClusterNodesBytes = []byte("*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
	// ErrBadReplyType      = errors.New("bad reply type")
)

// newFetcher will create new fetcher
func newFetcher(conn *libnet.Conn) *fetcher {
	f := &fetcher{
		conn: conn,
		br:   bufio.NewReader(conn, bufio.Get(1024)),
		bw:   bufio.NewWriter(conn),
	}
	return f
}

// Fetch new CLUSTER NODES result
func (f *fetcher) fetch() (ns *nodeSlots, err error) {
	if err = f.bw.Write(cmdClusterNodesBytes); err != nil {
		err = errors.Wrap(err, "while encode.")
		return
	}
	if err = f.bw.Flush(); err != nil {
		err = errors.Wrap(err, "while call writev")
		return
	}
	var data []byte
	begin := f.br.Mark()
	for {
		err = f.br.Read()
		if err != nil {
			err = errors.Wrap(err, "while call read syscall")
			return
		}
		reply := &redis.RESP{}
		if err = reply.Decode(f.br); err == bufio.ErrBufferFull {
			f.br.AdvanceTo(begin)
			continue
		} else if err != nil {
			err = errors.Wrap(err, "while decode")
			return
		}
		// if reply.rTp != respBulk {
		// 	err = ErrBadReplyType
		// 	return
		// }
		// idx := bytes.Index(reply.data, crlfBytes)
		// data = reply.data[idx+2:]
		break
	}
	return parseSlots(data)
}

// Close enable to close the conneciton of backend.
func (f *fetcher) Close() error {
	return f.conn.Close()
}
