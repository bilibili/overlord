package slowlog

import (
	"bufio"
	"os"
	"overlord/pkg/log"
	"overlord/proxy/proto"
	"strings"
	"time"
)

const byteSpace = byte(' ')
const byteLF = byte('\n')

type fileEntry struct {
	name  string
	entry *proto.SlowlogEntry
}

type fileHandler struct {
	fd            *os.File
	wr            *bufio.Writer
	exchange      chan fileEntry
	flushInterval time.Duration
}

func (f *fileHandler) save(cluster string, entry *proto.SlowlogEntry) {
	select {
	case f.exchange <- fileEntry{name: cluster, entry: entry}:
	default:
	}
}

func (f *fileHandler) openFile(file string) error {
	fd, err := os.Create(file)
	if err != nil {
		return err
	}
	f.fd = fd
	f.wr = bufio.NewWriter(f.fd)
	go func() {
		var sb strings.Builder
		var ticker = time.NewTicker(f.flushInterval)

		for {
			select {
			case fe := <-f.exchange:
				sb.Reset()
				sb.WriteString("cluster=")
				sb.WriteString(fe.name)
				sb.WriteByte(byteSpace)
				sb.WriteString(fe.entry.String())
				sb.WriteByte(byteLF)
				_, err := f.wr.WriteString(sb.String())
				if err != nil {
					log.Errorf("fail to output slowlog due %s", err)
					return
				}
			case <-ticker.C:
				if f.wr.Size() > 0 {
					err := f.wr.Flush()
					if err != nil {
						log.Errorf("fail to flush slowlog due %s", err)
						return
					}
				}
			}
		}
	}()

	return nil
}

var fh *fileHandler

// initFileHandler will init the file handler to the given file
func initFileHandler(file string) error {
	fh = &fileHandler{
		exchange:      make(chan fileEntry, 2048),
		flushInterval: time.Second,
	}
	return fh.openFile(file)
}
