package slowlog

import (
	"bufio"
	"os"
	"time"

	"encoding/json"
	"overlord/pkg/log"
	"overlord/proxy/proto"
)

const byteSpace = byte(' ')
const byteLF = byte('\n')

type fileHandler struct {
	fd            *os.File
	wr            *bufio.Writer
	encoder       *json.Encoder
	exchange      chan *proto.SlowlogEntry
	flushInterval time.Duration
}

func (f *fileHandler) save(cluster string, entry *proto.SlowlogEntry) {
	entry.Cluster = cluster
	select {
	case f.exchange <- entry:
	default:
	}
}

func (f *fileHandler) openFile(file string) error {
	var (
		fd *os.File
		err error
	)
	if _, err = os.Stat(file); os.IsNotExist(err) {
		// path/to/whatever does not exist
		fd, err = os.Create(file)
		if err != nil {
			return err
		}
	} else {
		fd, err = os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0755)
		if err != nil {
			return err
		}
	}


	f.fd = fd
	f.wr = bufio.NewWriter(f.fd)
	f.encoder = json.NewEncoder(f.wr)

	go func() {
		defer f.fd.Close()
		var ticker = time.NewTicker(f.flushInterval)

		for {
			select {
			case entry := <-f.exchange:
				err := f.encoder.Encode(entry)
				if err != nil {
					log.Errorf("fail to write slowlog into file due %s", err)
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
		exchange:      make(chan *proto.SlowlogEntry, 2048),
		flushInterval: time.Second * 5,
	}
	return fh.openFile(file)
}
