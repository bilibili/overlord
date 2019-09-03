package slowlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

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

	fileName    string
	curBytes    int
	maxBytes    int
	backupCount int
}

func (f *fileHandler) save(cluster string, entry *proto.SlowlogEntry) {
	entry.Cluster = cluster
	select {
	case f.exchange <- entry:
	default:
	}
}

func (f *fileHandler) openFile() error {
	if _, err := os.Stat(f.fileName); os.IsNotExist(err) {
		// path/to/whatever does not exist
		f.fd, err = os.Create(f.fileName)
		if err != nil {
			return err
		}
	} else {
		f.fd, err = os.OpenFile(f.fileName, os.O_APPEND|os.O_WRONLY, 0755)
		if err != nil {
			return err
		}
	}

	fdStat, err := f.fd.Stat()
	if err != nil {
		return err
	}
	f.curBytes = int(fdStat.Size())

	f.wr = bufio.NewWriterSize(f.fd, 40960)
	f.encoder = json.NewEncoder(f.wr)

	return nil
}

func (f *fileHandler) rotate() {
	fdStat, err := f.fd.Stat()
	if err != nil {
		return
	}

	if f.maxBytes <= 0 {
		return
	} else if fdStat.Size() < int64(f.maxBytes) {
		f.curBytes = int(fdStat.Size())
		return
	}

	if f.backupCount > 0 {
		_ = f.fd.Close()

		for i := f.backupCount - 1; i > 0; i-- {
			sfn := fmt.Sprintf("%s.%d", f.fileName, i)
			dfn := fmt.Sprintf("%s.%d", f.fileName, i+1)
			_ = os.Rename(sfn, dfn)
		}

		dfn := fmt.Sprintf("%s.1", f.fileName)
		_ = os.Rename(f.fileName, dfn)

		err := f.openFile()
		if err != nil {
			return
		}
	}
}

func (f *fileHandler) close() error {
	if f.fd != nil {
		return f.fd.Close()
	}
	return nil
}

func (f *fileHandler) run() {
	defer f.close()
	ticker := time.NewTicker(f.flushInterval)

	for {
		select {
		case entry := <-f.exchange:
			err := f.encoder.Encode(entry)
			if err != nil {
				log.Errorf("fail to write slowlog into file due %s", err)
				return
			}
		case <-ticker.C:
			f.rotate() // check slowlog file size and rotate slowlog file
			if f.wr.Buffered() > 0 {
				err := f.wr.Flush()
				if err != nil {
					log.Errorf("fail to flush slowlog due %s", err)
					return
				}
			}
		}
	}
}

var fh *fileHandler

// initFileHandler will init the file handler to the given file
func initFileHandler(fileName string, maxBytes int, backupCount int) error {
	fh = &fileHandler{
		exchange:      make(chan *proto.SlowlogEntry, 2048),
		flushInterval: time.Second * 5,
		maxBytes:      maxBytes,
		backupCount:   backupCount,
		fileName:      fileName,
	}
	err := fh.openFile()
	if err != nil {
		return err
	}
	go fh.run()
	return err
}
