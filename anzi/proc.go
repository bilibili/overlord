package anzi

// import "overlord/proxy/proto"
import (
	"bufio"
	"bytes"
	"sync/atomic"

	"fmt"
	"io"
	"net"
	"overlord/pkg/log"
	"strconv"
)

const (
	byteLF                  = byte('\n')
	replConfAckCmdFormatter = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n"
)

var (
	bytesSpace       = []byte(" ")
	psyncFullSyncCmd = []byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
)

// NewMigrateProc create new migrate proc in migrate data
func NewMigrateProc(cfg *MigrateConfig) *MigrateProc {
	m := &MigrateProc{
		cfg:      cfg,
		barrierC: make(chan struct{}, cfg.MaxRDBConcurrency),
	}


	return m
}

// MigrateProc is the process for anzi.
type MigrateProc struct {
	cfg      *MigrateConfig
	barrierC chan struct{}
}

// NewInstance create instances by given config
func NewInstance(bc chan struct{}, ) *Instnace {
	// TODO
	return nil
}

// Instance is the struct for instance node
type Instance struct {
	Addr   string
	Target string

	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	barrierC chan struct{}

	offset   int64
	masterID string
}

func (inst *Instance) parsePSyncReply(data []byte) error {
	splited := bytes.Split(data, bytesSpace)
	runidBs := string(splited[1])
	offsetBs := string(splited[2][:len(splited[2])-2])
	log.Infof("sync from %s by %s %s %s",
		inst.Addr, strconv.Quote(string(splited[0])), strconv.Quote(runidBs), offsetBs)

	offset, err := strconv.ParseInt(offsetBs, 10, 64)
	if err != nil {
		return err
	}
	inst.offset = offset
	inst.masterID = runidBs
	return nil
}

// Sync is the process of sync data
func (inst *Instance) Sync() (err error) {
	defer inst.conn.Close()

	<-inst.barrierC
	// 1. barrier run syncRDB
	// 1.1 send psync ? -1
	_, _ = inst.bw.Write(psyncFullSyncCmd)
	_ = inst.bw.Flush()
	data, err := inst.br.ReadBytes(byteLF)
	if err != nil {
		return
	}

	err = inst.parsePSyncReply(data)
	if err != nil {
		return err
	}

	// read full rdb
	err = inst.syncRDB()
	if err != nil {
		return
	}

	// 2. parsed rdb done then send notify to barier chan
	select {
	case inst.barrierC <- struct{}{}:
	default:
	}
	// 3. trying to receive more command and send back replconf size
	// 4. dispatch commands into cluster backend(for more, in copy model)
	go inst.replAck()
	err = inst.cmdForward()
	return
}

func (inst *Instance) cmdForward() error {
	conn, err := net.Dial("tcp", inst.Target)
	if err != nil {
		return err
	}
	defer conn.Close()
	for {
		size, err := io.Copy(conn, inst.br)
		if err != nil {
			return err
		}
		atomic.AddInt64(&inst.offset, int64(size))
	}
}

func getStrLen(v int64) int {
	rv := 0
	for {
		if v == 0 {
			return rv
		} else if v < 10 {
			return rv + 1
		} else if v < 100 {
			return rv + 2
		} else if v < 1000 {
			return rv + 3
		}

		v = v / 10000
		rv += 3
	}
}

func (inst *Instance) replAck() {
	for {
		offset := atomic.LoadInt64(&inst.offset)
		cmd := fmt.Sprintf(replConfAckCmdFormatter, getStrLen(offset), offset)
		_, err := inst.bw.WriteString(cmd)
		if err != nil {
			log.Errorf("fail to send repl ack command, connection maybe closed soon")
			return
		}
	}
}

func (inst *Instance) syncRDB() (err error) {
	return
}

// Slot is the struct of slot
type Slot struct {
	Begin, End int
}

// Migrate start new migrate process
func (m *MigrateProc) Migrate() error {

	return nil
}
