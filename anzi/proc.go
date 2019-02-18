package anzi

// import "overlord/proxy/proto"
import (
	"bufio"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	"io"
	"io/ioutil"
	"net"
	"overlord/pkg/conv"
	"overlord/pkg/log"
	"overlord/pkg/types"
	"overlord/proxy"
	"strconv"
	"strings"
)

const (
	byteLF                  = byte('\n')
	byteBulkString          = byte('$')
	byteArray               = byte('*')
	byteSpace               = byte(' ')
	replConfAckCmdFormatter = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n"
)

var (
	bytesSpace           = []byte(" ")
	psyncFullSyncCmd     = []byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	bytesClusterNodesCmd = []byte("*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
	pingInlineCMD        = []byte("PING\r\n")
)

// NewMigrateProc create new migrate proc in migrate data
func NewMigrateProc(cfg *MigrateConfig) *MigrateProc {
	m := &MigrateProc{
		cfg:      cfg,
		barrierC: make(chan struct{}, cfg.MaxRDBConcurrency),
		wg:       &sync.WaitGroup{},
	}

	lsp := strings.Split(m.cfg.To.ListenAddr, ":")
	m.target = fmt.Sprintf("127.0.0.1:%s", lsp[1])
	return m
}

// MigrateProc is the process for anzi.
type MigrateProc struct {
	cfg      *MigrateConfig
	barrierC chan struct{}
	wg       *sync.WaitGroup
	target   string
}

// Migrate start new migrate process
func (m *MigrateProc) Migrate() error {
	cfg := proxy.DefaultConfig()
	p, err := proxy.New(cfg)
	if err != nil {
		return err
	}
	p.Serve([]*proxy.ClusterConfig{m.cfg.To})

	addrs, err := m.fromServers(m.cfg.From)
	if err != nil {
		return err
	}

	log.Infof("parsed addrs %s", addrs)
	m.wg.Add(len(addrs))

	for _, addr := range addrs {
		inst := &Instance{
			Addr:     addr,
			Target:   m.target,
			barrierC: m.barrierC,
			wg:       m.wg,
		}
		go inst.Sync()
	}

	log.Infof("wait for cluster listening at %s", m.target)
	err = m.CheckPing()
	if err != nil {
		log.Errorf("fail to check with ping command")
		return err
	}

	for i := 0; i < m.cfg.MaxRDBConcurrency; i++ {
		m.barrierC <- struct{}{}
	}

	m.wg.Wait()
	return nil
}

func (m *MigrateProc) fromServers(from []*proxy.ClusterConfig) ([]string, error) {
	addrs := []string{}
	for _, cc := range from {

		// for redis cluster mode
		if cc.CacheType == types.CacheTypeRedisCluster {
			var (
				err    error
				caddrs []string
			)
			for _, seed := range cc.Servers {
				caddrs, err = m.fetchClusterNodes(seed)
				if err != nil {
					continue
				}
				addrs = append(addrs, caddrs...)
				break
			}
			continue
		}

		// for single/tw mode
		for _, server := range cc.Servers {
			ssp := strings.Split(server, ":")
			if len(ssp) < 2 {
				continue
			}
			addrs = append(addrs, ssp[0]+":"+ssp[1])
		}
	}
	return addrs, nil
}

func (m *MigrateProc) fetchClusterNodes(seed string) ([]string, error) {
	conn, err := net.Dial("tcp", seed)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	left := len(bytesClusterNodesCmd)
	for left != 0 {
		size, err := conn.Write(bytesClusterNodesCmd[len(bytesClusterNodesCmd)-left:])
		if err != nil {
			log.Errorf("fail to dial with target %s", err)
			return nil, err
		}
		left -= size
	}

	br := bufio.NewReader(conn)
	buf, err := br.ReadBytes(byteLF)
	if err != nil {
		return nil, err
	}
	size, err := conv.Btoi(buf[1 : len(buf)-2])
	if err != nil {
		return nil, err
	}
	data := make([]byte, size+2)
	_, err = io.ReadFull(br, data)
	if err != nil {
		return nil, err
	}
	ds := string(data[:len(data)-2])
	addrs := []string{}
	for _, line := range strings.Split(ds, "\n") {
		if len(line) == 0 {
			continue
		}
		if strings.Contains(line, "slave") {
			continue
		}
		lsp := strings.Split(line, " ")
		addr := strings.Split(lsp[1], "@")[0]
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// CheckPing will check if remote is ready for listen
func (m *MigrateProc) CheckPing() error {
	ticker := time.NewTicker(time.Second)
	buf := make([]byte, 7)
	for {
		err := m.checkPing(buf)
		if err != nil {
			<-ticker.C
			continue
		}
		break
	}
	return nil
}

func (m *MigrateProc) checkPing(buf []byte) error {
	conn, err := net.Dial("tcp", m.target)
	if err != nil {
		log.Errorf("fail to dial to target %s", err)
		return err
	}
	defer conn.Close()

	left := len(pingInlineCMD)
	for left != 0 {
		size, err := conn.Write(pingInlineCMD[len(pingInlineCMD)-left:])
		if err != nil {
			log.Errorf("fail to dial with target %s", err)
			return err
		}
		left -= size
	}

	_, err = io.ReadFull(conn, buf)
	return err
}

// Instance is the struct for instance node
type Instance struct {
	Addr   string
	Target string

	tconn net.Conn

	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	barrierC chan struct{}
	wg       *sync.WaitGroup

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
	defer inst.wg.Done()
	log.Infof("tring to sync with remote instance %s", inst.Addr)

	<-inst.barrierC
	log.Infof("starting to sync with remote instance %s", inst.Addr)
	conn, err := net.Dial("tcp", inst.Addr)
	if err != nil {
		return err
	}
	inst.conn = conn
	inst.bw = bufio.NewWriter(conn)
	inst.br = bufio.NewReader(conn)
	defer inst.conn.Close()

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

	// because rdb was transformed by RESP Bulk String, we need ignore first line
	_, err = inst.br.ReadBytes(byteLF)
	if err != nil {
		return err
	}

	// read full rdb
	err = inst.syncRDB(inst.Target)
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
	log.Infof("start forwarding command from %s to %s", inst.Addr, inst.Target)
	conn, err := net.Dial("tcp", inst.Target)
	if err != nil {
		return err
	}
	defer conn.Close()
	inst.tconn = conn

	var wg sync.WaitGroup
	wg.Add(2)

	// go inst.downStream(&wg)
	go func() {
		defer wg.Done()
		for {
			_, err := io.Copy(ioutil.Discard, conn)
			if err != nil {
				log.Infof("closed by upstream due %s", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer inst.Close()

		for {
			size, err := io.Copy(inst.tconn, inst.br)
			if err != nil {
				return
			}
			atomic.AddInt64(&inst.offset, size)
		}
	}()

	wg.Wait()
	return nil
}

func writeAll(buf []byte, w io.Writer) error {
	left := len(buf)
	for left != 0 {
		size, err := w.Write(buf[len(buf)-left:])
		if err != nil {
			return err
		}
		left -= size
	}

	return nil
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

		v = v / 1000
		rv += 3
	}
}

func (inst *Instance) replAck() {
	log.Infof("repl ack for %s", inst.Addr)

	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		offset := atomic.LoadInt64(&inst.offset)
		cmd := fmt.Sprintf(replConfAckCmdFormatter, getStrLen(offset), offset)
		_, err := inst.bw.WriteString(cmd)
		if err != nil {
			log.Errorf("fail to send repl ack command, connection maybe closed soon")
			return
		}
		err = inst.bw.Flush()
		if err != nil {
			log.Errorf("fail to send repl ack command, connection maybe closed soon")
			return
		}
	}
}

func (inst *Instance) syncRDB(addr string) (err error) {
	log.Infof("start syning rdb for %s", inst.Addr)
	cb := NewProtocolCallbacker(addr)
	rdb := NewRDB(inst.br, cb)
	err = rdb.Sync()
	return
}

// Close the up and down stream
func (inst *Instance) Close() {
	if inst.conn != nil {
		inst.conn.Close()
	}
	if inst.tconn != nil {
		inst.tconn.Close()
	}
	return
}
