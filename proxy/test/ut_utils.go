package proxy

import (
	// "log"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	// libnet "overlord/pkg/net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"overlord/pkg/log"
)

var (
	ErrWriteFail   = "write failed"
	ErrReadFail    = "read failed"
	ErrNotFound    = "key_not_found"
	StartSeqNO     = 0
	RedisServer    = 0
	MemcacheServer = 1
)

type CliConn interface {
	Connect() error
	Close()
	Get(key string) (string, error)
	Put(key, value string) error
}

type RedisConn struct {
	ServerAddr       string
	TimeoutInSeconds int
	conn             net.Conn
	readBuf          []byte
	hasConn          bool
	autoReconn       bool
}

func NewRedisConn(addr string) *RedisConn {
	var conn = &RedisConn{ServerAddr: addr, hasConn: false, autoReconn: false}
	conn.readBuf = make([]byte, 10240, 20480)
	return conn
}

func notFound(e error) bool {
	var msg = e.Error()
	if strings.Contains(msg, ErrNotFound) {
		return true
	}
	return false
}

func readWriteFail(e error) bool {
	var msg = e.Error()
	if strings.Contains(msg, ErrWriteFail) {
		return true
	}
	if strings.Contains(msg, ErrReadFail) {
		return true
	}
	return false
}

func (r *RedisConn) Connect() error {
	var err error
	r.conn, err = net.DialTimeout("tcp", r.ServerAddr, time.Duration(r.TimeoutInSeconds)*time.Second)
	if err != nil {
		return err
	}
	r.hasConn = true
	return nil
}

func (r *RedisConn) Close() {
	if !r.hasConn {
		return
	}
	r.conn.Close()
	r.hasConn = false
}

func (r *RedisConn) Put(key, value string) error {
	// SET key redis\r\n
	if !r.hasConn && r.autoReconn {
		var err = r.Connect()
		if err != nil {
			return err
		}
	}
	var req = "SET " + key + " " + value + "\r\n"
	var err = r.write(req)
	if err != nil {
		r.hasConn = false
		return err
	}
	var msgLen = 0
	var result = make([]byte, 0, 1024)
	for {
		var readLen = 0
		readLen, err = r.conn.Read(r.readBuf)
		if err != nil {
			r.hasConn = false
			return errors.New(ErrReadFail + " connection return:" + err.Error())
		}
		if readLen == 0 {
			return errors.New("put operation return value len:0")
		}
		result = append(result, r.readBuf[:readLen]...)
		msgLen += readLen
		if result[msgLen-1] == '\n' {
			break
		}
	}
	if msgLen < 4 {
		return errors.New("invalid response:" + strconv.Quote(string(result[:msgLen])))
	}
	var respType = result[0]
	if respType == '+' {
		return nil
	}
	var msg = string(result[1 : msgLen-2])
	// MOVED 6233 127.0.0.1:7001
	if strings.HasPrefix(msg, "MOVED") {
		msg = strings.Replace(msg, "\r\n", "\n", -1)
		msg = strings.TrimSuffix(msg, "\n")
		var msgList = strings.Split(msg, " ")
		var newAddr = msgList[2]
		var newCli = NewRedisConn(newAddr)
		newCli.autoReconn = true
		return newCli.Put(key, value)
	}
	var respMsgStr = strconv.Quote(string(result[:msgLen]))
	if respType == '-' {
		return fmt.Errorf("put operation redis return msg:%s, put cmd:%s", respMsgStr, req)
	}
	return fmt.Errorf("put operation redis return msg:%s#%s, put cmd:%s redis:%s", string(respType), respMsgStr, req, r.ServerAddr)
}

func (r *RedisConn) Get(key string) (string, error) {
	if !r.hasConn && r.autoReconn {
		var err = r.Connect()
		if err != nil {
			return "", err
		}
	}
	var req = "GET " + key + "\r\n"
	var err = r.write(req)
	if err != nil {
		r.hasConn = false
		return "", errors.New(ErrWriteFail)
	}
	var (
		result          = make([]byte, 0, 1024)
		hasReadHead     = false
		dataLen         = -1
		msgLen          = 0
		lenStr          = ""
		expectReadLen   = 0
		valueStartIndex = 0
		useRN           = false
	)
	for {
		var readLen, err = r.conn.Read(r.readBuf)
		if err != nil {
			fmt.Printf("failed to read due to error:%s\n", err.Error())
			r.hasConn = false
			return "", errors.New(ErrReadFail)
		}
		if readLen == 0 {
			var err = fmt.Errorf("get operation redis return empty msg")
			return "", err
		}
		if !hasReadHead {
			var respType = r.readBuf[0]
			if respType != '$' {
				var msg = string(r.readBuf[:readLen])
				var err = fmt.Errorf("get operation redis return msg:%s", msg)
				return "", err
			}
			hasReadHead = true
			result = append(result, r.readBuf[:readLen]...)
		} else {
			result = append(result, r.readBuf[:readLen]...)
		}
		//log.Infof("read from redis, return msg len:%d, msg:%s result:%s \n", readLen,
		//strconv.Quote(string(r.readBuf[:readLen])),
		//strconv.Quote(string(result)))
		msgLen += readLen
		if dataLen < 0 {
			for i := 1; i < msgLen; i++ {
				// log.Infof("start to process:%c\n", result[i])
				if result[i] == '\n' {
					if result[i-1] == '\r' {
						lenStr = string(result[1 : i-1])
						// log.Infof("try to parse len str:%s\n", strconv.Quote(lenStr))
						var msgLenInt, _ = strconv.Atoi(lenStr)
						dataLen = msgLenInt
						expectReadLen = i + dataLen + 2
						useRN = true
					} else {
						lenStr = string(result[1:i])
						// log.Infof("try to parse len str:%s\n", strconv.Quote(lenStr))
						var msgLenInt, _ = strconv.Atoi(lenStr)
						dataLen = msgLenInt
						expectReadLen = i + dataLen + 1
					}
					valueStartIndex = i + 1
					if dataLen < 0 {
						return "", errors.New(ErrNotFound)
					}
					// log.Infof("parse get data len:%d, has read len:%d expectReadLen:%d\n", dataLen, msgLen, expectReadLen)
					break
				}
			}
		}
		if expectReadLen > 0 && msgLen >= expectReadLen {
			break
		}
	}
	var content = result[valueStartIndex : msgLen-1]
	if useRN {
		content = result[valueStartIndex : msgLen-2]
	}
	return string(content), nil
}

func ParseRedisClientCnt(msg string) int {
	msg = strings.Replace(msg, "\r\n", "\n", -1)
	var msgList = strings.Split(msg, "\n")
	for i := 0; i < len(msgList); i++ {
		var one = msgList[i]
		if strings.HasPrefix(one, "connected_clients:") {
			var cntStr = strings.Split(one, ":")[1]
			var cnt, _ = strconv.Atoi(cntStr)
			return cnt
		}
	}
	log.Infof("failed to parse connected_clients from resp msg:%s\n", msg)
	return -1
}

func (r *RedisConn) GetInfo() (string, error) {
	var req = "INFO\r\n"
	var err = r.write(req)
	if err != nil {
		return "", err
	}
	var readLen = 0
	readLen, err = r.conn.Read(r.readBuf)
	if err != nil {
		return "", err
	}
	if readLen == 0 {
		var err = errors.New("info operation return value len:0")
		return "", err
	}
	var msg = r.readBuf[:readLen-2]
	return string(msg), nil
}
func (r *RedisConn) write(req string) error {
	for {
		var byteArray = []byte(req)
		var writeLen, err = r.conn.Write(byteArray)
		if err != nil {
			return errors.New(ErrWriteFail)
		}
		if writeLen == len(byteArray) {
			break
		}
		req = req[writeLen:]
	}
	return nil
}

type MCRequestHeader struct {
	magic    uint8
	opcode   uint8
	keylen   uint16
	extlen   uint8
	datatype uint8
	reserved uint16
	bodylen  uint32
	opaque   uint32
	cas      uint64
}

type MCResponseHeader struct {
	magic    uint8
	opcode   uint8
	keylen   uint16
	extlen   uint8
	datatype uint8
	status   uint16
	bodylen  uint32
	opaque   uint32
	cas      uint64
}

type MCSetHeader struct {
	head   MCRequestHeader
	flag   uint32
	expire uint32
}

type MemcacheConn struct {
	ServerAddr       string
	TimeoutInSeconds int
	conn             net.Conn
	readBuf          []byte
	hasConn          bool
	autoReconn       bool
	binary           bool
}

func NewMemcacheConn(addr string) *MemcacheConn {
	var conn = &MemcacheConn{ServerAddr: addr, hasConn: false, autoReconn: false, binary: false}
	conn.readBuf = make([]byte, 10240, 20480)
	return conn
}

func (m *MemcacheConn) Connect() error {
	var err error
	m.conn, err = net.DialTimeout("tcp", m.ServerAddr, time.Duration(m.TimeoutInSeconds)*time.Second)
	if err != nil {
		return err
	}
	m.hasConn = true
	return nil
}

func (m *MemcacheConn) Close() {
	if !m.hasConn {
		return
	}
	m.conn.Close()
	m.hasConn = false
}

func (m *MCRequestHeader) serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, m.magic)
	binary.Write(buf, binary.LittleEndian, m.opcode)
	binary.Write(buf, binary.BigEndian, m.keylen)
	binary.Write(buf, binary.LittleEndian, m.extlen)
	binary.Write(buf, binary.LittleEndian, m.datatype)
	binary.Write(buf, binary.LittleEndian, m.reserved)
	binary.Write(buf, binary.BigEndian, m.bodylen)
	binary.Write(buf, binary.LittleEndian, m.opaque)
	binary.Write(buf, binary.BigEndian, m.cas)
	fmt.Printf("% x\n", buf.Bytes())
	var ret = buf.Bytes()
	fmt.Printf("mc header len:%d\n", len(ret))
	return ret
}

func (m *MCSetHeader) serialize() []byte {
	var data1 = m.head.serialize()
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, data1)
	binary.Write(buf, binary.BigEndian, m.flag)
	binary.Write(buf, binary.BigEndian, m.expire)
	fmt.Printf("set head:% x\n", buf.Bytes())
	// var data2 = buf.Bytes()
	// var data = append(data1, data2)
	var ret = buf.Bytes()
	fmt.Printf("mc set header len:%d\n", len(ret))
	return ret
}

func (m *MemcacheConn) BPut(key, value string) error {
	if !m.hasConn && m.autoReconn {
		var err = m.Connect()
		if err != nil {
			return err
		}
	}
	var reqHeader = MCSetHeader{}
	reqHeader.head.magic = 0x80
	reqHeader.head.opcode = 0x01 // SET
	reqHeader.head.keylen = uint16(len(key))
	reqHeader.head.extlen = 8
	reqHeader.head.bodylen = uint32(len(key) + 8 + len(value))
	reqHeader.head.reserved = 0
	reqHeader.head.opaque = 0xdeadbeef
	reqHeader.flag = 0
	reqHeader.expire = 0

	var headByte = reqHeader.serialize()
	var data = key + value

	var oneline = append(headByte, []byte(data)...)
	fmt.Printf("to write data:% x\n", oneline)
	var _, err = m.conn.Write(oneline)
	if err != nil {
		m.hasConn = false
		return err
	}
	fmt.Printf("write head, key, value done\n")
	var readLen = 0
	var respHead = MCResponseHeader{}
	var expectLen = int(unsafe.Sizeof(respHead))
	fmt.Printf("expect to read header len:%d\n", expectLen)
	// var respBuf = make([]byte, expectLen)
	// readLen, err = m.conn.Read(respBuf)
	readLen, err = m.conn.Read(m.readBuf)
	if err != nil {
		m.hasConn = false
		return errors.New(ErrReadFail)
	}
	if readLen != expectLen {
		return errors.New("mem binary put operation return value len:" + strconv.Itoa(readLen) + " not as expect:" + strconv.Itoa(expectLen))
	}
	// respHead.keylen = binary.LittleEndian.Uint16(respBuf[2:4])
	// respHead.status = binary.LittleEndian.Uint16(respBuf[6:8])
	// respHead.bodylen = binary.LittleEndian.Uint32(respBuf[8:12])
	fmt.Printf("get response boy len:%d\n", int(respHead.bodylen))

	// var body = make([]byte, respHead.bodylen, respHead.bodylen)
	// readLen, err = m.conn.Read(body)
	return nil
}

func (m *MemcacheConn) Put(key, value string) error {
	// SET key flags ttl len\r\ndata
	if !m.hasConn && m.autoReconn {
		var err = m.Connect()
		if err != nil {
			return err
		}
	}
	var req = "set " + key + " 0 0 " + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
	var err = m.write(req)
	if err != nil {
		m.hasConn = false
		return err
	}
	var result = make([]byte, 0, 1024)
	var msgLen = 0
	for {
		var readLen = 0
		readLen, err = m.conn.Read(m.readBuf)
		if err != nil {
			m.hasConn = false
			return errors.New(ErrReadFail)
		}
		if readLen == 0 {
			return errors.New("mem put operation return value len:0")
		}
		result = append(result, m.readBuf[:readLen]...)
		msgLen += readLen
		if result[msgLen-1] == '\n' {
			break
		}
	}
	var returnVal = string(result[:msgLen])
	returnVal = strings.Replace(returnVal, " ", "", -1)
	returnVal = strings.Replace(returnVal, "\r\n", "\n", -1)
	var msgList = strings.Split(returnVal, "\n")
	if len(msgList) == 0 {
		return errors.New("mem put operation return invalid msg:" + returnVal)
	}
	if msgList[0] == "STORED" {
		return nil
	}
	var cmd = strconv.Quote(string(req))
	if strings.Contains(msgList[0], "SERVER_ERROR") {
		return fmt.Errorf("failed to put to memcache, get server error return msg:[%s], put cmd:%s server:%s", msgList[0], cmd, m.ServerAddr)
	}
	return fmt.Errorf("failed to put to memcache, return msg:[%s], put cmd:%s server:%s", msgList[0], cmd, m.ServerAddr)
}

func (m *MemcacheConn) Get(key string) (string, error) {
	if !m.hasConn && m.autoReconn {
		var err = m.Connect()
		if err != nil {
			return "", err
		}
	}
	// get key1
	// VALUE key1 0 5
	// 12345
	var req = "get " + key + "\r\n"
	var err = m.write(req)
	if err != nil {
		m.hasConn = false
		return "", errors.New(ErrWriteFail)
	}
	var result = make([]byte, 0, 1024)
	var msgLen = 0
	for {
		var readLen = 0
		readLen, err = m.conn.Read(m.readBuf)
		if err != nil {
			m.hasConn = false
			return "", errors.New(ErrReadFail)
		}
		if readLen == 0 {
			var err = errors.New("get operation return value len:0")
			return "", err
		}
		msgLen += readLen
		result = append(result, m.readBuf[:readLen]...)
		if result[msgLen-1] == '\n' {
			break
		}
	}
	var respMsg = string(result[:msgLen])
	// log.Infof("memcache client get resp:%s from addr:%s\n", strconv.Quote(respMsg), m.ServerAddr)
	respMsg = strings.Replace(respMsg, "\r\n", "\n", -1)
	respMsg = strings.TrimSuffix(respMsg, "\n")
	respMsg = strings.TrimSuffix(respMsg, " ")
	var msgList = strings.Split(respMsg, "\n")
	if len(msgList) == 1 {
		if msgList[0] == "END" {
			return "", nil
		}
		var err = fmt.Errorf("get operation memcache return unknown msg:%s", msgList[0])
		return "", err
	}
	if len(msgList) != 3 {
		var msgStr = strconv.Quote(respMsg)
		var err = fmt.Errorf("get operation memcache return unexpected msg:%s, msglen:%d\n", msgStr, msgLen)
		return "", err
	}
	return msgList[1], nil
}

func ParseMemcacheClientCnt(msg string) int {
	msg = strings.Replace(msg, "\r\n", "\n", -1)
	var msgList = strings.Split(msg, "\n")
	for i := 0; i < len(msgList); i++ {
		// STAT curr_connections 1
		var one = msgList[i]
		if strings.HasPrefix(one, "STAT curr_connections") {
			var cntStr = strings.Split(one, " ")[2]
			var cnt, _ = strconv.Atoi(cntStr)
			return cnt
		}
	}
	log.Infof("failed to parse memcache connection count from resp msg:%s\n", msg)
	return -1
}

func (m *MemcacheConn) GetInfo() (string, error) {
	var req = "stats\r\n"
	var err = m.write(req)
	if err != nil {
		return "", err
	}
	var readLen = 0
	readLen, err = m.conn.Read(m.readBuf)
	if err != nil {
		return "", err
	}
	if readLen == 0 {
		var err = errors.New("stats operation return value len:0")
		return "", err
	}
	var msg = string(m.readBuf[0:readLen])
	msg = strings.Replace(msg, "\r\n", "\n", -1)
	return string(msg), nil
}

func (m *MemcacheConn) writeByte(byteArray []byte) error {
	var hasWrite = 0
	for {
		var writeLen, err = m.conn.Write(byteArray[hasWrite:])
		if err != nil {
			return errors.New(ErrWriteFail)
		}
		if (writeLen + hasWrite) == len(byteArray) {
			fmt.Printf("has write:%d\n", len(byteArray))
			return nil
		}
		hasWrite += writeLen
	}
	return nil
}

func (m *MemcacheConn) write(req string) error {
	for {
		var byteArray = []byte(req)
		var writeLen, err = m.conn.Write(byteArray)
		if err != nil {
			return errors.New(ErrWriteFail)
		}
		if writeLen == len(byteArray) {
			break
		}
		req = req[writeLen:]
	}
	return nil
}

func ExecCmd(cmdStr string) (string, error) {
	// fmt.Printf("try to exec cmd:%s\n", cmdStr)
	cmd := exec.Command("/bin/bash", "-c", cmdStr)

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	var msg = out.String()
	// fmt.Printf("exec cmd get ret:%s\n", msg)
	return msg, err
}

var gStandAloneConfBase = `bind 127.0.0.1 ::1
protected-mode yes
tcp-backlog 511
timeout 0
tcp-keepalive 300
daemonize yes
supervised no
pidfile /var/run/redis/redis-server.pid
loglevel debug
databases 16
always-show-logo yes
save ""
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
dbfilename dump.rdb
slave-serve-stale-data yes
slave-read-only yes
repl-diskless-sync yes
repl-diskless-sync-delay 5
repl-disable-tcp-nodelay no
slave-priority 100
lazyfree-lazy-eviction no
lazyfree-lazy-expire no
lazyfree-lazy-server-del no
slave-lazy-flush no
appendonly no
appendfilename "appendonly.aof"
appendfsync no
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
aof-load-truncated yes
aof-use-rdb-preamble no
lua-time-limit 5000
slowlog-log-slower-than 10000
slowlog-max-len 128
latency-monitor-threshold 0
notify-keyspace-events ""
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-size -2
list-compress-depth 0
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
hll-sparse-max-bytes 3000
activerehashing yes
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit slave 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60
hz 10
aof-rewrite-incremental-fsync yes`

func GenRedisStandAlone(confName, port, logPath string) error {
	var conf = gStandAloneConfBase + "\n" + "port " + port + "\n" + "logfile " + logPath + "\n"
	var fd, err1 = os.Create(confName)
	if err1 != nil {
		return err1
	}
	defer fd.Close()
	var _, err2 = fd.WriteString(conf)
	if err2 != nil {
		return err2
	}
	fd.Sync()
	return nil
}

func StartStandAloneRedis(confName, port, logPath string) error {
	var err1 = GenRedisStandAlone(confName, port, logPath)
	if err1 != nil {
		return err1
	}
	var cmd = "redis-server " + confName + " &"
	var _, err2 = ExecCmd(cmd)
	return err2
}

func StartStandAloneMC(port string) error {
	StartSeqNO++
	// var cmd = "/usr/bin/memcached -d -m 64 -p " +  port + " -l 127.0.0.1  -P /tmp/memcached1." + port + "." + strconv.Itoa(StartSeqNO) + " logfile " + logPath + " &"
	var cmd = "/usr/bin/memcached -d -m 64 -p " + port + " -l 127.0.0.1  -P /tmp/memcached1." + port + "." + strconv.Itoa(StartSeqNO) + " &"
	// fmt.Printf("try to exec cmd:%s\n", cmd)
	var _, err2 = ExecCmd(cmd)
	return err2
}

func KillAllRedis() error {
	var cmd = "ps aux |grep redis-server |grep -v 6379 |grep -v cluster | grep -v grep  | awk '{print $2}' | xargs -n 1 kill -9"
	var _, err = ExecCmd(cmd)
	return err
}

func KillAllMC() error {
	var cmd = "ps aux |grep memcache |grep -v 11211| grep -v grep  | awk '{print $2}' | xargs -n 1 kill -9"
	var _, err = ExecCmd(cmd)
	return err
}

type RMServer struct {
	connectionCnt int32
	port          string
	state         int32
	kvStore       map[string]string
	mutex         sync.Mutex
	listener      net.Listener
	serverType    int
}

func NewRedisServer() *RMServer {
	var s = &RMServer{serverType: RedisServer}
	s.kvStore = make(map[string]string)
	s.connectionCnt = 0
	return s
}

func NewMemServer() *RMServer {
	var s = &RMServer{serverType: MemcacheServer}
	s.kvStore = make(map[string]string)
	s.connectionCnt = 0
	return s
}

func (s *RMServer) start() error {
	var addr = "0.0.0.0:" + s.port
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go s.accept()
	return nil
}

func (s *RMServer) stop() {
	atomic.StoreInt32(&s.state, -1)
	s.listener.Close()
}

func (s *RMServer) accept() {
	for {
		if atomic.LoadInt32(&s.state) != 0 {
			s.listener.Close()
			return
		}
		conn, err := s.listener.Accept()
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			log.Errorf("addr:0.0.0.0:%s accept connection error:%+v", s.port, err)
			continue
		}
		atomic.AddInt32(&s.connectionCnt, 1)
		if s.serverType == RedisServer {
			go s.processRedis(conn)
		} else {
			go s.processMemcache(conn)
		}
	}
}

func (s *RMServer) handleGetCmd(input string) (error, string) {
	// "*2\r\n$3\r\nGET\r\n$13\r\nkey_loop_get2\r\n"
	input = strings.Replace(input, "\r\n", "\n", -1)
	var cmdList = strings.Split(input, "$")
	if cmdList[0][0] != '*' {
		return errors.New("invalid request" + strconv.Quote(input) + ", not start with *"), ""
	}
	var cmd = strings.Split(cmdList[1], "\n")[1]
	if cmd != "GET" {
		return errors.New("request" + strconv.Quote(input) + " not get request"), ""
	}
	var key = strings.Split(cmdList[2], "\n")[1]
	s.mutex.Lock()
	var val, ok = s.kvStore[key]
	s.mutex.Unlock()
	var resp = ""
	if ok {
		resp = "$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"
	} else {
		resp = "$-1\r\n"
	}
	return nil, resp
}

func (s *RMServer) handleSetCmd(input string) error {
	// "*3\r\n   $3\r\nSET\r\n  $5\r\nmykey\r\n $7\r\nmyvalue\r\n"
	input = strings.Replace(input, "\r\n", "\n", -1)
	var cmdList = strings.Split(input, "$")
	if cmdList[0][0] != '*' {
		return errors.New("invalid request" + strconv.Quote(input) + ", not start with *")
	}
	var cmd = strings.Split(cmdList[1], "\n")[1]
	if cmd != "SET" {
		return errors.New("request" + strconv.Quote(input) + " not set request")
	}
	var key = strings.Split(cmdList[2], "\n")[1]
	var value = strings.Split(cmdList[3], "\n")[1]
	s.mutex.Lock()
	s.kvStore[key] = value
	s.mutex.Unlock()
	return nil
}

func countDollarCnt(data []byte, length int) int {
	var cnt = 0
	for i := 0; i < length; i++ {
		if data[i] == '$' {
			cnt++
		}
	}
	return cnt
}

func (s *RMServer) processRedis(conn net.Conn) {
	for {
		var databuf = make([]byte, 0, 1024)
		if atomic.LoadInt32(&s.state) != 0 {
			conn.Close()
			atomic.AddInt32(&s.connectionCnt, -1)
			return
		}
		var msgLen = 0
		for {
			var buf = make([]byte, 1024, 10240)
			var readLen, err = conn.Read(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				conn.Close()
				atomic.AddInt32(&s.connectionCnt, -1)
				log.Errorf("redis server failed to read from client, get error:%s\n", err.Error())
				return
			}
			if readLen == 0 {
				continue
			}
			msgLen += readLen
			databuf = append(databuf, buf[:readLen]...)
			if databuf[0] == '*' {
				if msgLen <= 1 {
					continue
				}
				var cntStr = string(databuf[1])
				var cnt, err = strconv.Atoi(cntStr)
				if err != nil {
					conn.Close()
					atomic.AddInt32(&s.connectionCnt, -1)
					log.Errorf("invalid request format, request:%s\n", strconv.Quote(string(databuf[:msgLen])))
					return
				}
				if countDollarCnt(databuf, msgLen) != cnt {
					continue
				}
				break
			} else if databuf[msgLen-1] == '\n' {
				break
			}
		}
		// fmt.Printf("recv msg:%s\n", strconv.Quote(string(databuf[:msgLen])))
		{
			var result = databuf[:msgLen]
			var cmd = string(result[0:3])
			cmd = strings.ToUpper(cmd)
			var resp = ""
			if cmd == "GET" {
				var msg = strings.Replace(string(result[:msgLen]), "\r", "", -1)
				msg = strings.Replace(msg, "\n", "", -1)
				var msgList = strings.Split(msg, " ")
				var key = msgList[1]
				s.mutex.Lock()
				var val, ok = s.kvStore[key]
				s.mutex.Unlock()
				if ok {
					resp = "$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"
				} else {
					resp = "$-1\r\n"
				}
			} else if cmd == "SET" {
				var msg = strings.Replace(string(result[:msgLen]), "\r", "", -1)
				msg = strings.Replace(msg, "\n", "", -1)
				var msgList = strings.Split(msg, " ")
				var key = msgList[1]
				var val = msgList[2]
				s.mutex.Lock()
				s.kvStore[key] = val
				s.mutex.Unlock()
				resp = "+OK\r\n"
			} else if cmd == "INF" {
				var cnt = atomic.LoadInt32(&s.connectionCnt)
				resp = "connected_clients:" + strconv.Itoa(int(cnt)) + "\r\n"
			} else if cmd[0] == '*' {
				var reqStr = string(databuf[:msgLen])
				if cmd[1] == '3' {
					var err = s.handleSetCmd(reqStr)
					if err != nil {
						conn.Close()
						atomic.AddInt32(&s.connectionCnt, -1)
						log.Errorf("failed to process cmd, get err:%s\n", err.Error())
						return
					}
					resp = "+OK\r\n"
				} else if cmd[1] == '2' {
					var err, getResp = s.handleGetCmd(reqStr)
					if err != nil {
						conn.Close()
						atomic.AddInt32(&s.connectionCnt, -1)
						log.Errorf("failed to process cmd, get err:%s\n", err.Error())
						return
					}
					resp = getResp
				}
			} else {
				log.Errorf("unknown cmd:%s, close client connection\n", cmd)
				conn.Close()
				atomic.AddInt32(&s.connectionCnt, -1)
				return
			}
			var byteArray = []byte(resp)
			var _, err = conn.Write(byteArray)
			if err != nil {
				log.Errorf("failed to write resp:%s to client, got err:%s\n", strconv.Quote(resp), err.Error())
				conn.Close()
				atomic.AddInt32(&s.connectionCnt, -1)
				return
			}
		}
	}
}

func (s *RMServer) processMemcache(conn net.Conn) {
	for {
		var databuf = make([]byte, 0, 1024)
		if atomic.LoadInt32(&s.state) != 0 {
			conn.Close()
			atomic.AddInt32(&s.connectionCnt, -1)
			return
		}
		var msgLen = 0
		for {
			var buf = make([]byte, 1024, 10240)
			var readLen, err = conn.Read(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Infof("memcache server, read time out, just continue\n")
					continue
				}
				conn.Close()
				atomic.AddInt32(&s.connectionCnt, -1)
				log.Errorf("memcache server failed to read from client, get error:%s\n", err.Error())
				return
			}
			// log.Infof("memcache server read msg len:%d\n", readLen)
			msgLen += readLen
			databuf = append(databuf, buf[:readLen]...)
			if databuf[msgLen-1] == '\n' {
				break
			}
		}
		// log.Infof("memcache recv msg:%s\n", strconv.Quote(string(databuf[:msgLen])))
		{
			var result = databuf[:msgLen]
			var reqStr = strings.Replace(string(result), "\r", "", -1)
			var argList = strings.Split(reqStr, "\n")

			var cmds = strings.Split(string(argList[0]), " ")
			var cmd = cmds[0]
			cmd = strings.ToUpper(cmd)
			var resp = ""
			if cmd == "GET" {
				var key = cmds[1]
				s.mutex.Lock()
				var val, ok = s.kvStore[key]
				s.mutex.Unlock()
				if ok {
					resp = "VALUE " + key + " 0 " + strconv.Itoa(len(val)) + "\r\n" + val + "\r\nEND\r\n"
				} else {
					resp = "END\r\n"
				}
			} else if cmd == "SET" {
				var body = ""
				if len(argList) >= 2 {
					body = argList[1]
				}
				var key = cmds[1]
				s.mutex.Lock()
				s.kvStore[key] = body
				s.mutex.Unlock()
				resp = "STORED\r\n"
			} else if cmd == "STATS" {
				var cnt = atomic.LoadInt32(&s.connectionCnt)
				resp = "STAT connected_clients:" + strconv.Itoa(int(cnt)) + "\r\n"
			} else {
				log.Errorf("unknown cmd:%s, close client connection\n", cmd)
				conn.Close()
				atomic.AddInt32(&s.connectionCnt, -1)
				return
			}
			var byteArray = []byte(resp)
			var _, err = conn.Write(byteArray)
			if err != nil {
				log.Errorf("mecache server failed to write resp:%s to client, got err:%s\n", strconv.Quote(resp), err.Error())
				conn.Close()
				atomic.AddInt32(&s.connectionCnt, -1)
				return
			}
			// log.Infof("succeed to echo memcache resp:%s\n", strconv.Quote(resp))
		}
	}
}
