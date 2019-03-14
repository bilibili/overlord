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
	"strconv"
	"strings"
	"time"
	"unsafe"

	"overlord/pkg/log"
)

var (
	ErrWriteFail = "write failed"
	ErrReadFail  = "read failed"
	ErrNotFound  = "key_not_found"
	StartSeqNO   = 0
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
			return errors.New(ErrReadFail)
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
	respMsg = strings.Replace(respMsg, "\r\n", "\n", -1)
	respMsg = strings.TrimSuffix(respMsg, "\n")
	respMsg = strings.TrimSuffix(respMsg, " ")
	var msgList = strings.Split(respMsg, "\n")
	if len(msgList) == 0 {
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

//func main() {
//    var conf = "/tmp/redis2.conf"
//    var port = "8888"
//    var logFile = "/tmp/redis2.log"
//    var error = StartStandAloneRedis(conf, port, logFile)
//    if (error != nil) {
//        fmt.Println("failed to start redis server")
//    }
//}

// func main() {
//    var error = KillAllRedis()
//    if (error != nil) {
//        fmt.Println("failed to start redis server")
//    }
// }
