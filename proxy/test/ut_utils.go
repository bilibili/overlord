package proxy

import (
    // "log"
    "bytes"
    "fmt"
    "strings"
    "net"
    "os"
    "os/exec"
    "strconv"
    "time"
    "errors"

	"overlord/pkg/log"
)

var (
    ErrWriteFail = "write failed"
    ErrReadFail = "read failed"
    ErrNotFound = "key_not_found"
)

type RedisConn struct {
    ServerAddr string
    TimeoutInSeconds int
    conn net.Conn
    readBuf []byte
    hasConn bool
    autoReconn bool
}

func NewRedisConn(addr string) *RedisConn {
    var conn = &RedisConn{ServerAddr:addr, hasConn:false, autoReconn: false}
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
    r.conn, err = net.DialTimeout("tcp", r.ServerAddr, time.Duration(r.TimeoutInSeconds) * time.Second)
    if (err != nil) {
        return err
    }
    r.hasConn = true
    return nil
}

func (r *RedisConn) Put(key, value string) error {
    // SET key redis\r\n
    if (!r.hasConn && r.autoReconn) {
        var err = r.Connect()
        if err != nil {
            return err
        }
    }
    var req = "SET " + key + " " + value + "\r\n"
    var err = r.write(req)
    if (err != nil) {
        r.hasConn = false
        return err
    }
    var readLen = 0
    readLen, err = r.conn.Read(r.readBuf)
    if err != nil {
        r.hasConn = false
        return errors.New(ErrReadFail)
    }
    if readLen == 0 {
        return  errors.New("put operation return value len:0")
    }
    var respType = r.readBuf[0]
    var msg = r.readBuf[1:readLen - 2]
    if respType == '+' {
        return nil
    }
    if respType == '-' {
        return fmt.Errorf("put operation redis return msg:%s, put cmd:%s", msg, req)
    }
    return fmt.Errorf("put operation redis return msg:%s#%s, put cmd:%s redis:%s", string(respType), msg, req, r.ServerAddr)
}

func (r *RedisConn) Get(key string) (string, error) {
    if (!r.hasConn && r.autoReconn) {
        var err = r.Connect()
        if err != nil {
            return "", err
        }
    }
    var req = "GET " + key + "\r\n"
    var err = r.write(req)
    if (err != nil) {
        r.hasConn = false
        return "", errors.New(ErrWriteFail)
    }
    var readLen = 0
    readLen, err = r.conn.Read(r.readBuf)
    if err != nil {
        r.hasConn = false
        return "", errors.New(ErrReadFail)
    }
    if readLen == 0 {
        var err = errors.New("get operation return value len:0")
        return "", err
    }
    var respType = r.readBuf[0]
    var msg = r.readBuf[1:readLen - 2]
    if respType != '$' {
        var err = fmt.Errorf("get operation redis return msg:%s", msg)
        return "", err
    }
    if msg[0] == '-' && msg[1] == '1' {
        var err = errors.New(ErrNotFound)
        return "", err
    }
    var msgLenStr = ""
    for i := 0; i < len(msg); i++ {
        if (msg[i] == '\r') {
            if (i + 1 >= len(msg) || msg[i + 1] != '\n') {
                var err = fmt.Errorf("get operation redis return invalid msg:%s", msg)
                return "", err
            }
            break
        }
        msgLenStr += string(msg[i])
    }
    var msgLen, _ = strconv.Atoi(msgLenStr)
    if len(msgLenStr)  + 2 + msgLen != len(msg) {
        var err = fmt.Errorf("get operation redis return msg:%s", msg)
        return "", err
    }
    return string(msg[len(msgLenStr) + 2 :]), nil
}

func ParseClientCnt(msg string) int {
    msg = strings.Replace(msg, "\r\n", "\n", -1)
    var msgList = strings.Split(msg, "\n")
    for i := 0; i < len(msgList); i++ {
        var one = msgList[i]
        if (strings.HasPrefix(one, "connected_clients:")) {
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
    if (err != nil) {
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
    var msg = r.readBuf[:readLen - 2]
    return string(msg), nil
}
func (r *RedisConn) write(req string) (error) {
    for {
        var byteArray = []byte(req)
        var writeLen, err = r.conn.Write(byteArray)
        if (err != nil) {
            return errors.New(ErrWriteFail)
        }
        if writeLen == len(byteArray) {
            break
        }
        req = req[writeLen:]
    }
    return nil
}

func ExecCmd(cmdStr string) (string, error){
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

func KillAllRedis() error {
    var cmd = "ps aux |grep redis-server  | grep -v grep  | awk '{print $2}' | xargs -n 1 kill -9"
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
