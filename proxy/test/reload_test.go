package proxy

import (
	"sync/atomic"
	"os"
	"testing"
	"time"
	"fmt"
    "strconv"

    "overlord/proxy"
	"overlord/pkg/log"

	"github.com/stretchr/testify/assert"
)

func DumpClusterConf(fileName string, confs []*proxy.ClusterConfig) error {
    var fullContent = ""
    for index := 0; index < len(confs); index++ {
        var conf = confs[index]
        var content = "[[clusters]]\n"
        content += "name = \"" + conf.Name + "\"\n"
        content += "hash_method = \"" + conf.HashMethod + "\"\n"
        content += "hash_distribution = \"" +  conf.HashDistribution + "\"\n"
        content += "hash_tag = \"" + conf.HashTag + "\"\n"
        content += "cache_type = \"" + string(conf.CacheType) + "\"\n"
        content += "listen_proto = \"" + conf.ListenProto + "\"\n"
        content += "listen_addr = \"" + conf.ListenAddr  + "\"\n"
        content += "redis_auth = \"" + conf.RedisAuth + "\"\n"
        content += "dial_timeout = " + strconv.FormatInt(int64(conf.DialTimeout), 10) + "\n"
        content += "read_timeout = " + strconv.FormatInt(int64(conf.ReadTimeout), 10) + "\n"
        content += "write_timeout = " + strconv.FormatInt(int64(conf.WriteTimeout), 10) + "\n"
        content += "node_connections = " + strconv.FormatInt(int64(conf.NodeConnections), 10) + "\n"
        content += "ping_fail_limit = " + strconv.FormatInt(int64(conf.PingFailLimit), 10) + "\n"
        if conf.PingAutoEject {
            content += "ping_auto_eject = true\n"
        } else {
            content += "ping_auto_eject = false\n"
        }
        if conf.CloseWhenChange {
            content += "close_front_conn_when_conf_change = true\n"
        } else {
            content += "close_front_conn_when_conf_change = false\n"
        }
        content += "servers = [\n"
        for i := 0; i < len(conf.Servers); i++ {
            content += "    \"" + conf.Servers[i] + "\",\n"
        }
        content += "]\n"
        fullContent += content
    }
    var fd, err1 = os.Create(fileName)
    if err1 != nil {
        return err1
    }
    defer fd.Close()
    var _, err2 = fd.WriteString(fullContent)
    if err2 != nil {
        return err2
    }
    fd.Sync()
    return nil
}

func LoopCheck(addr string, ch chan int) {
    var cli = NewRedisConn(addr)
    var err = cli.Connect()
    if err != nil {
        log.Errorf("failed to connect to redis, get error:%s\n", err.Error())
        return
    }
    var cnt int = 0
    log.Info("start to check redis conn cnt")
    for {
        select {
        case <- ch:
            log.Info("recv message from channel, exit now")
            return
        default:
            var msg = ""
            msg, err = cli.GetInfo()
            if err != nil {
                log.Errorf("failed to get info from redis, get error:%s\n", err.Error())
                return
            }
            var connCnt = ParseClientCnt(msg)
            if (connCnt != 1) {
                if (cnt % 20 == 19) {
                    log.Infof("get redis:%s connect count:%d\n", addr, connCnt)
                }
            }
            cnt++
            time.Sleep(time.Second * 1)
        }
    }
}

func LoopGet(addr string, ch chan int) {
    // var client = NewRedisConn(addr)
    log.Info("try to loop get")
    var key = "key_loop_get"
    var val = "loop_get_value"
    var cli = NewRedisConn(addr)
    var err = cli.Connect()
    if err != nil {
        log.Errorf("failed to connect to redis, get error:%s\n", err.Error())
        return
    }
    err = cli.Put(key, val)
    if err != nil {
        log.Errorf("failed to put kv to redis, get error:%s\n", err.Error())
        return
    }
    for {
        select {
        case <- ch:
            log.Info("loop get recv message from channel, exit now")
            return
        default:
            log.Info("loop get now")
            // var msg = ""
            var changed = atomic.LoadInt32(&proxy.GClusterCount)
            var _, err = cli.Get(key)
            if err != nil {
                if changed == 1 {
                    log.Infof("detect cluster changed, got error:%s", err.Error())
                    ch <- 1
                } else {
                    log.Infof("cluster is not changed, but got error:%s", err.Error())
                    ch <- -1
                }
                return
            }
        }
    }
}

func UpdateConf(intervalInSec int , addrs []string, fileName string, base []*proxy.ClusterConfig, ch chan int) {
    var confs = make([]*proxy.ClusterConfig, len(base), len(base))
    for i := 0; i < len(confs); i++ {
        confs[i] = &proxy.ClusterConfig{}
        *(confs[i]) = *(base[i])
    }
    var index = 0
    var cnt = len(addrs)
    for {
        select {
        case <- ch:
            log.Info("Update conf recv message from channel, exit now")
            return
        default:
            if index >= cnt {
                return
            }
            var addr = addrs[index]
            index++
            var addrList = make([]string, 1, 1)
            addrList[0] = addr
            copy(confs[0].Servers, addrList)
            log.Infof("start dump cluster conf:%s for %d times\n", fileName, index)
            var err = DumpClusterConf(fileName, confs)
            if err != nil {
                log.Infof("failed to dump cluster conf with error:%s\n", err.Error())
                return
            }
            time.Sleep(time.Duration(intervalInSec) * time.Second)
        }
    }
}

var gProxyConfFile = "./conf/proxy.conf"
var gClusterConfFile = "./conf/cluster.conf"

func initClusterRedisConf(t *testing.T, conf* proxy.ClusterConfigs) {
    var err = conf.LoadFromFile(gClusterConfFile)
	assert.NoError(t, err)
}

func TestClusterConfigLoadFromFile(t *testing.T) {
    var cmd = "cp conf/cluster_init.conf " + gClusterConfFile
    ExecCmd(cmd)
    proxy.MonitorCfgIntervalSecs = 1
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
	succ, msg, clusterConfs := proxy.LoadClusterConf(gClusterConfFile)
    if !succ {
        log.Infof("failed to load cluster conf, get error:%s\n", msg)
        return
    }
    succ2, msg2, clusterConfs2 := proxy.LoadClusterConf(gClusterConfFile)
    if !succ2 {
        log.Infof("failed to load cluster conf, get error:%s\n", msg2)
        return
    }


    var server, err = proxy.NewProxy(proxyConf)

    server.ClusterConfFile = gClusterConfFile
	assert.NoError(t, err)
    server.Serve(clusterConfs)
    var chGet = make(chan int, 1)
    var chCheck = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr = "127.0.0.1:8101"
    go LoopGet(frontAddr, chGet)
    var addr = "127.0.0.1:8201"
    go LoopCheck(addr, chCheck)
    var addrList = make([]string, 2, 2)
    addrList[0] = "127.0.0.1:8201:2 redis1"
    addrList[1] = "127.0.0.1:8202:2 redis2"
    // addrList[2] = "127.0.0.1:8201:2 redis1"
    // addrList[3] = "127.0.0.1:8202:2 redis2"
    // addrList[4] = "127.0.0.1:8201:2 redis1"
    log.Infof("cluster confs len:%d\n", len(clusterConfs2))
    go UpdateConf(10, addrList, gClusterConfFile, clusterConfs2, chUpdate)
    fmt.Println("start to loop wait")
    for {
        select {
        case getChanged := <-chGet:
            if getChanged == 1 {
                log.Info("get routine detected changed")
            } else {
                log.Errorf("get routine find some error changed")
                return
            }
        default:
            time.Sleep(1 * time.Second)
        }
    }
}
