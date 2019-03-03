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
	"github.com/stretchr/testify/require"
)

var gSN int = 0

func setup(port1, port2 string) {
    proxy.GClusterSn = 0
    proxy.MonitorCfgIntervalSecs = 1
    proxy.GClusterChangeCount = 0
    proxy.GClusterCount = 0
    KillAllRedis()
    var sn = strconv.Itoa(gSN)
    var redisConf1 = "/tmp/redis_s_" + port1 + "_" + sn + ".conf";
    var redisConf2 = "/tmp/redis_s_" + port2 + "_" + sn + ".conf";
    var err1 = StartStandAloneRedis(redisConf1, port1, redisConf1 + ".log")
    var err2 = StartStandAloneRedis(redisConf2, port2, redisConf2 + ".log")
    if err1 != nil {
        fmt.Printf("failed to start redis1, get error:%s\n", err1.Error())
    }
    if err2 != nil {
        fmt.Printf("failed to start redis1, get error:%s\n", err2.Error())
    }
    gSN++
}
func tearDown() {
    KillAllRedis()
}

func writeToFile(name, content string) error {
    var fd, err1 = os.Create(name)
    if err1 != nil {
        return err1
    }
    defer fd.Close()
    var _, err2 = fd.WriteString(content)
    if err2 != nil {
        return err2
    }
    fd.Sync()
    return nil
}

var fileSn = 0
// func dumpClusterConf(fileName string, confs []*proxy.ClusterConfig) error {
func dumpClusterConf(fileName string, confs proxy.ClusterConfigs) error {
    var fullContent = ""
    for index := 0; index < len(confs.Clusters); index++ {
        var conf = confs.Clusters[index]
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
    var err =  writeToFile(fileName, fullContent)
    // var file2 = fileName + "." + strconv.Itoa(int(fileSn))
    // fileSn++
    // writeToFile(file2, fullContent)
    return err
    /*
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
    */
}

func connentCnt(addr string) int {
    var cli = NewRedisConn(addr)
    var err = cli.Connect()
    if err != nil {
        log.Errorf("failed to connect to redis, get error:%s\n", err.Error())
        return int(-1)
    }
    // var cnt int = 0
    log.Info("start to check redis conn cnt")
    var msg = ""
    msg, err = cli.GetInfo()
    if err != nil {
        log.Errorf("failed to get info from redis, get error:%s\n", err.Error())
        return int(-1)
    }
    var connCnt = ParseClientCnt(msg)
    return int(connCnt)
}

func loopCheck(addr string, ch chan int) {
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

func loopGetToSucc(addr string, key, val string, ch chan int) {
    log.Infof("try to loop get from addr:%s\n", addr)
    var cli = NewRedisConn(addr)
    var hasConn = false
    for {
        select {
        case <- ch:
            log.Info("loop get recv message from channel, exit now")
            return
        default:
            if !hasConn {
                var err = cli.Connect()
                if err == nil {
                    hasConn = true
                } else {
                    time.Sleep(time.Duration(500) * time.Millisecond)
                    continue
                }
            }
            log.Infof("loop get succeed to connect to address:%s\n", addr)
            var _, err = cli.Get(key)
            if err != nil {
                log.Errorf("loop get failed to get from addr:%s got error:%s\n",  addr, err.Error())
                ch <- -1
            } else {
                ch <- 0
            }
            return
        }
    }
}

// changeCnt: configure load count 
// expRcnt: the number that read behaviour is changed, eg: (ok, fail, ok, fail) = 3
// connFailCnt: the number that connection is changed, eg: (ok, fail, ok) = 2
func LoopGet(addr, key, val string, changeCnt int, expRCnt, connFailCnt int, writeFirst bool, ch chan int) {
    log.Infof("try to loop put and get to addr:%s\n", addr)
    var cli = NewRedisConn(addr)
    var err = cli.Connect()
    if err != nil {
        log.Errorf("failed to connect to redis, get error:%s\n", err.Error())
        return
    }
    if (writeFirst) {
        err = cli.Put(key, val)
        if err != nil {
            log.Errorf("failed to put kv to redis, get error:%s\n", err.Error())
            return
        }
        log.Infof("succeed to write key:%s to addr:%s\n", key, addr)
    }
    var prevChangeCnt int32 = 0
    var prevConnSucc = true
    var prevRdSucc = true
    var cnnCloseCnt = 0
    var rChange = 0
    for {
        select {
        case <- ch:
            log.Info("loop get recv message from channel, exit now")
            return
        default:
            // log.Info("loop get now")
            // var msg = ""
            var gotChange = false
            var changed = atomic.LoadInt32(&proxy.GClusterChangeCount)
            if changed != prevChangeCnt {
                prevChangeCnt = changed;
                gotChange = true
            }
            var _, err = cli.Get(key)
            if err != nil {
                log.Infof("in loop get client get error:%s", err.Error())
                if gotChange {
                    log.Infof("changed, client get error:%s when cur changed cnt:%d\n", err.Error(), int(changed))
                }
                if readWriteFail(err) {
                    if prevConnSucc {
                        prevConnSucc = false
                        cnnCloseCnt++
                    }
                    var err2 = cli.Connect()
                    if err2 != nil {
                        ch <- -1
                        log.Errorf("failed to connect to redis, get error:%s\n", err2.Error())
                        return
                    }
                }
                if notFound(err) {
                    if prevRdSucc {
                        prevRdSucc = false
                        rChange++
                    }
                    prevConnSucc = true
                }
            } else {
                if !prevRdSucc {
                    rChange++
                }
                prevRdSucc = true
                prevConnSucc = true
                log.Infof("succeed to get from:%s\n", addr)
            }
            if cnnCloseCnt == connFailCnt && rChange == expRCnt {
                log.Infof("detect cluster changed, work as expect")
                ch <- 1
                return
            }
            if changed >= int32(changeCnt) {
                log.Infof("detect cluster changed, but read change and conn change not expect, changed:%d connection close:%d rChange:%d\n", changed, cnnCloseCnt, rChange)
                ch <- -1
                return
            }
            time.Sleep(time.Duration(200) * time.Millisecond)
        }
    }
}

func updateConfFromList(intervalInSec int, fileName string, confs []proxy.ClusterConfigs, ch chan int) {
    var index = 0
    var cnt = len(confs)
    if cnt == 0 {
        log.Warn("invalid input, configure list is empty")
        return
    }
    for {
        select {
        case <- ch:
            log.Info("Update conf recv message from channel, exit now")
            return
        default:
            log.Infof("start dump %dth cluster conf to %s\n", index, fileName)
            var err = dumpClusterConf(fileName, confs[index])
            if err != nil {
                log.Infof("failed to dump cluster conf with error:%s\n", err.Error())
                return
            }
            index++
            if index >= cnt {
                ch <- 1
                return
            }
            time.Sleep(time.Duration(intervalInSec) * time.Second)
        }
    }
}

var gProxyConfFile = "./conf/proxy.conf"
var gClusterConfFile = "./conf/cluster.conf"

func TestClusterConfigLoadFromFileNoCloseFront(t *testing.T) {
    setup("8201", "8202")
    var firstConfName = "conf/noclose/redis_standalone_0.conf"
    var cmd = "cp " + firstConfName + " " + gClusterConfFile
    ExecCmd(cmd)
    proxy.MonitorCfgIntervalSecs = 1
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    var confCnt = 4
    var confList = make([]proxy.ClusterConfigs, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/noclose/redis_standalone_" + strconv.Itoa(int(i)) + ".conf"
        succ, msg, clusterConfs := proxy.LoadClusterConf(name)
        require.True(t, succ, msg)
        confList[i].Clusters = clusterConfs
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = gClusterConfFile
	assert.NoError(t, err)
    server.Serve(confList[0].Clusters)
    var chGet1 = make(chan int, 1)
    var chGet2 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8101"
    var frontAddr2 = "127.0.0.1:8102"
    var key = "key_loop_get1"
    var val = "val_loop_get1"
    go LoopGet(frontAddr1, key, val, 3, 2, 0, true, chGet1)
    go loopGetToSucc(frontAddr2, key, val, chGet2)
    go updateConfFromList(3, gClusterConfFile, confList, chUpdate)
    var retCnt = 0
    for {
        select {
        case cluster2 := <-chGet2:
            assert.Equal(t, 0, cluster2)
            retCnt++
        case getChanged := <-chGet1:
            assert.Equal(t, 1, getChanged)
            retCnt++
        }
        if retCnt >= 2 {
            log.Info("two check channel has return")
            break
        }
    }
    var cnt1 = connentCnt("127.0.0.1:8201")
    var cnt2 = connentCnt("127.0.0.1:8202")
    assert.Equal(t, 3, cnt1)
    assert.Equal(t, 1, cnt2)
    log.Info("no close front connection case done1")
    server.Close()
    log.Info("no close front connection case done")
    tearDown()
}

func TestClusterConfigLoadFromFileCloseFront(t *testing.T) {
    setup("8203", "8204")
    var firstConfName = "conf/close/redis_standalone_0.conf"
    var cmd = "cp " + firstConfName + " " + gClusterConfFile
    ExecCmd(cmd)
    proxy.MonitorCfgIntervalSecs = 1
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    log.Info("start reload case on close front connection")
    var confCnt = 4
    var confList = make([]proxy.ClusterConfigs, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/close/redis_standalone_" + strconv.Itoa(int(i)) + ".conf"
        succ, msg, clusterConfs := proxy.LoadClusterConf(name)
        require.True(t, succ, msg)
        confList[i].Clusters = clusterConfs
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = gClusterConfFile
	assert.NoError(t, err)
    server.Serve(confList[0].Clusters)
    var chGet1 = make(chan int, 1)
    var chGet2 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8103"
    var frontAddr2 = "127.0.0.1:8104"
    var key = "key_loop_get2"
    var val = "val_loop_get2"
    go LoopGet(frontAddr1, key, val, 3, 2, 2, true, chGet1)
    go loopGetToSucc(frontAddr2, key, val, chGet2)
    go updateConfFromList(3, gClusterConfFile, confList, chUpdate)
    var retCnt = 0
    for {
        select {
        case cluster2 := <-chGet2:
            log.Infof("loop get on cluster 2 has returned:%d\n", cluster2)
            assert.Equal(t, 0, cluster2)
            retCnt++
        case getChanged := <-chGet1:
            log.Infof("loop get on cluster 1 has returned:%d\n", getChanged)
            assert.Equal(t, 1, getChanged)
            retCnt++
        }
        if retCnt >= 2 {
            log.Info("two check channel has return")
            break
        }
    }
    var cnt1 = connentCnt("127.0.0.1:8203")
    var cnt2 = connentCnt("127.0.0.1:8204")
    assert.Equal(t, 3, cnt1) // self, cluster1, cluster2
    assert.Equal(t, 1, cnt2)
    server.Close()
    log.Info("close front connection case done")
    // tearDown()
}
