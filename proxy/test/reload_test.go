package proxy

import (
	"sync/atomic"
	"os"
	"testing"
	"time"
	"fmt"
    "strconv"
    "errors"

    "overlord/proxy"
	"overlord/pkg/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var SeqNO int = 0

func setupRedis(port1, port2 string) {
    proxy.ClusterSn = 0
    proxy.MonitorCfgIntervalMilliSecs = 500
    proxy.ClusterChangeCount = 0
    proxy.ClusterCount = 0
    proxy.AddClusterFailCnt = 0
    proxy.ClusterConfChangeFailCnt = 0
    KillAllRedis()
    var sn = strconv.Itoa(SeqNO)
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
    SeqNO++
}
func tearDownRedis() {
    KillAllRedis()
}
func tearDownMecache() {
    KillAllMC()
}

func checkAndKillStandalone(expectCnt int) {
    for {
        var changed = atomic.LoadInt32(&proxy.ClusterChangeCount)
        if changed >= int32(expectCnt) {
            log.Infof("cluster changed:%d larger than expect:%d, go kill standalone\n", int(changed), expectCnt)
            KillAllRedis()
            return
        }
        time.Sleep(time.Duration(50) * time.Millisecond)
    }
}

func setupMC(port1, port2 string) {
    proxy.ClusterSn = 0
    proxy.MonitorCfgIntervalMilliSecs = 500
    proxy.ClusterChangeCount = 0
    proxy.ClusterCount = 0
    proxy.AddClusterFailCnt = 0
    proxy.ClusterConfChangeFailCnt = 0
    KillAllMC()
    StartStandAloneMC(port1)
    StartStandAloneMC(port2)
    SeqNO++
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
func dumpClusterConf(fileName string, confs proxy.ClusterConfigs) error {
    var fullContent = ""
    for index := 0; index < len(confs.Clusters); index++ {
        var conf = confs.Clusters[index]
        if conf == nil {
            continue
        }
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

func getRedisConnCnt(addr string) int {
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
    var connCnt = ParseRedisClientCnt(msg)
    cli.Close()
    return int(connCnt)
}

func nofreeConn(addr string, ch chan int) {
    var cli = NewRedisConn(addr)
    var err = cli.Connect()
    if err != nil {
        log.Errorf("failed to connect to redis, get error:%s\n", err.Error())
        return
    }
    log.Info("connected to redis, no operation, just wait")
    for {
        select {
        case <- ch:
            log.Info("recv message from channel, exit now")
            cli.Close()
            time.Sleep(time.Duration(100) * time.Millisecond)
            ch <- 1
            return
        }
    }
}


func loopCheck(addr string, ch chan int) {
    var cli = NewRedisConn(addr)
    var err = cli.Connect()
    if err != nil {
        log.Errorf("failed to connect to redis, get error:%s\n", err.Error())
        return
    }
    defer cli.Close()
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
            var connCnt = ParseRedisClientCnt(msg)
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
    loopGetToSuccImpl(addr, key, val, 0, ch)
}
func loopGetToSuccMc(addr string, key, val string, ch chan int) {
    loopGetToSuccImpl(addr, key, val, 1, ch)
}

func loopGetToSuccImpl(addr string, key, val string, servType int, ch chan int) {
    log.Infof("try to loop get from addr:%s\n", addr)
    var cli CliConn
    if servType == 0 {
        cli = NewRedisConn(addr)
    } else if servType == 1 {
        cli = NewMemcacheConn(addr)
    } else if servType == 2 {
        cli = NewMemcacheConn(addr)
    }

    defer cli.Close()
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

func get(addr string, key, val string, servType int) error {
    log.Infof("try to loop get from addr:%s\n", addr)
    var cli CliConn
    if servType == 0 {
        cli = NewRedisConn(addr)
    } else if servType == 1 {
        cli = NewMemcacheConn(addr)
    } else if servType == 2 {
        cli = NewMemcacheConn(addr)
    }

    defer cli.Close()
    var err = cli.Connect()
    if err != nil {
        return err
    }
    var newVal, err2 = cli.Get(key)
    if err2 != nil {
        return err2
    }
    if newVal != val {
        return errors.New("expectValue:" + val + " not equal with real:" + newVal)
    }
    return nil
}

// changeCnt: configure load count 
// expRcnt: the number that read behaviour is changed, eg: (ok, fail, ok, fail) = 3
// expCnnFailCnt: the number that connection is changed, eg: (ok, fail, ok) = 2
func loopGet(addr, key, val string, expChangeCnt, expRCnt, expCnnFailCnt int, writeFirst bool, ch chan int) {
    loopGetImpl(addr, key, val, expChangeCnt, expRCnt, expCnnFailCnt, writeFirst, 0, ch)
}
func loopGetMc(addr, key, val string, expChangeCnt, expRCnt, expCnnFailCnt int, writeFirst bool, ch chan int) {
    loopGetImpl(addr, key, val, expChangeCnt, expRCnt, expCnnFailCnt, writeFirst, 1, ch)
}

func loopGetImpl(addr, key, val string, expChangeCnt, expRCnt, expCnnFailCnt int,
        writeFirst bool, servType int, ch chan int) {
    log.Infof("try to loop put and get to addr:%s\n", addr)
    var cli CliConn
    if servType == 0 {
        cli = NewRedisConn(addr)
    } else if servType == 1 {
        cli = NewMemcacheConn(addr)
    } else if servType == 2 {
        cli = NewMemcacheConn(addr)
    }
    var err = cli.Connect()
    if err != nil {
        log.Errorf("failed to connect to redis, get error:%s\n", err.Error())
        return
    }
    defer cli.Close()
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
            var changeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
            if changeCnt != prevChangeCnt {
                prevChangeCnt = changeCnt;
                gotChange = true
            }
            var _, err = cli.Get(key)
            if err != nil {
                log.Infof("in loop get client get error:%s", err.Error())
                if gotChange {
                    log.Infof("changed, client get error:%s when cur change cnt:%d\n", err.Error(), int(changeCnt))
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
                log.Infof("succeed to get from:%s, cur changed cnt:%d expect change cnt:%d\n", addr, int(changeCnt), int(expChangeCnt))
            }
            if expRCnt == 0 && rChange > 0 {
                ch <- -1
                return
            }
            if  expCnnFailCnt == 0 && cnnCloseCnt > 0 {
                ch <- -1
                return
            }
            if expChangeCnt == 0 && changeCnt > 0 {
                ch <- -1
                return
            }
            if expChangeCnt > 0 && changeCnt >= int32(expChangeCnt) {
                log.Infof("detect cluster changed, but read change and conn change not expect, change:%d connection close:%d rChange:%d\n", changeCnt, cnnCloseCnt, rChange)

                if cnnCloseCnt == expCnnFailCnt && rChange == expRCnt {
                    log.Infof("detect cluster changed, work as expect")
                    ch <- 1
                    return
                }
                ch <- -1
                return
            }
            time.Sleep(time.Duration(200) * time.Millisecond)
        }
    }
}

func updateConf(intervalInSec int, fileName string, srcNames []string, ch chan int) {
    var index = 0
    var cnt = len(srcNames)
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
            var src = srcNames[index]
            log.Infof("start copy cluster conf from:%s to %s\n", src, fileName)
            var cmd = "cp " + src + " " + fileName
            var _, err = ExecCmd(cmd)
            if err != nil {
                log.Infof("failed to dump cluster conf with error:%s\n", err.Error())
                return
            }
            index++
            if index >= cnt {
                ch <- 1
                return
            }
            time.Sleep(time.Duration(intervalInSec) * time.Millisecond)
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

func TestClusterConfigLoadFromFileNoCloseFront(t *testing.T) {
    var ClusterConfFile = "./conf/noclose_cluster.conf"
    setupRedis("8201", "8202")
    var firstConfName = "conf/noclose/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    var confCnt = 4
    var confList = make([]string, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/noclose/" + strconv.Itoa(int(i)) + ".conf"
        confList[i] = name
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)

    initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

    server.Serve(initConf)
    var chGet1 = make(chan int, 1)
    var chGet2 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8101"
    var frontAddr2 = "127.0.0.1:8102"
    var key = "key_loop_get1"
    var val = "val_loop_get1"
    go loopGet(frontAddr1, key, val, 3, 2, 0, true, chGet1)
    go loopGetToSucc(frontAddr2, key, val, chGet2)
    go updateConf(3000, ClusterConfFile, confList, chUpdate)
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
    var cnt1 = getRedisConnCnt("127.0.0.1:8201")
    var cnt2 = getRedisConnCnt("127.0.0.1:8202")
    assert.Equal(t, 3, cnt1)
    assert.Equal(t, 1, cnt2)
    server.Close()
    log.Info("no close front connection case done")
    tearDownRedis()
}

func TestClusterConfigLoadFromFileCloseFront(t *testing.T) {
    var ClusterConfFile = "./conf/close_cluster.conf"
    setupRedis("8203", "8204")
    var firstConfName = "conf/close/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    log.Info("start reload case on close front connection")
    var confCnt = 4
    var confList = make([]string, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/close/" + strconv.Itoa(int(i)) + ".conf"
        confList[i] = name
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)

    initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

    server.Serve(initConf)
    var chGet1 = make(chan int, 1)
    var chGet2 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8103"
    var frontAddr2 = "127.0.0.1:8104"
    var key = "key_loop_get2"
    var val = "val_loop_get2"
    go loopGet(frontAddr1, key, val, 3, 2, 2, true, chGet1)
    go loopGetToSucc(frontAddr2, key, val, chGet2)
    go updateConf(3000, ClusterConfFile, confList, chUpdate)
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
    var cnt1 = getRedisConnCnt("127.0.0.1:8203")
    var cnt2 = getRedisConnCnt("127.0.0.1:8204")
    assert.Equal(t, 3, cnt1) // self, cluster1, cluster2
    assert.Equal(t, 1, cnt2)
    server.Close()
    log.Info("close front connection case done")
    tearDownRedis()
}

func TestClusterConfigLoadDuplicatedAddrNoPanic(t *testing.T) {
    log.Infof("start reload case on nopanic when conf is invalid\n")
    var ClusterConfFile = "./conf/nopanic_cluster.conf"
    setupRedis("8205", "8206")
    var firstConfName = "conf/nopanic/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    var confCnt = 26
    var confList = make([]string, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/nopanic/" + strconv.Itoa(int(i)) + ".conf"
        confList[i] = name
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
    initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

    server.Serve(initConf)
    var chGet1 = make(chan int, 1)
    var chGet2 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8105"
    var frontAddr2 = "127.0.0.1:8106"
    var key = "key_loop_get2"
    var val = "val_loop_get2"
    go loopGet(frontAddr1, key, val, 1, 0, 0, true, chGet1)
    go loopGetToSucc(frontAddr2, key, val, chGet2)
    go updateConf(1000, ClusterConfFile, confList, chUpdate)
    var updateConfRet = <-chUpdate
    log.Infof("update configure routine return ret:%d\n", updateConfRet)

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
    server.Close()

    var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
    var clusterCnt = atomic.LoadInt32(&proxy.ClusterCount)
    assert.Equal(t, 1, int(clusterChangeCnt))
    assert.Equal(t, 2, int(clusterCnt))
    log.Info("no panic case done")
    tearDownRedis()
}

func TestClusterConfigFrontConnectionLeak(t *testing.T) {
    var ClusterConfFile = "./conf/cnnleak_cluster.conf"
    setupRedis("8207", "8208")
    var firstConfName = "conf/frontleak/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    log.Info("start reload case on front connection leak")
    var confCnt = 3
    var confList = make([]string, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/frontleak/" + strconv.Itoa(int(i)) + ".conf"
        confList[i] = name
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
    initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

    server.Serve(initConf)
    var chGet1 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var chFrontLeak = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8107"
    var key = "key_loop_get5"
    var val = "val_loop_get5"
    go loopGet(frontAddr1, key, val, 2, 2, 0, true, chGet1)
    go updateConf(2000, ClusterConfFile, confList, chUpdate)
    go nofreeConn(frontAddr1, chFrontLeak)
    var retCnt = 0
    for {
        select {
        case getChanged := <-chGet1:
            log.Infof("loop get on cluster 1 has returned:%d\n", getChanged)
            assert.Equal(t, 1, getChanged)
            retCnt++
        }
        if retCnt >= 1 {
            log.Info("loop get channel just return")
            break
        }
    }

    var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
    var clusterCnt = atomic.LoadInt32(&proxy.ClusterCount)
    assert.Equal(t, 2, int(clusterChangeCnt))
    assert.Equal(t, 1, int(clusterCnt))
    log.Info("start to connection to check")
    var cnt1 = getRedisConnCnt("127.0.0.1:8207")
    assert.Equal(t, 3, int(cnt1))  // self, forwarder0, forwarder2
    chFrontLeak <- 1
    <-chFrontLeak  // just make sure front connection is closed
    cnt1 = getRedisConnCnt("127.0.0.1:8207")
    assert.Equal(t, 2, int(cnt1))  // self, forwarder2
    log.Info("front leak case done")
    server.Close()
    tearDownRedis()
}

func TestClusterConfigReloadMemcacheCluster(t *testing.T) {
    var ClusterConfFile = "./conf/mc_cluster.conf"
    setupMC("8209", "8210")
    var firstConfName = "conf/memcache/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    log.Info("start reload case of memcache")
    var confCnt = 3
    var confList = make([]string, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/memcache/" + strconv.Itoa(int(i)) + ".conf"
        confList[i] = name
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
    initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

    server.Serve(initConf)
    var chGet1 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8109"
    var key = "mckey_loop_get1"
    var val = "mcval_loop_get1"
    go loopGetMc(frontAddr1, key, val, 2, 0, 0, true, chGet1)
    go updateConf(2000, ClusterConfFile, confList, chUpdate)
    var retCnt = 0
    for {
        select {
        case getChanged := <-chGet1:
            log.Infof("loop get on cluster 1 has returned:%d\n", getChanged)
            assert.Equal(t, 1, getChanged)
            retCnt++
        }
        if retCnt >= 1 {
            log.Info("two check channel has return")
            break
        }
    }
    server.Close()

    var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
    var clusterCnt = atomic.LoadInt32(&proxy.ClusterCount)
    assert.Equal(t, 2, int(clusterChangeCnt))
    assert.Equal(t, 1, int(clusterCnt))
    log.Info("memcache reload done")
    tearDownMecache()
}

func TestClusterConfigReloadRedisCluster(t *testing.T) {
    var ClusterConfFile = "./conf/rediscluster_cluster.conf"
    setupRedis("8211", "8212")
    log.Infof("start reload case of redis cluster\n")

    var cluster1 = "127.0.0.1:7000"
    var cluster2 = "127.0.0.1:9000"
    var cli1 = NewRedisConn(cluster1)
    var cli2 = NewRedisConn(cluster2)
    cli1.autoReconn = true
    cli2.autoReconn = true
    var key = "cluster_key1"
    var val = "cluster_value1"
    var err0 = cli1.Put(key, val)
	require.NoError(t, err0)
    err0 = cli2.Put(key, val)
	require.NoError(t, err0)
    cli1.Close()
    cli2.Close()

    var firstConfName = "conf/rediscluster/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    var confCnt = 4
    var confList = make([]string, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/rediscluster/" + strconv.Itoa(int(i)) + ".conf"
        confList[i] = name
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
    var name = "conf/rediscluster/0.conf"
    initConf, initErr := proxy.LoadClusterConf(name)
	require.NoError(t, initErr)

    server.Serve(initConf)
    var chGet1 = make(chan int, 1)
    var chGet2 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8111"
    var frontAddr2 = "127.0.0.1:8112"
    go loopGet(frontAddr1, key, val, 3, 0, 1, true, chGet1)
    go loopGetToSucc(frontAddr2, key, val, chGet2)
    go updateConf(2000, ClusterConfFile, confList, chUpdate)
    go checkAndKillStandalone(1)
    var retCnt = 0
    for {
        select {
        case getChanged := <-chGet1:
            log.Infof("loop get on cluster 1 has returned:%d\n", getChanged)
            assert.Equal(t, 1, getChanged)
            retCnt++
        case cluster2 := <-chGet2:
            assert.Equal(t, 0, cluster2)
            retCnt++
        }
        if retCnt >= 2 {
            log.Info("two check channel has return")
            break
        }
    }
    server.Close()

    var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
    var clusterCnt = atomic.LoadInt32(&proxy.ClusterCount)
    assert.Equal(t, 3, int(clusterChangeCnt))
    assert.Equal(t, 2, int(clusterCnt))
    log.Info("redis cluster reload done")
    tearDownRedis()
}

func TestClusterConfigLoadLotsofCluster(t *testing.T) {
    var ClusterConfFile = "./conf/lotsof_cluster.conf"
    setupRedis("8213", "8214")
    proxy.MonitorCfgIntervalMilliSecs = 100
    var firstConfName = "conf/lotsof/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}

    initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

    log.Info("start reload case on lots of when conf is invalid")
    var confCnt = int(proxy.MaxClusterCnt) + 3
    var confNameList = make([]string, confCnt, confCnt)
    confNameList[0] = firstConfName

    var baseConfig proxy.ClusterConfig;
    baseConfig = *(initConf[0])
    var confList proxy.ClusterConfigs;
    confList.Clusters = make([]*proxy.ClusterConfig, confCnt, confCnt)
    confList.Clusters[0] = &baseConfig;
    for i := 1; i < confCnt - 1; i++ {
        var fileName = "conf/lotsof/" + strconv.Itoa(int(i)) + ".conf"
        var newConf = baseConfig;
        newConf.Name = baseConfig.Name + "_" + strconv.Itoa(int(i))
        newConf.ListenAddr = "127.0.0.1:" + strconv.Itoa(8513 + int(i))
        confList.Clusters[i] = &newConf;
        dumpClusterConf(fileName, confList)
        confNameList[i] = fileName
        // fmt.Printf("add conf file:%s\n", confNameList[i])
    }
    confNameList[confCnt - 1] =  "conf/lotsof/127.conf"

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)

    server.Serve(initConf)
    var chGet1 = make(chan int, 1)
    var chUpdate = make(chan int, 1)
    var frontAddr1 = "127.0.0.1:8513"
    var key = "key_lots_get2"
    var val = "val_lots_get2"
    go loopGet(frontAddr1, key, val, 0, 0, 0, true, chGet1)
    go updateConf(400, ClusterConfFile, confNameList, chUpdate)
    var updateConfRet = <-chUpdate
    log.Infof("update configure routine return ret:%d\n", updateConfRet)
    for {
        var breakNow = false
        select {
        case getChanged := <-chGet1:
            log.Infof("loop get on cluster 1 has returned:%d\n", getChanged)
            assert.Equal(t, 1, getChanged)
            breakNow = true
        default:
            var clusterCnt = atomic.LoadInt32(&proxy.ClusterCount)
            if int(clusterCnt) == int(proxy.MaxClusterCnt) {
                breakNow = true
            }

        }
        if breakNow {
            chGet1 <- 1
            log.Info("main routine break now")
            break
        }
    }
    for i := 0; i < confCnt; i++ {
        var addr = "127.0.0.1:" + strconv.Itoa(8513 + int(i))
        var err = get(addr, key, val, 0);
        if i < int(proxy.MaxClusterCnt) {
            assert.NoError(t, err)
        } else {
            require.True(t, err != nil)
        }
    }

    server.Close()

    var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
    assert.Equal(t, 0, int(clusterChangeCnt))
    log.Info("lots of case done")
    tearDownRedis()
}

func TestClusterConfigReloadGetRedisSeedFail(t *testing.T) {
    var ClusterConfFile = "./conf/rediscluster_seed_cluster.conf"
    setupRedis("8215", "8216")
    log.Infof("start reload case of get seed fail\n")

    var firstConfName = "conf/getseedfail/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    var confCnt = 2
    var confList = make([]string, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/getseedfail/" + strconv.Itoa(int(i)) + ".conf"
        confList[i] = name
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
    var name = "conf/getseedfail/0.conf"
    initConf, initErr := proxy.LoadClusterConf(name)
	require.NoError(t, initErr)

    server.Serve(initConf)
    var chUpdate = make(chan int, 1)
    go updateConf(1000, ClusterConfFile, confList, chUpdate)
    var updateConfRet = <-chUpdate
    log.Infof("update configure routine return ret:%d\n", updateConfRet)
    time.Sleep(time.Duration(2 * proxy.MonitorCfgIntervalMilliSecs) * time.Millisecond)
    server.Close()

    var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
    var clusterCnt = atomic.LoadInt32(&proxy.ClusterCount)
    assert.Equal(t, 0, int(clusterChangeCnt))
    assert.Equal(t, 1, int(clusterCnt))
    var faildCnt = atomic.LoadInt32(&proxy.AddClusterFailCnt)
    assert.True(t, faildCnt > 0)
    log.Info("redis cluster get seek fail case done")
    tearDownRedis()
}

func TestClusterConfigReloadValidateAddress(t *testing.T) {
    var redisCluster1 = make ([]string, 0, 10)
	require.Error(t, proxy.ValidateRedisCluster(redisCluster1))

    redisCluster1 = append(redisCluster1, ":")
	require.Error(t, proxy.ValidateRedisCluster(redisCluster1))

    redisCluster1[0] = " : "
	require.Error(t, proxy.ValidateRedisCluster(redisCluster1))

    redisCluster1[0] = "0.0.0.0: "
	require.Error(t, proxy.ValidateRedisCluster(redisCluster1))

    redisCluster1[0] = "0.0.0.0: "
	require.Error(t, proxy.ValidateRedisCluster(redisCluster1))

    redisCluster1[0] = "0.0.0.0:0"
	require.Error(t, proxy.ValidateRedisCluster(redisCluster1))

    redisCluster1[0] = "0.0.0.0: 1"
	require.Error(t, proxy.ValidateRedisCluster(redisCluster1))

    var stCluster1 = make ([]string, 0, 10)
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1 = append(stCluster1, "")
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = " "
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = "0.0.0.0"
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = "0.0.0.0 name"
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = "0.0.0.0 name name2"
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = "0.0.0.0:1 name"
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = "0.0.0.0:0:2 name"
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = "0.0.0.0:X:2 name"
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = "0.0.0.0:0:Y name"
	require.Error(t, proxy.ValidateStandalone(stCluster1))

    stCluster1[0] = "0.0.0.0:0:-1 name"
	require.Error(t, proxy.ValidateStandalone(stCluster1))
}

func TestClusterConfigReloadClusterRemoved(t *testing.T) {
    var ClusterConfFile = "./conf/some_cluster_removed.conf"
    setupRedis("8217", "8218")
    log.Infof("start reload case of cluster removed\n")

    var firstConfName = "conf/cluster_removed/0.conf"
    var cmd = "cp " + firstConfName + " " + ClusterConfFile
    ExecCmd(cmd)
    var proxyConf = &proxy.Config{}
    var loadConfError = proxyConf.LoadFromFile(gProxyConfFile)
	assert.NoError(t, loadConfError)
    if log.Init(proxyConf.Config) {
		defer log.Close()
	}
    var confCnt = 2
    var confList = make([]string, confCnt, confCnt)
    for i := 0; i < confCnt; i++ {
        var name = "conf/cluster_removed/" + strconv.Itoa(int(i)) + ".conf"
        confList[i] = name
    }

    var server, err = proxy.NewProxy(proxyConf)
    server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
    var name = "conf/cluster_removed/0.conf"
    initConf, initErr := proxy.LoadClusterConf(name)
	require.NoError(t, initErr)

    server.Serve(initConf)
    var chUpdate = make(chan int, 1)
    go updateConf(1000, ClusterConfFile, confList, chUpdate)
    var updateConfRet = <-chUpdate
    log.Infof("update configure routine return ret:%d\n", updateConfRet)
    time.Sleep(time.Duration(2 * proxy.MonitorCfgIntervalMilliSecs) * time.Millisecond)
    server.Close()

    var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
    var clusterCnt = atomic.LoadInt32(&proxy.ClusterCount)
    assert.Equal(t, 0, int(clusterChangeCnt))
    assert.Equal(t, 2, int(clusterCnt))
    var faildCnt = atomic.LoadInt32(&proxy.FailedDueToRemovedCnt)
    assert.True(t, faildCnt > 0)
    log.Info("redis cluster removed case done")
    tearDownRedis()
}
