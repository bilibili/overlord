package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"overlord/pkg/log"
	"overlord/proxy"

	"github.com/BurntSushi/toml"
	"github.com/Pallinder/go-randomdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var SeqNO int = 0

func init() {
	proxy.PingSleepTime = func(t bool) time.Duration {
		return 5 * time.Millisecond
	}
}

func clearEnv() {
	proxy.ClusterChangeCount = 0
	proxy.LoadFailCnt = 0
}

func setupRedis(port1, port2 string, s1 *RMServer, s2 *RMServer) {
	clearEnv()
	s1.port = port1
	s2.port = port2
	var err1 = s1.start()
	if err1 != nil {
		fmt.Printf("failed to start redis1, get error:%s\n", err1.Error())
	}
	var err2 = s2.start()
	if err2 != nil {
		fmt.Printf("failed to start redis2, get error:%s\n", err2.Error())
	}
}
func teardownRMServer(s1 *RMServer, s2 *RMServer) {
	s1.stop()
	s2.stop()
}

func setupMC(port1, port2 string, s1, s2 *RMServer) {
	clearEnv()

	s1.port = port1
	s2.port = port2
	var err1 = s1.start()
	if err1 != nil {
		fmt.Printf("failed to start memcache1, get error:%s\n", err1.Error())
	}
	var err2 = s2.start()
	if err2 != nil {
		fmt.Printf("failed to start memcache2, get error:%s\n", err2.Error())
	}
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
	var buf bytes.Buffer
	encoder := toml.NewEncoder(&buf)
	err := encoder.Encode(confs)
	if err != nil {
		return err
	}
	var content = buf.Bytes()
	err = ioutil.WriteFile(fileName, content, 0644)
	return err
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
		case <-ch:
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
			if connCnt != 1 {
				if cnt%20 == 19 {
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
		case <-ch:
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
				log.Errorf("loop get failed to get from addr:%s got error:%s\n", addr, err.Error())
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
		ch <- -1
		return
	}
	defer cli.Close()
	if writeFirst {
		err = cli.Put(key, val)
		if err != nil {
			ch <- -1
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
		case <-ch:
			log.Info("loop get recv message from channel, exit now")
			return
		default:
			// log.Info("loop get now")
			// var msg = ""
			var gotChange = false
			var changeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
			if changeCnt != prevChangeCnt {
				prevChangeCnt = changeCnt
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
						log.Infof("find connection close, new connection close count is:%d\n", cnnCloseCnt)
					}
					var err2 = cli.Connect()
					if err2 != nil {
						ch <- -1
						log.Errorf("failed to connect to redis, get error:%s\n", err2.Error())
						return
					}
				} else {
					if notFound(err) {
						if prevRdSucc {
							prevRdSucc = false
							rChange++
							log.Infof("find read result change from succeed to not found, now change cnt:%d\n", rChange)
						}
						prevConnSucc = true
					} else {
						log.Infof("meet unprocessed error:%s\n", err.Error())
					}
				}
			} else {
				if !prevRdSucc {
					rChange++
					log.Infof("find read result change from fail to succeed, now change cnt:%d\n", rChange)
				}
				prevRdSucc = true
				prevConnSucc = true
				log.Infof("succeed to get from:%s, cur changed cnt:%d expect change cnt:%d\n", addr, int(changeCnt), int(expChangeCnt))
			}
			if expRCnt == 0 && rChange > 0 {
				ch <- -1
				return
			}
			if expCnnFailCnt == 0 && cnnCloseCnt > 0 {
				ch <- -1
				return
			}
			if expChangeCnt == 0 && changeCnt > 0 {
				ch <- -1
				return
			}
			if expChangeCnt > 0 && changeCnt >= int32(expChangeCnt) {
				log.Infof("detect cluster changed, but read change and conn lost not expect, change:%d connection close:%d readChange:%d\n", changeCnt, cnnCloseCnt, rChange)

				if cnnCloseCnt == expCnnFailCnt && rChange == expRCnt {
					log.Infof("detect cluster changed, work as expect")
					ch <- 1
					return
				}
				ch <- -1
				return
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
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
		case <-ch:
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
		case <-ch:
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

var ProxyConfFile = "./conf/proxy.conf"

func putLots(cnt int, cli CliConn, prefix string) (error, []string, []string) {
	var keylist = make([]string, cnt, cnt)
	var valuelist = make([]string, cnt, cnt)
	for i := 0; i < cnt; i++ {
		var key = prefix + "_" + randomdata.SillyName() + strconv.Itoa(i)
		var val = "val_" + key
		var err = cli.Put(key, val)
		if err != nil {
			return err, nil, nil
		}
		keylist[i] = key
		valuelist[i] = val
	}
	return nil, keylist, valuelist
}

func check(keylist, valuelist []string, cli CliConn, prefix string, exist bool) error {
	var findCnt = 0
	var cnt = len(keylist)
	for i := 0; i < cnt; i++ {
		var key = keylist[i]
		var val = valuelist[i]
		var ret, err = cli.Get(key)
		if err != nil {
			if notFound(err) {
				continue
			}
			return err
		}
		if ret == "" {
			continue
		}
		if val != ret {
			return errors.New("return value:" + ret + " != " + val)
		}
		findCnt++
	}
	if exist && findCnt == 0 {
		return errors.New("total:" + strconv.Itoa(cnt) + " keys, no key fond")
	}
	if !exist && findCnt > 0 {
		return errors.New("total:" + strconv.Itoa(cnt) + " keys, expect not found, but found:" + strconv.Itoa(cnt))
	}
	return nil
}

func TestClusterConfigReloadMemcacheCluster(t *testing.T) {
	var ClusterConfFile = "./conf/mc_cluster.conf"
	var s1 = NewMemServer()
	var s2 = NewMemServer()
	setupMC("8209", "8210", s1, s2)

	var firstConfName = "conf/memcache/0.conf"
	var cmd = "cp " + firstConfName + " " + ClusterConfFile
	ExecCmd(cmd)
	var proxyConf = &proxy.Config{}
	var loadConfError = proxyConf.LoadFromFile(ProxyConfFile)
	assert.NoError(t, loadConfError)
	if log.Init(proxyConf.Config) {
		defer log.Close()
	}
	log.Info("start reload case of memcache")

	var server, err = proxy.New(proxyConf)
	server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
	initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

	server.Serve(initConf)
	var cli = NewMemcacheConn("127.0.0.1:8109")
	err = cli.Connect()
	require.NoError(t, err)
	defer cli.Close()
	var keylist []string
	var valuelist []string
	err, keylist, valuelist = putLots(100, cli, "mem_cache_put")
	require.NoError(t, err)

	var src = "conf/memcache/1.conf"
	log.Infof("start to copy file:%s\n", src)
	cmd = "cp " + src + " " + ClusterConfFile
	var _, err1 = ExecCmd(cmd)
	require.NoError(t, err1)
	time.Sleep(time.Duration(100) * time.Millisecond)
	err = check(keylist, valuelist, cli, "mem_cache_put", false)
	require.NoError(t, err)

	src = "conf/memcache/2.conf"
	log.Infof("start to copy file:%s\n", src)
	cmd = "cp " + src + " " + ClusterConfFile
	var _, err2 = ExecCmd(cmd)
	require.NoError(t, err2)
	time.Sleep(time.Duration(100) * time.Millisecond)
	err = check(keylist, valuelist, cli, "mem_cache_put", true)
	require.NoError(t, err)

	var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
	assert.Equal(t, 2, int(clusterChangeCnt))
	log.Info("memcache reload done")
	teardownRMServer(s1, s2)
}

func TestClusterConfigReloadRedisCluster(t *testing.T) {
	var ClusterConfFile = "./conf/st_redis.conf"
	var s1 = NewRedisServer()
	var s2 = NewRedisServer()
	setupRedis("8201", "8202", s1, s2)

	var firstConfName = "conf/standalone_redis/0.conf"
	var cmd = "cp " + firstConfName + " " + ClusterConfFile
	ExecCmd(cmd)
	var proxyConf = &proxy.Config{}
	var loadConfError = proxyConf.LoadFromFile(ProxyConfFile)
	assert.NoError(t, loadConfError)
	if log.Init(proxyConf.Config) {
		defer log.Close()
	}
	log.Info("start reload case of standalone redis")

	var server, err = proxy.New(proxyConf)
	server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
	initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

	server.Serve(initConf)
	var cli = NewRedisConn("127.0.0.1:8101")
	err = cli.Connect()
	require.NoError(t, err)
	defer cli.Close()
	var keylist []string
	var valuelist []string
	err, keylist, valuelist = putLots(100, cli, "redis_put")
	require.NoError(t, err)

	var src = "conf/standalone_redis/1.conf"
	log.Infof("start to copy file:%s\n", src)
	cmd = "cp " + src + " " + ClusterConfFile
	var _, err1 = ExecCmd(cmd)
	require.NoError(t, err1)
	time.Sleep(time.Duration(100) * time.Millisecond)
	err = check(keylist, valuelist, cli, "redis_put", false)
	require.NoError(t, err)

	src = "conf/standalone_redis/2.conf"
	log.Infof("start to copy file:%s\n", src)
	cmd = "cp " + src + " " + ClusterConfFile
	var _, err2 = ExecCmd(cmd)
	require.NoError(t, err2)
	time.Sleep(time.Duration(100) * time.Millisecond)
	err = check(keylist, valuelist, cli, "redis_put", true)
	require.NoError(t, err)

	var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
	assert.Equal(t, 2, int(clusterChangeCnt))
	log.Info("standalone redis reload done")
	teardownRMServer(s1, s2)
}

func TestClusterConfigReloadNoChange(t *testing.T) {
	clearEnv()
	var proxyConf = &proxy.Config{}
	var loadConfError = proxyConf.LoadFromFile(ProxyConfFile)
	require.NoError(t, loadConfError)
	if log.Init(proxyConf.Config) {
		defer log.Close()
	}

	var ClusterConfFile = "./conf/nochange.conf"
	var firstConfName = "conf/nochange/0.conf"

	var server, err = proxy.New(proxyConf)
	server.ClusterConfFile = ClusterConfFile
	assert.NoError(t, err)
	initConf, initErr := proxy.LoadClusterConf(firstConfName)
	require.NoError(t, initErr)

	server.Serve(initConf)
	var src = "conf/nochange/1.conf"
	log.Infof("start to copy file:%s\n", src)
	var cmd = "cp " + src + " " + ClusterConfFile
	var _, err1 = ExecCmd(cmd)
	require.NoError(t, err1)
	time.Sleep(time.Duration(100) * time.Millisecond)

	log.Infof("start to remove file:%s\n", ClusterConfFile)
	cmd = "rm -f " + ClusterConfFile
	var _, err2 = ExecCmd(cmd)
	require.NoError(t, err2)
	time.Sleep(time.Duration(100) * time.Millisecond)

	src = "conf/nochange/2.conf"
	log.Infof("start to copy file:%s\n", src)
	cmd = "cp " + src + " " + ClusterConfFile
	var _, err3 = ExecCmd(cmd)
	require.NoError(t, err3)
	time.Sleep(time.Duration(1000) * time.Millisecond)

	log.Infof("start to rename file:%s\n", ClusterConfFile)
	cmd = "mv " + ClusterConfFile + " " + ClusterConfFile + ".tmp"
	var _, err4 = ExecCmd(cmd)
	require.NoError(t, err4)
	time.Sleep(time.Duration(1000) * time.Millisecond)

	src = "conf/nochange/2.conf"
	log.Infof("start to copy file:%s\n", src)
	cmd = "cp " + src + " " + ClusterConfFile
	var _, err5 = ExecCmd(cmd)
	require.NoError(t, err5)
	time.Sleep(time.Duration(1000) * time.Millisecond)

	var clusterChangeCnt = atomic.LoadInt32(&proxy.ClusterChangeCount)
	assert.Equal(t, 0, int(clusterChangeCnt))
}

func removeFiles(toRemove []string) {
	for _, file := range toRemove {
		if file == "" {
			continue
		}
		var cmd = "rm " + file
		ExecCmd(cmd)
	}
}
