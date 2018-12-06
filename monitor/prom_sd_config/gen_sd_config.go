package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"overlord/job/create"
	"overlord/lib/etcd"
	"overlord/lib/log"

	"github.com/BurntSushi/toml"
)

var (
	confPath string
)

// ServerConfig is gen_sd_config's config
type ServerConfig struct {
	Listen       string `toml:"listen"`
	Etcd         string `toml:"etcd"`
	EtcdPath     string `toml:"etcd_path"`
	SdConfig     string `toml:"sd_config"`
	DefaultOwner string `toml:"default_owner"`
	*log.Config
}

// MetricLabels is prometheus metric labels
type MetricLabels struct {
	Job      string `json:"job"`
	Owner    string `json:"owner"`
	Pro      string `json:"pro"`
	Name     string `json:"name"`
	NodeType string `json:"node_type"`
}

// MetricInfo is prometheus metric info
type MetricInfo struct {
	Labels  MetricLabels `json:"labels"`
	Targets []string     `json:"targets"`
}

func genMetric(conf *ServerConfig, info *create.CacheInfo) (data MetricInfo) {
	var (
		metricLabels MetricLabels
		addrs        []string
	)

	if info.CacheType == "redis" || info.CacheType == "redis-cluster" {
		metricLabels = MetricLabels{
			Job:      "redis_exporter",
			Owner:    conf.DefaultOwner,
			Pro:      info.Name,
			Name:     info.Name,
			NodeType: string(info.CacheType),
		}
	} else if info.CacheType == "memcache" {
		metricLabels = MetricLabels{
			Job:   "memcache_exporter",
			Owner: conf.DefaultOwner,
			Pro:   info.Name,
			Name:  info.Name,
		}
	}

	// get redis/redis-cluster/memcache addrs
	for _, addr := range info.Dist.Addrs {
		addrs = append(addrs, addr.String())
	}

	data = MetricInfo{
		Labels:  metricLabels,
		Targets: addrs,
	}
	return
}

func genSdConfig(conf *ServerConfig, e *etcd.Etcd) {
	nodes, err := e.LS(context.TODO(), "/overlord/clusters")
	if err != nil {
		panic(err)
	}

	var metrics []MetricInfo

	for _, node := range nodes {
		var (
			val  string
			info *create.CacheInfo
		)

		val, err = e.Get(context.TODO(), fmt.Sprintf("%s/info", node.Key))
		if err != nil {
			log.Error(err.Error())
			continue
		}
		de := json.NewDecoder(strings.NewReader(val))
		err = de.Decode(&info)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		metricInfo := genMetric(conf, info)
		metrics = append(metrics, metricInfo)
	}

	b, err := json.MarshalIndent(metrics, "", "\t")
	if err != nil {
		log.Error(err.Error())
	}

	err = ioutil.WriteFile(conf.SdConfig, b, 0644)
	if err != nil {
		log.Warn("%s file write failed.", conf.SdConfig)
	}
}

func main() {
	flag.StringVar(&confPath, "conf", "conf.toml", "scheduler conf")
	flag.Parse()
	conf := new(ServerConfig)
	_, err := toml.DecodeFile(confPath, &conf)
	if err != nil {
		panic(err)
	}
	if log.Init(conf.Config) {
		defer log.Close()
	}

	e, err := etcd.New(conf.Etcd)
	if err != nil {
		panic(err)
	}

	out := make(chan int)
	// Start recursively watching for updates.
	go func() {
		etcdWatcher, err := e.WatchOn(context.TODO(), conf.EtcdPath, etcd.ActionSet, etcd.ActionCreate, etcd.ActionDelete)
		if err != nil {
			panic(err)
		}
		for {
			select {
			case <-etcdWatcher:
				select {
				case out <- 1:
					continue
				default:
					continue
				}

			}
		}
	}()

	for {
		select {
		// Update the configuration at regular intervals
		case <-time.After(15 * time.Second):
			go genSdConfig(conf, e)
			// Configuration updates are triggered based on events
		case <-out:
			go genSdConfig(conf, e)
			<-time.After(3 * time.Second)
		}
	}
}
