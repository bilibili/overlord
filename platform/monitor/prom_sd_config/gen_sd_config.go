package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"overlord/pkg/etcd"
	"overlord/pkg/log"
	"overlord/platform/job/create"

	"os"

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
	SdConfig     string `toml:"sd_config_dir"`
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

	if info.CacheType == "redis" || info.CacheType == "redis_cluster" {
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

	// get redis/redis_cluster/memcache addrs
	if info.CacheType == "redis" || info.CacheType == "memcache" {
		for _, addr := range info.Dist.Addrs {
			addrs = append(addrs, addr.String())
		}
	} else if info.CacheType == "redis_cluster" {
		for _, nodes := range info.Chunks {
			for _, addr := range nodes.Nodes {
				address := fmt.Sprintf("%s:%d", addr.Name, addr.Port)
				addrs = append(addrs, address)
			}
		}
	}

	data = MetricInfo{
		Labels:  metricLabels,
		Targets: addrs,
	}
	return
}

// genSdConfig generate prom_sd_config
func genSdConfig(conf *ServerConfig, e *etcd.Etcd) {
	nodes, err := e.LS(context.TODO(), "/overlord/clusters")
	if err != nil {
		panic(err)
	}

	metricsMap := make(map[string][]MetricInfo)

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

		if info.CacheType == "redis" || info.CacheType == "redis_cluster" {
			if _, ok := metricsMap["redis"]; !ok {
				metricsMap["redis"] = []MetricInfo{}
			}
			metricsMap["redis"] = append(metricsMap["redis"], metricInfo)

		} else if info.CacheType == "memcache" {
			if _, ok := metricsMap["memcache"]; !ok {
				metricsMap["memcache"] = []MetricInfo{}
			}
			metricsMap["memcache"] = append(metricsMap["memcache"], metricInfo)
		}
	}

	for cacheType, metrics := range metricsMap {
		var confPath string
		if cacheType == "redis" {
			confPath = fmt.Sprintf("%s/redis.json", conf.SdConfig)
		} else if cacheType == "memcache" {
			confPath = fmt.Sprintf("%s/memcache.json", conf.SdConfig)
		}

		fmt.Println(confPath)
		file, err := os.Open(confPath)
		if err != nil && os.IsNotExist(err) {
			file, err = os.Create(confPath)
			if err != nil {
				panic(err)
			}
			file.Close()
		}

		err = writeToFile(confPath, metrics)
		if err != nil {
			log.Error(err.Error())
		}
	}
}

// writeToFile write metrics to file
func writeToFile(filePath string, metrics []MetricInfo) (err error) {
	b, err := json.MarshalIndent(metrics, "", "\t")
	if err != nil {
		log.Error(err.Error())
	}

	err = ioutil.WriteFile(filePath, b, 0644)
	if err != nil {
		log.Warn(filePath, " file write failed.")
	}
	return
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
