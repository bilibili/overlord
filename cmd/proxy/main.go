package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof" // NOTE: use http pprof
	"os"
	"os/signal"
	"strings"
	"syscall"

	"overlord/pkg/log"
	"overlord/pkg/prom"
	"overlord/proxy"
	"overlord/proxy/slowlog"
)

const (
	// VERSION version
	VERSION = "1.8.0"
)

var (
	version         bool
	check           bool
	stat            string
	metrics         bool
	confFile        string
	clusterConfFile string
	reload          bool
	slowlogFile     string
)

type clustersFlag []string

func (c *clustersFlag) String() string {
	return strings.Join([]string(*c), " ")
}

func (c *clustersFlag) Set(n string) error {
	*c = append(*c, n)
	return nil
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of Overlord proxy:\n")
	flag.PrintDefaults()
}

func init() {
	flag.Usage = usage
	flag.BoolVar(&check, "t", false, "conf file check")
	flag.BoolVar(&version, "v", false, "print version.")
	flag.StringVar(&stat, "stat", "", "stat listen addr. high priority than conf.stat.")
	flag.BoolVar(&metrics, "metrics", false, "proxy support prometheus metrics and reuse stat port.")
	flag.StringVar(&confFile, "conf", "", "conf file of proxy itself.")
	flag.StringVar(&clusterConfFile, "cluster", "", "conf file of backend cluster.")
	flag.BoolVar(&reload, "reload", false, "reloading the servers in cluster config file.")
	flag.StringVar(&slowlogFile, "slowlog", "", "slowlog is the file where slowlog output")
}

func main() {
	flag.Parse()
	if version {
		fmt.Printf("overlord version %s\n", VERSION)
		os.Exit(0)
	}
	if check {
		parseConfig()
		os.Exit(0)
	}
	c, ccs := parseConfig()
	if log.Init(c.Config) {
		defer log.Close()
	}
	// init slowlog if need
	err := slowlog.Init(slowlogFile)
	if err != nil {
		log.Errorf("fail to init slowlog due %s", err)
	}

	// new proxy
	p, err := proxy.New(c)
	if err != nil {
		panic(err)
	}
	defer p.Close()
	p.Serve(ccs)
	if reload {
		go p.MonitorConfChange(clusterConfFile)
	}
	// pprof
	if c.Stat != "" {
		go http.ListenAndServe(c.Stat, nil)
		if c.Proxy.UseMetrics {
			prom.Init()
		} else {
			prom.On = false
		}
	}
	prom.VersionState(VERSION)
	// hanlde signal
	signalHandler()
}

func parseConfig() (c *proxy.Config, ccs []*proxy.ClusterConfig) {
	if confFile != "" {
		c = &proxy.Config{}
		if err := c.LoadFromFile(confFile); err != nil {
			panic(err)
		}
	} else {
		c = proxy.DefaultConfig()
	}
	// high priority start
	if stat != "" {
		c.Stat = stat
	}
	if metrics {
		c.Proxy.UseMetrics = metrics
	}
	// high priority end
	var tmpCCS, err = proxy.LoadClusterConf(clusterConfFile)
	if err != nil {
		panic(err)
	}
	ccs = tmpCCS
	return
}

func signalHandler() {
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		log.Infof("overlord proxy version[%s] start serving", VERSION)
		si := <-ch
		log.Infof("overlord proxy version[%s] signal(%s) stop the process", VERSION, si.String())
		switch si {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			log.Infof("overlord proxy version[%s] exited", VERSION)
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
