package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/felixhao/overlord/proxy"
)

const (
	// VERSION version
	VERSION = "1.0.0"
)

var (
	version  bool
	debug    bool
	pprof    string
	config   string
	clusters clustersFlag
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
	flag.BoolVar(&version, "v", false, "print version.")
	flag.BoolVar(&debug, "debug", false, "debug model.")
	flag.StringVar(&pprof, "pprof", "", "pprof listen addr.")
	flag.StringVar(&config, "conf", "", "run with the specific configuration.")
	flag.Var(&clusters, "cluster", "specify cache cluster configuration.")
}

func main() {
	flag.Parse()
	if version {
		log.Printf("overlord version %s\n", VERSION)
		os.Exit(0)
	}
	c, ccs := parseConfig()
	// new proxy
	p, err := proxy.New(c)
	if err != nil {
		panic(err)
	}
	defer p.Close()
	go p.Serve(ccs)
	// hanlde signal
	signalHandler()
}

func parseConfig() (c *proxy.Config, ccs []*proxy.ClusterConfig) {
	if config != "" {
		c = &proxy.Config{}
		if err := c.LoadFromFile(config); err != nil {
			panic(err)
		}
	} else {
		c = proxy.DefaultConfig()
	}
	// high priority
	if pprof != "" {
		c.Pprof = pprof
	}
	if debug {
		c.Debug = debug
	}
	checks := map[string]struct{}{}
	for _, cluster := range clusters {
		cs := &proxy.ClusterConfigs{}
		if err := cs.LoadFromFile(cluster); err != nil {
			panic(err)
		}
		for _, cc := range cs.Clusters {
			if _, ok := checks[cc.Name]; ok {
				panic("the same cluster name cannot be repeated")
			}
			checks[cc.Name] = struct{}{}
		}
		ccs = append(ccs, cs.Clusters...)
	}
	return
}

func signalHandler() {
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		log.Printf("overlord proxy version[%s] already started\n", VERSION)
		si := <-ch
		log.Printf("overlord proxy version[%s] signal(%s) stop the process\n", VERSION, si.String())
		switch si {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			log.Printf("overlord proxy version[%s] already exited\n", VERSION)
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
