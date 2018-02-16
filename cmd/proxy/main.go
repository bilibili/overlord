package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/felixhao/overlord/proxy"
)

var (
	config string
)

func init() {
	flag.StringVar(&config, "conf", "", "run with the specific configuration.")
}

func main() {
	flag.Parse()
	var (
		c   *proxy.Config
		err error
	)
	if config != "" {
		c = &proxy.Config{}
		if err = c.LoadFromFile(config); err != nil {
			log.Println("")
			panic(err)
		}
	} else {
		c = proxy.NewDefaultConfig()
	}
	p, err := proxy.New(c)
	if err != nil {
		panic(err)
	}
	defer p.Close()
	go p.Serve()
	// hanlder signal
	signalHandler()
}

func signalHandler() {
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		si := <-ch
		log.Printf("get a signal %s, stop the overlord proxy process\n", si.String())
		switch si {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
