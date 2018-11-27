package main

import (
	"flag"

	"overlord/api/server"
	"overlord/api/service"
	"overlord/config"
	"overlord/lib/log"

	"github.com/BurntSushi/toml"
)

var (
	confPath string
)

func main() {
	flag.StringVar(&confPath, "conf", "conf.toml", "scheduler conf")
	flag.Parse()
	conf := new(config.ServerConfig)
	_, err := toml.DecodeFile(confPath, &conf)
	if err != nil {
		panic(err)
	}
	if log.Init(conf.Config) {
		defer log.Close()
	}
	svc := service.New(conf)
	server.Run(conf, svc)
}
