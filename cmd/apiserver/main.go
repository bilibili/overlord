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
	confPath        string
	defaultConfPath = "./conf.toml"
)

func main() {
	log.Init(log.NewStdHandler())
	flag.StringVar(&confPath, "conf", "", "scheduler conf")
	flag.Parse()
	conf := new(config.ServerConfig)
	if confPath != "" {
		confPath = defaultConfPath
	}

	_, err := toml.DecodeFile(confPath, &conf)
	if err != nil {
		panic(err)
	}

	svc := service.New(conf)
	server.Run(conf, svc)
}
