package main

import (
	"flag"

	"github.com/BurntSushi/toml"

	"overlord/pkg/log"
	"overlord/platform/api/model"
	"overlord/platform/api/server"
	"overlord/platform/api/service"
	"overlord/version"
)

var (
	confPath string
)

func main() {
	flag.StringVar(&confPath, "conf", "conf.toml", "scheduler conf")
	flag.Parse()

	if version.ShowVersion() {
		return
	}

	conf := new(model.ServerConfig)
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
