package main

import (
	"flag"
	"overlord/anzi"

	"overlord/pkg/log"

	"github.com/BurntSushi/toml"
)

var confPath string

func main() {
	flag.StringVar(&confPath, "conf", "anzi.toml", "anzi conf")
	flag.Parse()

	conf := new(anzi.Config)
	_, err := toml.DecodeFile(confPath, &conf)
	if err != nil {
		panic(err)
	}
	if log.Init(conf.Config) {
		defer log.Close()
	}
	log.Info("start anzi redis migrate data tool")

	proc := anzi.NewMigrateProc(conf.Migrate)
	proc.Migrate()
}
