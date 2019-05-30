package main

import (
	"flag"

	"github.com/BurntSushi/toml"

	"overlord/anzi"
	"overlord/pkg/log"
	"overlord/version"
)

var confPath string

func main() {
	flag.StringVar(&confPath, "conf", "anzi.toml", "anzi config file")
	flag.Parse()
	if version.ShowVersion() {
		return
	}

	conf := new(anzi.Config)
	_, err := toml.DecodeFile(confPath, &conf)
	if err != nil {
		panic(err)
	}
	conf.Migrate.SetDefault()
	if log.Init(conf.Config) {
		defer log.Close()
	}
	log.Info("start anzi redis migrate data tool")

	proc := anzi.NewMigrateProc(conf.Migrate)
	proc.Migrate()
}
