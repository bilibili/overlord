package main

import (
	"flag"
	"time"

	"overlord/pkg/etcd"
	"overlord/pkg/log"
	"overlord/platform/mesos"
	"overlord/version"

	"github.com/BurntSushi/toml"
)

var confPath string
var defConf = &mesos.Config{
	User:        "root",
	Name:        "test",
	Master:      "127.0.0.1:5050",
	ExecutorURL: "http://127.0.0.1:8000/executor",
	DBEndPoint:  "http://127.0.0.1:2379",
	Checkpoint:  true,
	FailOver:    mesos.Duration(time.Hour),
}

func main() {
	flag.StringVar(&confPath, "conf", "", "scheduler conf")
	flag.Parse()
	if version.ShowVersion() {
		return
	}

	conf := new(mesos.Config)
	if confPath != "" {
		_, err := toml.DecodeFile(confPath, &conf)
		if err != nil {
			panic(err)
		}
	} else {
		conf = defConf
	}
	if log.Init(conf.Config) {
		defer log.Close()
	}
	log.Infof("start framework with conf %v", conf)
	db, err := etcd.New(conf.DBEndPoint)
	if err != nil {
		panic(err)
	}
	log.Info("init etcd successful")
	sched := mesos.NewScheduler(conf, db)
	_ = sched.Run()
}
