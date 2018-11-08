package main

import (
	"flag"
	"overlord/lib/etcd"
	"overlord/mesos"
	"time"

	"github.com/BurntSushi/toml"
)

var confPath string
var defConf = &mesos.Config{
	User:        "root",
	Name:        "test",
	Master:      "127.0.0.1:5050",
	ExecutorURL: "http://10.23.170.136:8000/executor",
	DBEndPoint:  "http://127.0.0.1:2379",
	Checkpoint:  true,
	FailVoer:    time.Hour,
}

func main() {
	flag.StringVar(&confPath, "conf", "", "scheduler conf")
	flag.Parse()
	conf := new(mesos.Config)
	if confPath != "" {
		toml.DecodeFile(confPath, &conf)
	} else {
		conf = defConf
	}
	db, _ := etcd.New(conf.DBEndPoint)
	sched := mesos.NewScheduler(conf, db)
	sched.Run()
}
