package main

import (
	"context"
	"overlord/lib/store/etcd"
	"overlord/mesos"
)

func main() {
	db := etcd.New()
	db.Setup(context.TODO(), "127.0.0.1:2379")
	sched := mesos.NewScheduler(&mesos.Config{}, db)
	sched.Run()
}
