package main

import (
	"context"
	"overlord/lib/store/etcd"
	"overlord/mesos"
)

func main() {
	db := etcd.New()
	err := db.Setup(context.TODO(), "http://127.0.0.1:2379")
	if err != nil {
		panic(err)
	}
	sched := mesos.NewScheduler(&mesos.Config{User: "root", Name: "test", Master: "127.0.0.1:5050", ExecutorURL: "http://127.0.0.1:8000/executor"}, db)
	sched.Run()
}
