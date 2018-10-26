package main

import (
	"context"
	"overlord/lib/proc"
	"overlord/mesos"
)

func main() {
	redis := proc.NewRedis(&proc.Config{
		Path: "redis-server -p 11211",
	})
	ec := mesos.New(redis)
	ec.Subscribe(context.Background())
}
