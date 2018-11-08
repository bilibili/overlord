package main

import (
	"context"
	"overlord/lib/log"
	"overlord/mesos"
)

func main() {
	ec := mesos.New()
	//	config.SetRunMode(config.RunModeProd)
	ec.Subscribe(context.Background())
	log.Init(log.NewStdHandler())
}
