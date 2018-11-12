package main

import (
	"context"
	"overlord/config"
	"overlord/lib/log"
	"overlord/mesos"
)

func main() {
	ec := mesos.New()
	config.SetRunMode(config.RunModeProd)
	log.Init(log.NewStdHandler())
	ec.Run(context.Background())

}
