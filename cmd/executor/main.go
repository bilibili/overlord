package main

import (
	"context"

	"overlord/pkg/log"
	"overlord/platform/mesos"
)

func main() {
	ec := mesos.New()
	log.InitHandle(log.NewStdHandler())
	ec.Run(context.Background())
}
